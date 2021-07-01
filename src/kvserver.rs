use super::{
    cp::*,
    kvsengine::KvsEngine,
    kvstore::KvStore,
    sledkvsengine::SledKvsEngine,
    thread_pool::{SharedQueueThreadPool, ThreadPool},
};
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use smallvec::{smallvec, SmallVec};
use std::io::prelude::*;
use std::{
    convert::TryFrom,
    error::Error,
    fmt,
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    str::FromStr,
};

macro_rules! skip_err {
    ($res:expr, $logger:expr, $desc:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
                error!($logger, "Could not {}", $desc; "error" => e.to_string());
                return;
            }
        }
    };
}

macro_rules! unwrap_or_exit_err {
    ($res:expr, $logger:expr, $desc:expr) => {
        $res.map_err(|e| {
            error!($logger, "Could not {}", $desc; "error" => e.to_string());
            1i32
        })?
    };
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
enum Engine {
    Kvs,
    Sled,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Engine::Kvs => f.write_str("kvs"),
            Engine::Sled => f.write_str("sled"),
        }
    }
}

impl<'a> std::convert::TryFrom<&'a str> for Engine {
    type Error = KvServerCreationError<'a>;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Ok(match value {
            "sled" => Engine::Sled,
            "kvs" => Engine::Kvs,
            _ => return Err(KvServerCreationError::EngineParseError { engine_name: value }),
        })
    }
}

/// The server that listens for tcp connections,
/// receive commands, directly communicates with the database engine executing each command
/// and send responses to kvs clients
#[derive(Debug)]
pub struct KvServer {
    engine: Engine,
    address: SocketAddr,
    config_file_path: PathBuf,
}

/// The error type returned by the new function of the KvServer
#[derive(Debug)]
pub enum KvServerCreationError<'a> {
    /// Engine not supported
    EngineParseError {
        /// engine that the user tried to instantiate
        engine_name: &'a str,
    },

    /// Socket is invalid
    InvalidSocketAddress {
        /// Actual address
        addr: &'a str,
        /// Internal cause
        cause: String,
    },
}

struct LogConnectionClosedGuard {
    peer_addr: SocketAddr,
    log_server: Logger,
}

impl Drop for LogConnectionClosedGuard {
    fn drop(&mut self) {
        info!(self.log_server, "closed connection"; "peer" => self.peer_addr);
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ServerConfiguration {
    engine: Engine,
}

impl<'a> fmt::Display for KvServerCreationError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvServerCreationError::EngineParseError { engine_name } => f.write_fmt(format_args!(
                "Engine parse error: engine {} not supported",
                engine_name
            )),
            KvServerCreationError::InvalidSocketAddress { addr, cause } => f.write_fmt(
                format_args!("Invalid socket address: {}. Error: {}", addr, cause),
            ),
        }
    }
}

impl<'a> Error for KvServerCreationError<'a> {}

impl KvServer {
    /// Creates a new KvServer object given the `engine` type name,
    /// the server `address` and the `config_file` for reading/storing server configuration.
    pub fn new<'a>(
        engine: &'a str,
        address: &'a str,
        config_file: &'a str,
    ) -> Result<Self, KvServerCreationError<'a>> {
        Ok(Self {
            engine: Engine::try_from(engine)?,
            address: match address.parse::<SocketAddr>() {
                Ok(addr) => addr,
                Err(err) => {
                    return Err(KvServerCreationError::InvalidSocketAddress {
                        addr: address,
                        cause: err.to_string(),
                    })
                }
            },
            config_file_path: PathBuf::from_str(config_file).unwrap(),
        })
    }

    /// Starts listening for connections and enter the forever loop handling server connections
    pub fn run(&self) -> Result<(), i32> {
        let decorator = slog_term::TermDecorator::new().stderr().build();
        let drain = slog_term::FullFormat::new(decorator)
            .use_file_location()
            .use_utc_timestamp()
            .use_original_order()
            .build()
            .fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let log = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));
        let log_server = log.new(o!("address" => self.address, "engine" => self.address));
        info!(log_server, "starting");

        self.check_engine(log_server.clone())?;

        match self.engine {
            Engine::Kvs => {
                let dbengine =
                    unwrap_or_exit_err!(KvStore::open("./"), log_server, "open database file");
                self.handle_connections(dbengine, log_server)
            }
            Engine::Sled => {
                let dbengine = unwrap_or_exit_err!(
                    SledKvsEngine::open("./"),
                    log_server,
                    "open database file"
                );
                self.handle_connections(dbengine, log_server)
            }
        }
    }

    fn check_engine(&self, log_server: Logger) -> Result<(), i32> {
        {
            let config_file_reader = std::fs::File::open(&self.config_file_path)
            .map_or_else(
                |err| {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        Ok(None)
                    } else {
                        error!(
                            log_server,
                            "Could not open configuration file for reading"; "error" => err.to_string()
                        );
                        return Err(1);
                    }
                },
                |f| Ok(Some(std::io::BufReader::new(f))),
            )
            .unwrap();
            if let Some(rdr) = config_file_reader {
                let config: Result<ServerConfiguration, _> = serde_json::from_reader(rdr);
                let server_conf =
                    unwrap_or_exit_err!(config, log_server, "get the json configuration from file");
                if server_conf.engine != self.engine {
                    error!(
                        log_server,
                        "The server was running earlier with another engine"; "old_engine" => server_conf.engine.to_string()
                    );
                    return Err(1);
                }
            }
        }
        let config_file_writer = std::io::BufWriter::new(unwrap_or_exit_err!(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&self.config_file_path),
            log_server,
            "open configuration file for writting"
        ));
        unwrap_or_exit_err!(
            serde_json::to_writer(
                config_file_writer,
                &ServerConfiguration {
                    engine: self.engine
                }
            ),
            log_server,
            "write the json configuration to the file"
        );
        Ok(())
    }

    fn handle_connections<Engine: KvsEngine>(
        &self,
        dbengine: Engine,
        log_server: Logger,
    ) -> Result<(), i32> {
        let listener = unwrap_or_exit_err!(
            std::net::TcpListener::bind(self.address),
            log_server,
            format!("open listener on address {}", self.address)
        );
        let thread_pool = unwrap_or_exit_err!(
            SharedQueueThreadPool::new(num_cpus::get() as u32),
            log_server,
            "instantiate a thread pool"
        );
        for stream in listener.incoming() {
            let db = dbengine.clone();
            let log_server = log_server.clone();
            thread_pool.spawn(move || {
                let mut stream = skip_err!(stream, log_server, "open connection");
                skip_err!(
                    stream.set_read_timeout(Some(std::time::Duration::from_secs(3))),
                    log_server,
                    "set read timeout configuration for connection"
                );
                let peer_addr = skip_err!(
                    stream.peer_addr(),
                    log_server,
                    "get the peer address from connection"
                );
                info!(log_server, "acceppted connection"; "peer" => peer_addr);
                let _log_conn_closed_guard = LogConnectionClosedGuard {
                    peer_addr,
                    log_server: log_server.clone(),
                };
                match skip_err!(
                    KvServer::recv_payload(&mut stream),
                    log_server,
                    "get the message payload from peer's message"
                ) {
                    MessagePayload::Request(Request::Set(req)) => {
                        info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestSet", "key" => req.key(), "value" => req.value());
                        let res = db.set(req.key().to_owned(), req.value().to_owned());
                        let resp = ResponseSet::new_message(StatusCode::from(&res));
                        skip_err!(
                            KvServer::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                        info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseSet", "status" => StatusCode::from(&res).to_string());
                    }
                    MessagePayload::Request(Request::Get(req)) => {
                        info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestGet", "key" => req.key());
                        let res = db.get(req.key().to_owned());
                        let value = res.as_ref().unwrap_or(&None).clone();
                        let resp = ResponseGet::new_message(StatusCode::from(&res), value.clone());
                        skip_err!(
                            KvServer::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                        info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseGet", "status" => StatusCode::from(&res).to_string(), "value" => value);
                    }
                    MessagePayload::Request(Request::Remove(req)) => {
                        info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestRemove", "key" => req.key());
                        let res = db.remove(req.key().to_owned());
                        let resp = ResponseRemove::new_message(StatusCode::from(&res));
                        skip_err!(
                            KvServer::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                        info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseRemove", "status" => StatusCode::from(&res).to_string());
                    }
                    MessagePayload::Response(_) => {
                        // Error: client sent a response message
                        error!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "Response");
                        let resp = ResponseSet::new_message(StatusCode::FatalError);
                        skip_err!(
                            KvServer::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                    }
                }
            });
        }
        Ok(())
    }

    fn send_response(msg: &Message, stream: &mut TcpStream) -> Result<(), error::Error> {
        let mut buf = SmallVec::<[u8; 1024]>::new();
        buf.resize(ser::calc_len(msg)?, 0u8);
        ser::to_bytes(msg, &mut buf[..])?;
        let mut idx = 0usize;
        loop {
            idx += stream.write(&buf[idx..])?;
            if idx == buf.len() {
                break;
            }
        }
        Ok(())
    }

    fn recv_payload(stream: &mut TcpStream) -> Result<MessagePayload, error::Error> {
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf)?;
        let header: Result<Header, _> = de::from_bytes(&header_buf);
        let header = header?;

        let mut payload_buf: SmallVec<[u8; 1024]> = smallvec![0; header.payload_length() as usize];
        stream.read_exact(&mut payload_buf)?;
        de::from_bytes(&payload_buf)
    }
}
