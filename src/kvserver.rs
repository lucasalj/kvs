use super::{cp::*, kvsengine::KvsEngine, thread_pool::ThreadPool};
use slog::Logger;
use smallvec::{smallvec, SmallVec};
use std::io::prelude::*;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{
    error::Error,
    fmt,
    net::{SocketAddr, TcpStream},
};

/// Macro to unwrap the Ok of a result or if Err, log and returns the control flow to the caller
#[macro_export]
macro_rules! unwrap_or_return_on_err {
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

/// Macro to unwrap the Ok of a result or if Err, log and returns the control flow with the value 1 to the caller
#[macro_export]
macro_rules! unwrap_or_return_code1_on_err {
    ($res:expr, $logger:expr, $desc:expr) => {
        $res.map_err(|e| {
            error!($logger, "Could not {}", $desc; "error" => e.to_string());
            1i32
        })?
    };
}

/// The server that listens for tcp connections,
/// receive commands, directly communicates with the database engine executing each command
/// and send responses to kvs clients
#[derive(Debug)]
pub struct KvServer<Engine: KvsEngine, Tp: ThreadPool> {
    db: Engine,
    address: SocketAddr,
    thread_pool: Tp,
    logger: Logger,
    shutdown_trigger: KvServerShutdownTrigger,
    listener: TcpListener,
}

/// The signal that is sent to the KvServer indicanting that it should stop running
#[derive(Debug, Clone)]
pub struct KvServerShutdownTrigger(Arc<AtomicBool>, SocketAddr);

impl KvServerShutdownTrigger {
    /// Creates a new Shutdown signal
    pub fn new(server_addresss: SocketAddr) -> Self {
        Self(Arc::new(AtomicBool::new(false)), server_addresss)
    }

    /// Fires a signal to the server indicating that it must shutdown
    pub fn trigger(&self) -> Result<(), KvServerShutdownTriggerError> {
        self.0.store(true, Ordering::SeqCst);
        TcpStream::connect(&self.1)?;
        Ok(())
    }

    /// Tells if the signal was fired
    pub fn must_shutdown(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// An error returned when KvServerShutdownTrigger fails in the process of signaling
#[derive(Debug)]
pub enum KvServerShutdownTriggerError {
    /// Fails to connect to server to force a atomic read from the server
    TCPConnection(std::io::Error),
}

impl fmt::Display for KvServerShutdownTriggerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvServerShutdownTriggerError::TCPConnection(e) => f.write_str(e.to_string().as_str()),
        }
    }
}

impl std::error::Error for KvServerShutdownTriggerError {}

impl std::convert::From<std::io::Error> for KvServerShutdownTriggerError {
    fn from(e: std::io::Error) -> Self {
        KvServerShutdownTriggerError::TCPConnection(e)
    }
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

    /// The listener failed to start
    UnableToStartListener,
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
            KvServerCreationError::UnableToStartListener => f.write_str("Could not start listener"),
        }
    }
}

impl<'a> Error for KvServerCreationError<'a> {}

impl<Engine, Tp> KvServer<Engine, Tp>
where
    Engine: KvsEngine,
    Tp: ThreadPool,
{
    /// Creates a new KvServer object given the `engine` type name,
    /// the server `address` and the `config_file` for reading/storing server configuration.
    pub fn new<'a>(
        engine: Engine,
        address: &'a str,
        thread_pool: Tp,
        logger: Logger,
    ) -> Result<Self, KvServerCreationError<'a>> {
        let address = match address.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(err) => {
                return Err(KvServerCreationError::InvalidSocketAddress {
                    addr: address,
                    cause: err.to_string(),
                })
            }
        };
        let listener = std::net::TcpListener::bind(address).map_err(|e| {
            error!(logger, "Could not open listener on address {}", address; "error" => e.to_string());
            KvServerCreationError::UnableToStartListener
        })?;
        Ok(Self {
            db: engine,
            address,
            thread_pool,
            logger,
            shutdown_trigger: KvServerShutdownTrigger::new(address),
            listener,
        })
    }

    /// Starts listening for connections and enter the forever loop handling server connections
    pub fn run(&self) -> Result<(), i32> {
        for stream in self.listener.incoming() {
            if self.shutdown_trigger.must_shutdown() {
                info!(self.logger, "server stopped listening to connections");
                break;
            }
            let log_server = self.logger.clone();
            let db = self.db.clone();
            self.thread_pool.spawn(move || {
                let mut stream = unwrap_or_return_on_err!(stream, log_server, "open connection");
                let peer_addr = unwrap_or_return_on_err!(
                    stream.peer_addr(),
                    log_server,
                    "get the peer address from connection"
                );
                info!(log_server, "acceppted connection"; "peer" => peer_addr);
                let _log_conn_closed_guard = LogConnectionClosedGuard {
                    peer_addr,
                    log_server: log_server.clone(),
                };
                match unwrap_or_return_on_err!(
                    KvServer::<Engine, Tp>::recv_payload(&mut stream),
                    log_server,
                    "get the message payload from peer's message"
                ) {
                    MessagePayload::Request(Request::Set(req)) => {
                        info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestSet", "key" => req.key(), "value" => req.value());
                        let res = db.set(req.key().to_owned(), req.value().to_owned());
                        let resp = ResponseSet::new_message(StatusCode::from(&res));
                        unwrap_or_return_on_err!(
                            KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
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
                        unwrap_or_return_on_err!(
                            KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                        info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseGet", "status" => StatusCode::from(&res).to_string(), "value" => value);
                    }
                    MessagePayload::Request(Request::Remove(req)) => {
                        info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestRemove", "key" => req.key());
                        let res = db.remove(req.key().to_owned());
                        let resp = ResponseRemove::new_message(StatusCode::from(&res));
                        unwrap_or_return_on_err!(
                            KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                        info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseRemove", "status" => StatusCode::from(&res).to_string());
                    }
                    MessagePayload::Response(_) => {
                        // Error: client sent a response message
                        error!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "Response");
                        let resp = ResponseSet::new_message(StatusCode::FatalError);
                        unwrap_or_return_on_err!(
                            KvServer::<Engine, Tp>::send_response(&resp, &mut stream),
                            log_server,
                            "send response to peer"
                        );
                    }
                }
            });
        }
        Ok(())
    }

    /// Gives the user a trigger that can be used to signal to the server that it must stop running
    pub fn get_shutdown_trigger(&self) -> KvServerShutdownTrigger {
        self.shutdown_trigger.clone()
    }

    fn send_response(msg: &Message, stream: &mut TcpStream) -> Result<(), error::Error> {
        let mut buf = SmallVec::<[u8; 256]>::new();
        buf.resize(ser::calc_len(msg)?, 0u8);
        ser::to_bytes(msg, &mut buf[..])?;
        stream.write_all(&buf[..])?;
        Ok(())
    }

    fn recv_payload(stream: &mut TcpStream) -> Result<MessagePayload, error::Error> {
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf)?;
        let header: Result<Header, _> = de::from_bytes(&header_buf);
        let header = header?;

        let mut payload_buf: SmallVec<[u8; 256]> = smallvec![0; header.payload_length() as usize];
        stream.read_exact(&mut payload_buf)?;
        de::from_bytes(&payload_buf)
    }
}
