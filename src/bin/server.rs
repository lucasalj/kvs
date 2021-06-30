#[macro_use]
extern crate clap;

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
};

use clap::{App, Arg};

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use serde::{Deserialize, Serialize};

use kvs::{
    cp::{de, ser, Header, MessagePayload, StatusCode, HEADER_SIZE},
    thread_pool::*,
    KvsEngine,
};
use slog::{Drain, Logger};
use smallvec::{smallvec, SmallVec};

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";
const DEFAULT_CONF_FILE_PATH: &'static str = "./.kvs-server-conf.json";

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
    engine: String,
}

fn send_response(
    msg: &kvs::cp::Message,
    stream: &mut TcpStream,
) -> Result<(), kvs::cp::error::Error> {
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

fn recv_payload(stream: &mut TcpStream) -> Result<MessagePayload, kvs::cp::error::Error> {
    let mut header_buf = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header_buf)?;
    let header: Result<Header, _> = de::from_bytes(&header_buf);
    let header = header?;

    let mut payload_buf: SmallVec<[u8; 1024]> = smallvec![0; header.payload_length() as usize];
    stream.read_exact(&mut payload_buf)?;
    de::from_bytes(&payload_buf)
}

fn handle_connections<Engine: KvsEngine>(
    dbengine: Engine,
    server_addr: &String,
    log_server: &Logger,
) -> Result<(), i32> {
    let listener = unwrap_or_exit_err!(
        std::net::TcpListener::bind(server_addr),
        log_server,
        format!("open listener on address {}", server_addr)
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
                recv_payload(&mut stream),
                log_server,
                "get the message payload from peer's message"
            ) {
                kvs::cp::MessagePayload::Request(kvs::cp::Request::Set(req)) => {
                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestSet", "key" => req.key(), "value" => req.value());
                    let res = db.set(req.key().to_owned(), req.value().to_owned());
                    let resp = kvs::cp::ResponseSet::new_message(StatusCode::from(&res));
                    skip_err!(
                        send_response(&resp, &mut stream),
                        log_server,
                        "send response to peer"
                    );
                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseSet", "status" => StatusCode::from(&res).to_string());
                }
                kvs::cp::MessagePayload::Request(kvs::cp::Request::Get(req)) => {
                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestGet", "key" => req.key());
                    let res = db.get(req.key().to_owned());
                    let value = res.as_ref().unwrap_or(&None).clone();
                    let resp = kvs::cp::ResponseGet::new_message(StatusCode::from(&res), value.clone());
                    skip_err!(
                        send_response(&resp, &mut stream),
                        log_server,
                        "send response to peer"
                    );
                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseGet", "status" => StatusCode::from(&res).to_string(), "value" => value);
                }
                kvs::cp::MessagePayload::Request(kvs::cp::Request::Remove(req)) => {
                    info!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "RequestRemove", "key" => req.key());
                    let res = db.remove(req.key().to_owned());
                    let resp = kvs::cp::ResponseRemove::new_message(StatusCode::from(&res));
                    skip_err!(
                        send_response(&resp, &mut stream),
                        log_server,
                        "send response to peer"
                    );
                    info!(log_server, "sent message"; "peer" => peer_addr, "payload_type" => "ResponseRemove", "status" => StatusCode::from(&res).to_string());
                }
                kvs::cp::MessagePayload::Response(_) => {
                    // Error: client sent a response message
                    error!(log_server, "received message"; "peer" => peer_addr, "payload_type" => "Response");
                    let resp = kvs::cp::ResponseSet::new_message(StatusCode::FatalError);
                    skip_err!(
                        send_response(&resp, &mut stream),
                        log_server,
                        "send response to peer"
                    );
                }
            }
        });
    }
    Ok(())
}

fn check_engine(engine: &String, log_server: &Logger) -> Result<(), i32> {
    {
        let config_file_reader = std::fs::File::open(DEFAULT_CONF_FILE_PATH)
        .map(|f| std::io::BufReader::new(f))
        .map_or_else(
            |err| {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    error!(
                        log_server,
                        "Could not open configuration file for reading"; "error" => err.to_string()
                    );
                    Err(err)
                }
            },
            |rdr| Ok(Some(rdr)),
        )
        .unwrap();
        if let Some(rdr) = config_file_reader {
            let config: Result<ServerConfiguration, _> = serde_json::from_reader(rdr);
            let server_conf =
                unwrap_or_exit_err!(config, log_server, "get the json configuration from file");
            if &server_conf.engine != engine {
                error!(
                    log_server,
                    "The server was running earlier with another engine"; "old_engine" => server_conf.engine
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
            .open(DEFAULT_CONF_FILE_PATH),
        log_server,
        "open configuration file for writting"
    ));
    unwrap_or_exit_err!(
        serde_json::to_writer(
            config_file_writer,
            &ServerConfiguration {
                engine: engine.clone()
            }
        ),
        log_server,
        "write the json configuration to the file"
    );
    Ok(())
}

fn run_server(engine: String, server_addr: String) -> Result<(), i32> {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .use_utc_timestamp()
        .use_original_order()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => crate_version!()));
    let log_server = log.new(o!("address" => server_addr.clone(), "engine" => engine.clone()));
    info!(log_server, "starting");

    check_engine(&engine, &log_server)?;

    if engine == "kvs" {
        let dbengine =
            unwrap_or_exit_err!(kvs::KvStore::open("./"), log_server, "open database file");
        handle_connections(dbengine, &server_addr, &log_server)
    } else {
        let dbengine = unwrap_or_exit_err!(
            kvs::SledKvsEngine::open("./"),
            log_server,
            "open database file"
        );
        handle_connections(dbengine, &server_addr, &log_server)
    }
}

fn main() {
    let is_valid_addr = |v: String| {
        v.parse::<std::net::SocketAddr>()
            .map(|_| ())
            .map_err(|e| e.to_string())
    };
    let app = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about("A key-value store server")
        .args(&[Arg::with_name("addr")
            .long("addr")
            .value_name("IP-PORT")
            .help("Sets the server IP address, either v4 or v6, and port number, with the format IP:PORT")
            .takes_value(true)
            .default_value(DEFAULT_SERVER_IP_PORT)
            .validator(is_valid_addr),
               Arg::with_name("engine")
            .long("engine")
            .value_name("ENGINE-NAME")
            .possible_values(&["kvs", "sled"])
            .help("Sets the engine to be used if it is the first run. That is, if there is no data previously persisted")
            .takes_value(true)
            .default_value("kvs")]);
    let matches = app.get_matches();

    let server_addr = matches.value_of("addr").unwrap().to_string();

    let engine = matches.value_of("engine").unwrap().to_string();

    run_server(engine, server_addr).unwrap_or_else(|code| std::process::exit(code));
}
