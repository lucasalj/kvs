#[macro_use]
extern crate clap;

use std::{
    io::{Read, Write},
    net::TcpStream,
};

use clap::{App, Arg};

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use kvs::cp::{de, ser, Header, MessagePayload, StatusCode, HEADER_SIZE};
use slog::Drain;
use smallvec::{smallvec, SmallVec};

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";

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

fn main() -> Result<(), std::boxed::Box<dyn std::error::Error>> {
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

    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .use_utc_timestamp()
        .use_original_order()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => crate_version!()));
    let server = log.new(o!("address" => server_addr.clone(), "engine" => engine));
    info!(server, "starting");

    let mut kvs = kvs::KvStore::open("./")?;

    let listener = std::net::TcpListener::bind(server_addr)?;
    for stream in listener.incoming() {
        let mut stream = stream?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        let peer_addr = stream.peer_addr()?;
        info!(server, "acceppted connection"; "peer" => peer_addr);
        match recv_payload(&mut stream)? {
            kvs::cp::MessagePayload::Request(kvs::cp::Request::Set(req)) => {
                info!(server, "received message"; "peer" => peer_addr, "payload_type" => "RequestSet", "key" => req.key(), "value" => req.value());
                let res = kvs.set(req.key().to_owned(), req.value().to_owned());
                let resp = kvs::cp::ResponseSet::new_message(StatusCode::from(&res));
                send_response(&resp, &mut stream)?;
            }
            kvs::cp::MessagePayload::Request(kvs::cp::Request::Get(req)) => {
                info!(server, "received message"; "peer" => peer_addr, "payload_type" => "RequestGet", "key" => req.key());
                let res = kvs.get(req.key().to_owned());
                let resp =
                    kvs::cp::ResponseGet::new_message(StatusCode::from(&res), res.unwrap_or(None));
                send_response(&resp, &mut stream)?;
            }
            kvs::cp::MessagePayload::Request(kvs::cp::Request::Remove(req)) => {
                info!(server, "received message"; "peer" => peer_addr, "payload_type" => "RequestRemove", "key" => req.key());
                let res = kvs.remove(req.key().to_owned());
                let resp = kvs::cp::ResponseRemove::new_message(StatusCode::from(&res));
                send_response(&resp, &mut stream)?;
            }
            kvs::cp::MessagePayload::Response(_) => {
                // Error: client sent a response message
                error!(server, "received message"; "peer" => peer_addr, "payload_type" => "Response");
                let resp = kvs::cp::ResponseSet::new_message(StatusCode::FatalError);
                send_response(&resp, &mut stream)?;
            }
        }
    }

    Ok(())
}
