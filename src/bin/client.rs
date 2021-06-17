#[macro_use]
extern crate clap;

use clap::{App, Arg, SubCommand};
use kvs::cp::{de, ser, Header, MessagePayload, StatusCode, HEADER_SIZE};
use smallvec::{smallvec, SmallVec};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";

fn send_request(
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

fn main() -> std::result::Result<(), std::boxed::Box<dyn std::error::Error>> {
    let is_valid_addr = |v: String| {
        v.parse::<std::net::SocketAddr>()
            .map(|_| ())
            .map_err(|e| e.to_string())
    };

    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .subcommand(
            SubCommand::with_name("set")
                .author(crate_authors!())
                .version(crate_version!())
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").required(true).index(1))
                .arg(Arg::with_name("VALUE").required(true).index(2))
                .arg(Arg::with_name("addr")
                     .long("addr")
                    .value_name("IP-PORT")
                    .help("Sets the server IP address, either v4 or v6, and port number, with the format IP:PORT")
                    .takes_value(true)
                    .default_value(DEFAULT_SERVER_IP_PORT)
                    .validator(is_valid_addr)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .author(crate_authors!())
                .version(crate_version!())
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true).index(1))
                .arg(Arg::with_name("addr")
                     .long("addr")
                     .value_name("IP-PORT")
                     .help("Sets the server IP address, either v4 or v6, and port number, with the format IP:PORT")
                     .takes_value(true)
                     .default_value(DEFAULT_SERVER_IP_PORT)
                     .validator(is_valid_addr)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .author(crate_authors!())
                .version(crate_version!())
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true).index(1))
                .arg(Arg::with_name("addr")
                     .long("addr")
                     .value_name("IP-PORT")
                     .help("Sets the server IP address, either v4 or v6, and port number, with the format IP:PORT")
                     .takes_value(true)
                     .default_value(DEFAULT_SERVER_IP_PORT)
                     .validator(is_valid_addr)),
        )
        .get_matches();

    if matches.subcommand.is_none() {
        std::process::exit(1);
    }

    match matches.subcommand() {
        ("set", Some(m)) => {
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let mut stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
            let msg = kvs::cp::RequestSet::new_message(
                m.value_of("KEY").unwrap().to_owned(),
                m.value_of("VALUE").unwrap().to_owned(),
            );
            send_request(&msg, &mut stream)?;
            match recv_payload(&mut stream)? {
                kvs::cp::MessagePayload::Response(kvs::cp::Response::Set(r)) => match r.code() {
                    StatusCode::KeyNotFound => {
                        println!("Key not found");
                        std::process::exit(1);
                    }
                    StatusCode::FatalError => std::process::exit(1),
                    StatusCode::Ok => {}
                },
                _ => std::process::exit(1),
            }
        }
        ("get", Some(m)) => {
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let mut stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
            let msg = kvs::cp::RequestGet::new_message(m.value_of("KEY").unwrap().to_owned());
            send_request(&msg, &mut stream)?;
            match recv_payload(&mut stream)? {
                kvs::cp::MessagePayload::Response(kvs::cp::Response::Get(r)) => match r.code() {
                    StatusCode::KeyNotFound => {
                        println!("Key not found");
                    }
                    StatusCode::FatalError => std::process::exit(1),
                    StatusCode::Ok => match r.value() {
                        Some(s) => println!("{}", s),
                        None => {
                            println!("Key not found");
                        }
                    },
                },
                _ => std::process::exit(1),
            }
        }
        ("rm", Some(m)) => {
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let mut stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
            let msg = kvs::cp::RequestRemove::new_message(m.value_of("KEY").unwrap().to_owned());
            send_request(&msg, &mut stream)?;
            match recv_payload(&mut stream)? {
                kvs::cp::MessagePayload::Response(kvs::cp::Response::Remove(r)) => match r.code() {
                    StatusCode::KeyNotFound => {
                        eprintln!("Key not found");
                        std::process::exit(1);
                    }
                    StatusCode::FatalError => std::process::exit(1),
                    StatusCode::Ok => {}
                },
                _ => std::process::exit(1),
            }
        }
        _ => std::process::exit(1),
    };
    Ok(())
}
