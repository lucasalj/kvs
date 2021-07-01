#[macro_use]
extern crate clap;

use clap::{App, Arg, SubCommand};
use kvs::KvClient;
use std::net::SocketAddr;

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";

fn is_valid_address(addr: String) -> Result<(), String> {
    addr.parse::<SocketAddr>()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn main() {
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
                    .validator(is_valid_address)),
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
                     .validator(is_valid_address)),
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
                     .validator(is_valid_address)),
        )
        .get_matches();

    if matches.subcommand.is_none() {
        std::process::exit(1);
    }

    match matches.subcommand() {
        ("set", Some(m)) => {
            let client = KvClient::new(m.value_of("addr").unwrap()).unwrap_or_else(|err| {
                eprintln!("{}", err);
                std::process::exit(1);
            });
            client
                .send_cmd_set(
                    m.value_of("KEY").unwrap().to_owned(),
                    m.value_of("VALUE").unwrap().to_owned(),
                )
                .unwrap_or_else(|err| {
                    eprintln!("{}", err);
                    std::process::exit(1);
                });
        }
        ("get", Some(m)) => {
            let client = KvClient::new(m.value_of("addr").unwrap()).unwrap_or_else(|err| {
                eprintln!("{}", err);
                std::process::exit(1);
            });
            let value = client
                .send_cmd_get(m.value_of("KEY").unwrap().to_owned())
                .unwrap_or_else(|err| {
                    eprintln!("{}", err);
                    std::process::exit(1);
                });
            match value {
                Some(val) => println!("{}", val),
                None => println!("Key not found"),
            }
        }
        ("rm", Some(m)) => {
            let client = KvClient::new(m.value_of("addr").unwrap()).unwrap_or_else(|err| {
                eprintln!("{}", err);
                std::process::exit(1);
            });
            client
                .send_cmd_rm(m.value_of("KEY").unwrap().to_owned())
                .unwrap_or_else(|err| {
                    eprintln!("{}", err);
                    std::process::exit(1);
                });
        }
        _ => std::process::exit(1),
    };
}
