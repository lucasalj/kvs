#[macro_use]
extern crate clap;

use clap::{App, Arg};
use kvs::KvServer;
use std::net::SocketAddr;

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";
const DEFAULT_CONF_FILE_PATH: &'static str = "./.kvs-server-conf.json";

fn is_valid_address(addr: String) -> Result<(), String> {
    addr.parse::<SocketAddr>()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn main() {
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
            .validator(is_valid_address),
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

    let server = KvServer::new(
        engine.as_str(),
        server_addr.as_str(),
        DEFAULT_CONF_FILE_PATH,
    )
    .unwrap();
    server.run().unwrap_or_else(|code| std::process::exit(code));
}
