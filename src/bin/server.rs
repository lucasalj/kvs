#[macro_use]
extern crate clap;

use clap::{App, Arg};
use itertools::Itertools;
use kvs::Result;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
use slog::Drain;

const DEFAULT_SERVER_IP: &'static str = "127.0.0.1";
const DEFAULT_SERVER_PORT: &'static str = "4000";

fn main() -> Result<()> {
    let default_ip_port = format!("{}:{}", DEFAULT_SERVER_IP, DEFAULT_SERVER_PORT);
    let app = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .about("A key-value store server")
        .args(&[Arg::with_name("addr")
            .long("addr")
            .value_name("IP-PORT")
            .help("Sets the server IP address, either v4 or v6, and port number, with the format IP:PORT")
            .takes_value(true)
            .default_value(default_ip_port.as_str()),
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
    let server = log.new(o!("address" => server_addr, "engine" => engine));
    info!(server, "starting");

    Ok(())
}
