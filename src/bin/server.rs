#[macro_use]
extern crate clap;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use clap::{App, Arg};
use kvs::{
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    unwrap_or_return_code1_on_err, KvServer, KvStore, SledKvsEngine,
};
use serde::{Deserialize, Serialize};
use slog::{Drain, Logger};
use std::net::SocketAddr;

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";
const DEFAULT_CONF_FILE_PATH: &'static str = "./.kvs-server-conf.json";

fn is_valid_address(addr: String) -> Result<(), String> {
    addr.parse::<SocketAddr>()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[derive(Serialize, Deserialize, Debug)]
struct ServerConfiguration {
    engine: String,
}

fn check_engine(engine: &str, config_file_path: &str, log_server: Logger) -> Result<(), i32> {
    {
        let config_file_reader = std::fs::File::open(config_file_path)
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
            let server_conf = unwrap_or_return_code1_on_err!(
                config,
                log_server,
                "get the json configuration from file"
            );
            if server_conf.engine != engine {
                error!(
                    log_server,
                    "The server was running earlier with another engine"; "old_engine" => server_conf.engine.to_string()
                );
                return Err(1);
            }
        }
    }
    let config_file_writer = std::io::BufWriter::new(unwrap_or_return_code1_on_err!(
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(config_file_path),
        log_server,
        "open configuration file for writting"
    ));
    unwrap_or_return_code1_on_err!(
        serde_json::to_writer(
            config_file_writer,
            &ServerConfiguration {
                engine: engine.to_string()
            }
        ),
        log_server,
        "write the json configuration to the file"
    );
    Ok(())
}

fn run_server_logging(engine: String, server_addr: String) -> Result<(), i32> {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .use_utc_timestamp()
        .use_original_order()
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!("version" => crate_version!()));
    let log_server =
        log.new(o!("address" => server_addr.to_owned(), "engine" => engine.to_owned()));
    info!(log_server, "starting");

    check_engine(engine.as_str(), DEFAULT_CONF_FILE_PATH, log_server.clone())?;

    match engine.as_str() {
        "kvs" => {
            let server = KvServer::new(
                unwrap_or_return_code1_on_err!(
                    KvStore::open("./"),
                    log_server,
                    "open database file"
                ),
                server_addr.as_str(),
                unwrap_or_return_code1_on_err!(
                    SharedQueueThreadPool::new(num_cpus::get() as u32),
                    log_server,
                    "instantiate a thread pool"
                ),
                log_server,
            )
            .unwrap();

            server.run()?;
        }
        "sled" => {
            let server = KvServer::new(
                unwrap_or_return_code1_on_err!(
                    SledKvsEngine::open("./"),
                    log_server,
                    "open database file"
                ),
                server_addr.as_str(),
                unwrap_or_return_code1_on_err!(
                    SharedQueueThreadPool::new(num_cpus::get() as u32),
                    log_server,
                    "instantiate a thread pool"
                ),
                log_server,
            )
            .unwrap();

            server.run()?;
        }
        _ => unreachable!(),
    }
    Ok(())
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

    run_server_logging(engine, server_addr).unwrap_or_else(|code| std::process::exit(code));
}
