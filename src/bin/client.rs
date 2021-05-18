#[macro_use]
extern crate clap;
use clap::{App, Arg, SubCommand};
use kvs::{KvStore, Result};

const DEFAULT_SERVER_IP_PORT: &'static str = "127.0.0.1:4000";

fn main() -> Result<()> {
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
    let mut kv_store = KvStore::open("./")?;

    match matches.subcommand() {
        ("set", Some(m)) => {
            // let (key, value) = {
            //     (
            //         m.value_of("KEY").unwrap().to_owned(),
            //         m.value_of("VALUE").unwrap().to_owned(),
            //     )
            // };
            // kv_store.set(key, value)?;
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let _stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
        }
        ("get", Some(m)) => {
            // let key = m.value_of("KEY").unwrap().to_owned();
            // match kv_store.get(key) {
            //     Ok(None) => {
            //         println!("Key not found");
            //     }
            //     Ok(Some(val)) => {
            //         println!("{}", &val);
            //     }
            //     _ => std::process::exit(1),
            // }
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let _stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
        }
        ("rm", Some(m)) => {
            // let key = m.value_of("KEY").unwrap();
            // if let Err(kvs::KvStoreError::RemoveNonExistentKey) = kv_store.remove(key.into()) {
            //     println!("Key not found");
            //     std::process::exit(1);
            // }
            let server_addr = m
                .value_of("addr")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .unwrap();
            let _stream = std::net::TcpStream::connect_timeout(
                &server_addr,
                std::time::Duration::from_secs(3),
            )?;
        }
        _ => std::process::exit(1),
    };
    Ok(())
}
