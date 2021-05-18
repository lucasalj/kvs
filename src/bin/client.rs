#[macro_use]
extern crate clap;
use clap::{App, Arg, SubCommand};
use kvs::{KvStore, Result};

fn main() -> Result<()> {
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
                .arg(Arg::with_name("VALUE").required(true).index(2)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .author(crate_authors!())
                .version(crate_version!())
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .author(crate_authors!())
                .version(crate_version!())
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true).index(1)),
        )
        .get_matches();

    if matches.subcommand.is_none() {
        std::process::exit(1);
    }
    let mut kv_store = KvStore::open("./")?;

    match matches.subcommand() {
        ("set", Some(m)) => {
            let (key, value) = {
                (
                    m.value_of("KEY").unwrap().to_owned(),
                    m.value_of("VALUE").unwrap().to_owned(),
                )
            };
            kv_store.set(key, value)?;
        }
        ("get", Some(m)) => {
            let key = m.value_of("KEY").unwrap().to_owned();
            match kv_store.get(key) {
                Ok(None) => {
                    println!("Key not found");
                }
                Ok(Some(val)) => {
                    println!("{}", &val);
                }
                _ => std::process::exit(1),
            }
        }
        ("rm", Some(m)) => {
            let key = m.value_of("KEY").unwrap();
            if let Err(kvs::KvStoreError::RemoveNonExistentKey) = kv_store.remove(key.into()) {
                println!("Key not found");
                std::process::exit(1);
            }
        }
        _ => std::process::exit(1),
    };
    Ok(())
}