#[macro_use]
extern crate clap;
use clap::{App, Arg, SubCommand};
use kvs::KvStore;

use core::panic;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
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

    let mut _kv_store = KvStore::new();

    match matches.subcommand() {
        ("set", Some(_)) => {
            // let (key, value) = (|m: &ArgMatches| {
            //     (
            //         m.value_of("KEY").unwrap().to_owned(),
            //         m.value_of("VALUE").unwrap().to_owned(),
            //     )
            // })(m);
            // kv_store.set(key, value);
            panic!("unimplemented");
        }
        ("get", Some(_)) => {
            // let key = m.value_of("KEY").unwrap().to_owned();
            // if let Some(val) = kv_store.get(key) {
            //     println!("{}", &val);
            // } else {
            //     std::process::exit(1);
            // }
            panic!("unimplemented");
        }
        ("rm", Some(_)) => {
            // let key = m.value_of("KEY").unwrap();
            // kv_store.remove(key.into());
            panic!("unimplemented");
        }
        _ => std::process::exit(1),
    }
}
