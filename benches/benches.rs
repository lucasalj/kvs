use criterion::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::collections::HashSet;
use std::iter::repeat;
use tempfile::TempDir;

use kvs::*;

fn write_benchmark<E: 'static + KvsEngine>(mut db: E, name: &str, c: &mut Criterion) {
    const TIMES: usize = 100;

    let mut rng = thread_rng();

    let mut keys_set = HashSet::new();
    let keys = repeat(())
        .take(TIMES)
        .map(|_| {
            let len = rng.gen_range(1, 100000);
            loop {
                let key: String = rng.sample_iter(&Alphanumeric).take(len).collect();
                if keys_set.insert(key.clone()) {
                    return key;
                }
            }
        })
        .collect::<Vec<String>>();

    let mut values_set = HashSet::new();
    let values = repeat(())
        .take(TIMES)
        .map(|_| {
            let len = rng.gen_range(1, 100000);
            loop {
                let val: String = rng.sample_iter(&Alphanumeric).take(len).collect();
                if values_set.insert(val.clone()) {
                    return val;
                }
            }
        })
        .collect::<Vec<String>>();

    c.bench_function(name, move |b| {
        b.iter(|| {
            keys.iter().zip(values.iter()).for_each(|(key, value)| {
                db.set(key.clone(), value.clone()).unwrap_or_else(|err| {
                    panic!(
                        "Error while executing operation set with key {} and value {}. Error: {}",
                        key,
                        value,
                        err.to_string()
                    )
                })
            });
        })
    });
}

fn read_benchmark<E: 'static + KvsEngine>(mut db: E, name: &str, c: &mut Criterion) {
    const TIMES: usize = 1000;

    let mut rng = thread_rng();

    let mut keys_set = HashSet::new();
    let keys = repeat(())
        .take(TIMES)
        .map(|_| {
            let len = rng.gen_range(1, 100000);
            loop {
                let key: String = rng.sample_iter(&Alphanumeric).take(len).collect();
                if keys_set.insert(key.clone()) {
                    return key;
                }
            }
        })
        .collect::<Vec<String>>();

    let mut values_set = HashSet::new();
    let values = repeat(())
        .take(TIMES)
        .map(|_| {
            let len = rng.gen_range(1, 100000);
            loop {
                let val: String = rng.sample_iter(&Alphanumeric).take(len).collect();
                if values_set.insert(val.clone()) {
                    return val;
                }
            }
        })
        .collect::<Vec<String>>();

    keys.iter().zip(values.iter()).for_each(|(key, value)| {
        db.set(key.clone(), value.clone()).unwrap_or_else(|err| {
            panic!(
                "Error while executing operation set with key {} and value {}. Error: {}",
                key,
                value,
                err.to_string()
            )
        })
    });

    c.bench_function(name, move |b| {
        b.iter(|| {
            keys.iter().zip(values.iter()).for_each(|(key, value)| {
                assert_eq!(
                    db.get(key.clone()).unwrap_or_else(|err| panic!(
                        "Error while executing operation get with key {}. Error: {}",
                        key,
                        err.to_string()
                    )),
                    Some(value.clone())
                );
            });
        });
    });
}

pub fn kvs_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let db = KvStore::open(temp_dir.path()).unwrap();
    write_benchmark(db, "kvs_write", c);
}

pub fn sled_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let db = SledKvsEngine::open(temp_dir.path()).unwrap();
    write_benchmark(db, "sled_write", c);
}

pub fn kvs_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to crewriteate temporary working directory");
    let db = KvStore::open(temp_dir.path()).unwrap();
    read_benchmark(db, "kvs_read", c);
}

pub fn sled_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let db = SledKvsEngine::open(temp_dir.path()).unwrap();
    read_benchmark(db, "sled_read", c);
}

criterion_group!(benches, kvs_write, sled_write, kvs_read, sled_read);
criterion_main!(benches);
