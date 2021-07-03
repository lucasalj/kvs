use criterion::*;
use crossbeam::channel::{bounded, Receiver, Sender};
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use slog::o;
use std::collections::HashSet;
use std::iter::repeat;
use tempfile::TempDir;

use kvs::*;

fn write_benchmark<E: 'static + KvsEngine>(db: E, name: &str, c: &mut Criterion) {
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

fn read_benchmark<E: 'static + KvsEngine>(db: E, name: &str, c: &mut Criterion) {
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

pub fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = std::iter::successors(Some(1u32), |n| n.checked_mul(2))
        .take_while(|n| *n <= (2 * num_cpus::get() as u32))
        .collect::<Vec<u32>>();
    const N_CLIENT_THREADS: u32 = 1000;
    const KEY_LENGTH: usize = 30;
    const VALUE_LENGTH: usize = 30;

    let mut rng = thread_rng();

    let value: String = rng.sample_iter(&Alphanumeric).take(VALUE_LENGTH).collect();

    let mut keys_set = HashSet::new();
    let keys = repeat(())
        .take(N_CLIENT_THREADS as usize)
        .map(|_| loop {
            let key: String = rng.sample_iter(&Alphanumeric).take(KEY_LENGTH).collect();
            if keys_set.insert(key.clone()) {
                return key;
            }
        })
        .collect::<Vec<String>>();

    c.bench_function_over_inputs(
        "write_queued_kvstore",
        move |b, threads| {
            let server_port = portpicker::pick_unused_port().unwrap();
            let server_addr = format!("127.0.0.1:{}", server_port);
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let clients_thread_pool = SharedQueueThreadPool::new(N_CLIENT_THREADS).expect(
                format!(
                    "unable to initialize a thread pool with {} threads",
                    N_CLIENT_THREADS
                )
                .as_str(),
            );

            let server = KvServer::new(
                KvStore::open(temp_dir.path()).expect("unable to open database file"),
                server_addr.as_str(),
                SharedQueueThreadPool::new(*threads).expect(
                    format!(
                        "unable to initialize a thread pool with {} threads",
                        *threads
                    )
                    .as_str(),
                ),
                slog::Logger::root(slog::Discard, o!("" => "")),
            )
            .expect("unable to start the kvs server");
            let server_shutdown_trigger = server.get_shutdown_trigger();

            let server_join_handle = std::thread::spawn(move || {
                server.run().expect("server stopped with an error");
            });

            let (tx_start, rx_start): (Sender<()>, Receiver<()>) =
                bounded(N_CLIENT_THREADS as usize);
            let (tx_result, rx_result): (
                Sender<std::result::Result<(), KvClientError<'static>>>,
                Receiver<std::result::Result<(), KvClientError<'static>>>,
            ) = bounded(N_CLIENT_THREADS as usize);
            let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(N_CLIENT_THREADS as usize);

            for i in 0..N_CLIENT_THREADS {
                let key = keys[i as usize].clone();
                let value = value.clone();
                let server_addr = server_addr.clone();
                let rx_start = rx_start.clone();
                let tx_result = tx_result.clone();
                let rx_next = rx_next.clone();
                clients_thread_pool.spawn(move || {
                    let client = KvClient::new(server_addr.as_str()).unwrap();
                    while let Ok(_) = rx_start.recv() {
                        tx_result
                            .send(client.send_cmd_set(key.clone(), value.clone()))
                            .unwrap();
                        rx_next.recv().unwrap();
                    }
                });
            }

            // The part that actually matters
            // Enable the client threads to start sending requests
            // And latter assert that the operation went successfully
            b.iter(|| {
                for _ in 0..N_CLIENT_THREADS {
                    tx_start.send(()).unwrap();
                }
                for _ in 0..N_CLIENT_THREADS {
                    let result = rx_result.recv().unwrap();
                    assert!(result.is_ok(), "error: {}", result.unwrap_err());
                }
                for _ in 0..N_CLIENT_THREADS {
                    tx_next.send(()).unwrap();
                }
            });

            server_shutdown_trigger
                .trigger()
                .expect("unable to trigger server shutdown");
            server_join_handle.join().unwrap();
        },
        inputs,
    );
}

criterion_group!(
    benches,
    kvs_write,
    sled_write,
    kvs_read,
    sled_read,
    write_queued_kvstore,
);
criterion_main!(benches);
