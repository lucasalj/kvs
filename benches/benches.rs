use criterion::*;
use crossbeam::channel::{bounded, Receiver, Sender};
use kvs::thread_pool::RayonThreadPool;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::*;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use slog::o;
use std::collections::HashSet;
use std::iter::repeat;
use std::rc::Rc;
use tempfile::TempDir;

const CLIENT_SEND_CMD_SET_ATTEMPTS: u16 = 10000;
const CLIENT_SEND_CMD_GET_ATTEMPTS: u16 = 10000;

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

fn client_send_cmd_set(
    client: &'_ KvClient,
    key: String,
    value: String,
    mut attempts: u16,
) -> std::result::Result<(), KvClientError<'static>> {
    loop {
        match client.send_cmd_set(key.clone(), value.clone()) {
            Ok(()) => return Ok(()),
            Err(_) if attempts > 0 => attempts -= 1,
            Err(e) => return Err(e),
        }
    }
}

fn client_send_cmd_get(
    client: &'_ KvClient,
    key: String,
    mut attempts: u16,
) -> std::result::Result<Option<String>, KvClientError<'static>> {
    loop {
        match client.send_cmd_get(key.clone()) {
            Ok(v) => return Ok(v),
            Err(_) if attempts > 0 => attempts -= 1,
            Err(e) => return Err(e),
        }
    }
}

fn write_queued_kvstore(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = SharedQueueThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
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
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let value = (*value).clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_set(
                            &client,
                            key.clone(),
                            value.clone(),
                            CLIENT_SEND_CMD_SET_ATTEMPTS,
                        )
                        .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

fn read_queued_kvstore(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = SharedQueueThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
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
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_get(&client, key.clone(), CLIENT_SEND_CMD_GET_ATTEMPTS)
                            .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    let client = KvClient::new(server_addr.as_str()).unwrap();
    keys.iter().cloned().for_each(|key| {
        client_send_cmd_set(&client, key, (*value).clone(), CLIENT_SEND_CMD_SET_ATTEMPTS).unwrap()
    });

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

pub fn write_rayon_kvstore(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = RayonThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
        KvStore::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        RayonThreadPool::new(*threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                *threads
            )
            .as_str(),
        ),
        slog::Logger::root(slog::Discard, o!("" => "")),
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let value = (*value).clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_set(
                            &client,
                            key.clone(),
                            value.clone(),
                            CLIENT_SEND_CMD_SET_ATTEMPTS,
                        )
                        .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

pub fn read_rayon_kvstore(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = RayonThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
        KvStore::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        RayonThreadPool::new(*threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                *threads
            )
            .as_str(),
        ),
        slog::Logger::root(slog::Discard, o!("" => "")),
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_get(&client, key.clone(), CLIENT_SEND_CMD_GET_ATTEMPTS)
                            .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    let client = KvClient::new(server_addr.as_str()).unwrap();
    keys.iter().cloned().for_each(|key| {
        client_send_cmd_set(&client, key, (*value).clone(), CLIENT_SEND_CMD_SET_ATTEMPTS).unwrap()
    });

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

pub fn write_rayon_sledkvengine(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = RayonThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
        SledKvsEngine::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        RayonThreadPool::new(*threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                *threads
            )
            .as_str(),
        ),
        slog::Logger::root(slog::Discard, o!("" => "")),
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let value = (*value).clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_set(
                            &client,
                            key.clone(),
                            value.clone(),
                            CLIENT_SEND_CMD_SET_ATTEMPTS,
                        )
                        .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

pub fn read_rayon_sledkvengine(
    b: &mut Bencher,
    threads: &u32,
    n_client_threads: u32,
    value: Rc<String>,
    keys: Rc<Vec<String>>,
) {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let clients_thread_pool = RayonThreadPool::new(n_client_threads).expect(
        format!(
            "unable to initialize a thread pool with {} threads",
            n_client_threads
        )
        .as_str(),
    );

    let mut server = KvServer::new(
        SledKvsEngine::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        RayonThreadPool::new(*threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                *threads
            )
            .as_str(),
        ),
        slog::Logger::root(slog::Discard, o!("" => "")),
        None,
    )
    .expect("unable to start the kvs server");
    let server_shutdown_trigger = server.get_shutdown_trigger();

    let server_join_handle = std::thread::spawn(move || {
        server.run().expect("server stopped with an error");
    });
    std::thread::sleep(std::time::Duration::from_secs(1));

    let (tx_start, rx_start): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);
    let (tx_result, rx_result): (Sender<bool>, Receiver<bool>) = bounded(n_client_threads as usize);
    let (tx_next, rx_next): (Sender<()>, Receiver<()>) = bounded(n_client_threads as usize);

    for i in 0..n_client_threads {
        let key = keys[i as usize].clone();
        let server_addr = server_addr.clone();
        let rx_start = rx_start.clone();
        let tx_result = tx_result.clone();
        let rx_next = rx_next.clone();
        clients_thread_pool.spawn(move || {
            let client = KvClient::new(server_addr.as_str()).unwrap();
            while let Ok(_) = rx_start.recv() {
                tx_result
                    .send(
                        client_send_cmd_get(&client, key.clone(), CLIENT_SEND_CMD_GET_ATTEMPTS)
                            .is_ok(),
                    )
                    .unwrap();
                rx_next.recv().unwrap();
            }
        });
    }

    let client = KvClient::new(server_addr.as_str()).unwrap();
    keys.iter().cloned().for_each(|key| {
        client_send_cmd_set(&client, key, (*value).clone(), CLIENT_SEND_CMD_SET_ATTEMPTS).unwrap()
    });

    // The part that actually matters
    // Enable the client threads to start sending requests
    // And latter assert that the operation went successfully
    b.iter(|| {
        for _ in 0..n_client_threads {
            tx_start.send(()).unwrap();
        }
        for _ in 0..n_client_threads {
            assert!(rx_result.recv().unwrap());
        }
        for _ in 0..n_client_threads {
            tx_next.send(()).unwrap();
        }
    });

    server_shutdown_trigger.trigger();
    std::thread::sleep(std::time::Duration::from_secs(1));
    server_join_handle.join().unwrap();
}

pub fn set_max_open_file_limit() {
    let open_files_limit: Box<libc::rlimit> = Box::new(libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    });
    let rlim: *mut libc::rlimit = Box::into_raw(open_files_limit);
    let res = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, rlim) };
    assert_eq!(res, 0, "{}", std::io::Error::last_os_error().to_string());

    unsafe { (*rlim).rlim_cur = (*rlim).rlim_max };
    unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, rlim) };
    assert_eq!(res, 0, "{}", std::io::Error::last_os_error().to_string());

    let _ = unsafe { Box::from_raw(rlim) };
}

pub fn bench_server_write_read(c: &mut Criterion) {
    set_max_open_file_limit();
    let inputs = std::iter::successors(Some(1u32), |n| n.checked_mul(2))
        .take_while(|n| *n <= (2 * num_cpus::get_physical() as u32))
        .collect::<Vec<u32>>();

    const N_CLIENT_THREADS: u32 = 1000;
    const KEY_LENGTH: usize = 30;
    const VALUE_LENGTH: usize = 30;

    let mut rng = thread_rng();

    let value: Rc<String> = Rc::new(rng.sample_iter(&Alphanumeric).take(VALUE_LENGTH).collect());

    let mut keys_set = HashSet::new();
    let keys = Rc::new(
        repeat(())
            .take(N_CLIENT_THREADS as usize)
            .map(|_| loop {
                let key: String = rng.sample_iter(&Alphanumeric).take(KEY_LENGTH).collect();
                if keys_set.insert(key.clone()) {
                    return key;
                }
            })
            .collect::<Vec<String>>(),
    );

    for input in inputs.iter() {
        c.bench_functions(
            format!("server_write_{}_threads", input).as_str(),
            vec![
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("write_queued_kvstore_{}_threads", input).as_str(),
                        move |b, threads| {
                            write_queued_kvstore(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("write_rayon_kvstore_{}_threads", input).as_str(),
                        move |b, threads| {
                            write_rayon_kvstore(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("write_rayon_sledkvengine_{}_threads", input).as_str(),
                        move |b, threads| {
                            write_rayon_sledkvengine(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
            ],
            *input,
        );

        c.bench_functions(
            format!("server_read_{}_threads", input).as_str(),
            vec![
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("read_queued_kvstore_{}_threads", input).as_str(),
                        move |b, threads| {
                            read_queued_kvstore(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("read_rayon_kvstore_{}_threads", input).as_str(),
                        move |b, threads| {
                            read_rayon_kvstore(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
                {
                    let value = value.clone();
                    let keys = keys.clone();
                    Fun::new(
                        format!("read_rayon_sledkvengine_{}_threads", input).as_str(),
                        move |b, threads| {
                            read_rayon_sledkvengine(
                                b,
                                threads,
                                N_CLIENT_THREADS,
                                value.clone(),
                                keys.clone(),
                            )
                        },
                    )
                },
            ],
            *input,
        );
    }
}

criterion_group!(
    benches,
    kvs_write,
    sled_write,
    kvs_read,
    sled_read,
    bench_server_write_read,
);
criterion_main!(benches);
