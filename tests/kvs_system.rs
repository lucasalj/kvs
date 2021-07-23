use kvs::{
    thread_pool::{SharedQueueThreadPool, ThreadPool},
    KvClient, KvServer, KvStore,
};
use slog::o;
use tempfile::TempDir;
use walkdir::WalkDir;

#[test]
fn compaction() {
    let server_port = portpicker::pick_unused_port().unwrap();
    let server_addr = format!("127.0.0.1:{}", server_port);
    let cpu_threads = num_cpus::get_physical() as u32;
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut server = KvServer::new(
        KvStore::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        SharedQueueThreadPool::new(cpu_threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                cpu_threads
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

    let mut current_size = dir_size();

    let client = KvClient::new(server_addr.as_str()).expect("unable to start client");
    let mut last_iter = 0;
    for iter in 0..1000 {
        last_iter = iter;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            let mut attempts = 1000;
            loop {
                if let Err(e) = client.send_cmd_set(key.clone(), value.clone()) {
                    if attempts > 0 {
                        attempts -= 1;
                    } else {
                        panic!("{}", e.to_string())
                    }
                } else {
                    break;
                }
            }
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        } else {
            break;
        }
    }
    // Compaction triggered
    server_shutdown_trigger
        .trigger()
        .expect("unable to send shutdown trigger");
    server_join_handle
        .join()
        .expect("unable to join server thread");

    drop(server_shutdown_trigger);

    // reopen and check content
    let mut server = KvServer::new(
        KvStore::open(temp_dir.path()).expect("unable to open database file"),
        server_addr.as_str(),
        SharedQueueThreadPool::new(cpu_threads).expect(
            format!(
                "unable to initialize a thread pool with {} threads",
                cpu_threads
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

    let value = Some(format!("{}", last_iter));
    for key_id in 0..1000 {
        let key = format!("key{}", key_id);
        let mut attempts = 10;
        loop {
            match client.send_cmd_get(key.clone()) {
                Err(_) if attempts > 0 => attempts -= 1,
                Err(e) => panic!("{}", e.to_string()),
                Ok(recvd_val) => {
                    assert_eq!(recvd_val, value);
                    break;
                }
            }
        }
    }
    server_shutdown_trigger
        .trigger()
        .expect("unable to send shutdown trigger");
    server_join_handle
        .join()
        .expect("unable to join server thread");
}
