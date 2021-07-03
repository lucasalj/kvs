//! Thread Pool trait and implementations

use std::{convert, fmt};

use crossbeam::channel::*;
use rayon::ThreadPoolBuildError;
type Result<T> = std::result::Result<T, ThreadPoolError>;

/// Thread Pool minimal API
pub trait ThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned threads are
    /// terminated.
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// bash zip function tupleSpawn a function into the threadpool.
    ///
    /// Spawning always succeeds, but if the function panics the threadpool
    /// continues to operate with the same number of threads — the thread
    /// count is not reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// An error type returned by the Thread Pool api
#[derive(Debug)]
pub enum ThreadPoolError {
    /// An error returned by the rayon crate while building a thread pool
    RayonThreadPoolBuildErr(rayon::ThreadPoolBuildError),
}

impl fmt::Display for ThreadPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            ThreadPoolError::RayonThreadPoolBuildErr(err) => f.write_fmt(format_args!("{}", err)),
        }
    }
}

impl std::error::Error for ThreadPoolError {}

impl convert::From<ThreadPoolBuildError> for ThreadPoolError {
    fn from(err: ThreadPoolBuildError) -> Self {
        ThreadPoolError::RayonThreadPoolBuildErr(err)
    }
}

/// Naive implementation of a Thread Pool
///
/// It juts immediatly spawns a new thread every time spawn function is called.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let _ = threads;
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::spawn(job);
    }
}

/// A more sophisticated thread pool with a shared work queue
pub struct SharedQueueThreadPool {
    channel_sender: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

struct PanicGuard {
    channel_receiver: Receiver<Box<dyn FnOnce() + Send + 'static>>,
}

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let r = self.channel_receiver.clone();
            std::thread::spawn(move || loop {
                match r.recv() {
                    Ok(task) => {
                        let _panic_guard = PanicGuard {
                            channel_receiver: r.clone(),
                        };
                        task();
                    }
                    _ => break,
                }
            });
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver): (
            Sender<Box<dyn FnOnce() + Send + 'static>>,
            Receiver<Box<dyn FnOnce() + Send + 'static>>,
        ) = unbounded();
        for _ in 0..threads {
            let r = receiver.clone();
            std::thread::spawn(move || loop {
                match r.recv() {
                    Ok(task) => {
                        let _panic_guard = PanicGuard {
                            channel_receiver: r.clone(),
                        };
                        task();
                    }
                    _ => break,
                }
            });
        }
        Ok(SharedQueueThreadPool {
            channel_sender: sender,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.channel_sender.send(Box::new(job)).unwrap();
    }
}

/// A more sophisticated thread pool that uses the rayon crate
pub struct RayonThreadPool {
    thread_pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(Self {
            thread_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()?,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread_pool.spawn(job)
    }
}
