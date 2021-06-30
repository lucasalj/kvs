//! Thread Pool trait and implementations

use super::Result;
use crossbeam::channel::*;

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
            let r2 = self.channel_receiver.clone();
            std::thread::spawn(move || {
                let _panic_guard = PanicGuard {
                    channel_receiver: r2,
                };
                loop {
                    let task = r.recv().unwrap();
                    task();
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
            let r2 = receiver.clone();
            std::thread::spawn(move || {
                let _panic_guard = PanicGuard {
                    channel_receiver: r2,
                };
                loop {
                    let task = r.recv().unwrap();
                    task();
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
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let _ = threads;
        todo!()
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = job;
        todo!()
    }
}
