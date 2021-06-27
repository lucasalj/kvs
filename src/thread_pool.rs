//! Thread Pool trait and implementations

use super::Result;

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
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
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
