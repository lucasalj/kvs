#![deny(missing_docs)]

//! The kvs crate provides a persistent key-value store with a user-friendly interface.

#[macro_use]
extern crate failure;

#[macro_use]
extern crate enum_primitive_derive;
extern crate num_traits;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

pub use error::*;
pub use kvclient::*;
pub use kvsengine::*;
pub use kvserver::*;
pub use kvstore::*;
pub use sledkvsengine::*;

pub mod cp;
mod error;
mod kvclient;
mod kvsengine;
mod kvserver;
mod kvstore;
mod sledkvsengine;
pub mod thread_pool;
