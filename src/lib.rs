#![deny(missing_docs)]

//! The kvs crate provides a persistent key-value store with a user-friendly interface.

#[macro_use]
extern crate failure;

pub use error::*;
pub use kvstore::*;

mod error;
mod kvstore;
