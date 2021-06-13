#![deny(missing_docs)]

//! The kvs crate provides a persistent key-value store with a user-friendly interface.

#[macro_use]
extern crate failure;

#[macro_use]
extern crate enum_primitive_derive;
extern crate num_traits;

pub use error::*;
pub use kvsengine::*;
pub use kvstore::*;

pub mod cp;
mod error;
mod kvsengine;
mod kvstore;
