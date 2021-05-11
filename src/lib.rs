// #![deny(missing_docs)]

//! This crate is composed of KvStore data structure and its methods

#[macro_use]
extern crate failure;

pub use error::*;
pub use kvstore::*;

mod error;
mod kvstore;
