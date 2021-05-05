#![deny(missing_docs)]

//! This crate is composed of KvStore data structure and its methods

use std::result;
use std::{collections::HashMap, path::PathBuf};

/// An error type for any errors returned by the KvStore API
#[derive(Debug)]
pub enum KvStoreError {}

/// Data structure that implements a key-value store
#[derive(Debug)]
pub struct KvStore {
    data: HashMap<String, String>,
}

/// An alias for the result type that includes the common error type
pub type Result<T> = result::Result<T, KvStoreError>;

impl KvStore {
    /// Set the `value` of a string `key` to a string.
    /// Return an error if the `value` is not written successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::new();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// user_data.set("age".to_owned(), "21".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()), Some("John".to_owned()));
    ///
    /// user_data.set("age".to_owned(), "22".to_owned());
    /// assert_eq!(user_data.get("age".to_owned()), Some("22".to_owned()));
    /// ```
    pub fn set(&mut self, _key: String, _value: String) -> Result<()> {
        // self.data.insert(key, value);
        panic!("unimplemented");
    }

    /// Get the string value of a string `key`.
    /// If the `key` does not exist, return None.
    /// Return an error if the value is not read successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::new();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// user_data.set("age".to_owned(), "21".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()), Some("John".to_owned()));
    /// assert_eq!(user_data.get("mother's name".to_owned()), None);
    /// ```
    pub fn get(&mut self, _key: String) -> Result<Option<String>> {
        // self.data.get(&key).map(|v| v.to_owned())
        panic!("unimplemented");
    }

    /// Remove a given `key`.
    /// Return an error if the key does not exist or is not removed successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::new();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()), Some("John".to_owned()));
    ///
    /// user_data.remove("name".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()), None);
    /// ```
    pub fn remove(&mut self, _key: String) -> Result<()> {
        // self.data.remove(&key);
        panic!("unimplemented");
    }

    /// Open the KvStore at a given `path`.
    /// Return the KvStore.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let dictionary = KvStore::new();
    ///
    /// ```
    pub fn open(_path: impl Into<PathBuf>) -> Result<Self> {
        // KvStore {
        //     data: HashMap::new(),
        // }
        panic!("unimplemented");
    }
}
