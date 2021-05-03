#![deny(missing_docs)]

//! This crate is composed of KvStore data structure and its methods

use std::collections::HashMap;

/// Data structure that implements a key-value store
#[derive(Debug)]
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    /// Creates an empty `KvStore`
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let dictionary = KvStore::new();
    ///
    /// ```
    pub fn new() -> Self {
        KvStore {
            data: HashMap::new(),
        }
    }

    /// Searches for an element based on the `key`.
    /// If the element was found, returns a copy of the associated value.
    /// If the element was not found, returns None.
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
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).map(|v| v.to_owned())
    }

    /// Inserts a new element with this `key` and `value`.
    /// If an element with the same `key` already existed,
    /// the `value` is updated.
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
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// Removes the element based on the `key` passed.
    /// If the `key` was not found, nothing happens.
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
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key);
    }
}
