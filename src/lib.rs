// #![deny(missing_docs)]

//! This crate is composed of KvStore data structure and its methods

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{BufReader, Write},
};
use std::{path::PathBuf, result};

#[macro_use]
extern crate failure;

use serde::{Deserialize, Serialize};

const LOG_FILE_NAME: &str = "db.log";

/// An error type for any errors returned by the KvStore API
#[derive(Debug, Fail)]
pub enum KvStoreError {
    #[fail(display = "Bincode error: {}.", _0)]
    Bincode(#[cause] bincode::Error),
    #[fail(display = "Io error: {}.", _0)]
    Io(#[cause] std::io::Error),
    #[fail(display = "Tried to remove a non existent key in the database")]
    RemoveNonExistentKey,
}

impl From<bincode::Error> for KvStoreError {
    fn from(error: bincode::Error) -> Self {
        Self::Bincode(error)
    }
}

impl From<std::io::Error> for KvStoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// Data structure that implements a key-value store
#[derive(Debug)]
pub struct KvStore {
    data_index: HashMap<String, String>,
    log_file_path: PathBuf,
    log_file: std::fs::File,
    is_index_built: bool,
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
    /// use kvs::Result;
    ///
    /// if let Ok(ref mut user_data) = KvStore::open("./db.log") {
    ///     user_data.set("name".to_owned(), "John".to_owned());
    ///     user_data.set("age".to_owned(), "21".to_owned());
    ///     assert_eq!(user_data.get("name".to_owned()).unwrap(), Some("John".to_owned()));
    ///
    ///     user_data.set("age".to_owned(), "22".to_owned());
    ///     assert_eq!(user_data.get("age".to_owned()).unwrap(), Some("22".to_owned()));
    /// }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.write_cmd_to_file(Command::Set {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.data_index.insert(key, value);

        Ok(())
    }

    fn write_cmd_to_file(&mut self, cmd: Command) -> Result<()> {
        let cmd_serialized = bincode::serialize(&cmd)?;
        self.log_file.write_all(&*cmd_serialized)?;
        Ok(())
    }

    /// Get the string value of a string `key`.
    /// If the `key` does not exist, return None.
    /// Return an error if the value is not read successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut user_data = KvStore::open("./db.log").unwrap();
    /// user_data.set("name".to_owned(), "John".to_owned());
    /// user_data.set("age".to_owned(), "21".to_owned());
    /// assert_eq!(user_data.get("name".to_owned()).unwrap(), Some("John".to_owned()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.is_index_built {
            self.build_index()?;
        }
        Ok(self.data_index.get(&key).and_then(|val| Some(val.clone())))
    }

    fn build_index(&mut self) -> Result<()> {
        let mut reader = BufReader::new(&self.log_file);
        loop {
            let res = bincode::deserialize_from::<_, Command>(&mut reader);
            match res {
                Ok(Command::Set { key, value }) => {
                    self.data_index.insert(key, value);
                }
                Ok(Command::Remove { key }) => {
                    self.data_index.remove(&*key);
                }
                Err(err) => match *err {
                    bincode::ErrorKind::Io(ref bincode_io_err) => match bincode_io_err.kind() {
                        std::io::ErrorKind::UnexpectedEof => {
                            return Ok(());
                        }
                        _ => return Err(KvStoreError::from(err)),
                    },
                    _ => return Err(KvStoreError::from(err)),
                },
            }
        }
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
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.is_index_built {
            self.build_index()?;
        }
        self.write_cmd_to_file(Command::Remove { key: key.clone() })?;

        if let None = self.data_index.remove(&key) {
            Err(KvStoreError::RemoveNonExistentKey)
        } else {
            Ok(())
        }
    }

    /// Open the KvStore at a given `path`.
    /// Return the KvStore.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let dictionary = KvStore::new();
    /// ```
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let path = (path.into() as PathBuf).join(LOG_FILE_NAME);
        Ok(KvStore {
            data_index: HashMap::new(),
            log_file_path: path.clone(),
            log_file: OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(path)?,
            is_index_built: false,
        })
    }
}
