#![deny(missing_docs)]

//! This crate is composed of KvStore data structure and its methods

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{self, OpenOptions},
    io::{BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    rc::Rc,
    result,
};

#[macro_use]
extern crate failure;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use chrono::Utc;
use walkdir::WalkDir;

const LOG_FILE_PREFIX: &str = "db";
const LOG_FILE_SUFFIX: &str = ".log";
const CMD_KEY_FACTOR: f64 = 1.5;
const CMDS_THRESHOLD: u64 = 1000;
const CURR_FILE_OFFSET_THRESHOLD: u64 = 256;
/// Data structure that implements a key-value store
#[derive(Debug)]
pub struct KvStore {
    data_index: HashMap<String, LogCmdIndex>,
    log_dir_path: PathBuf,
    old_log_files_data: Vec<LogFileData>,
    curr_log_file_path: Rc<PathBuf>,
    curr_log_file: std::fs::File,
    curr_log_cmd_counter: u64,
    curr_log_offset: u64,
    total_cmd_counter: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug, Clone)]
struct LogCmdIndex {
    log_file_path: Rc<PathBuf>,
    offset: u64,
}

#[derive(Debug)]
struct FileKeys {
    log_file_path: Rc<PathBuf>,
    keys: Vec<String>,
    cmd_counter: u64,
}

#[derive(Debug)]
struct LogFileData {
    path: Rc<PathBuf>,
    cmd_counter: u64,
}

/// An alias for the result type that includes the common error type
pub type Result<T> = result::Result<T, KvStoreError>;

/// An error type for any errors returned by the KvStore API
#[derive(Debug, Fail)]
pub enum KvStoreError {
    /// An error that came from bincode crate
    #[fail(display = "Bincode error: {}.", _0)]
    Bincode(#[cause] bincode::Error),
    /// An error that came from std::io
    #[fail(display = "Io error: {}.", _0)]
    Io(#[cause] std::io::Error),
    /// An error returned when the user tried to remove a key not found in the database
    #[fail(display = "Tried to remove a non existent key in the database.")]
    RemoveNonExistentKey,
    /// An error returned when tried to read the file at an invalid offset
    #[fail(display = "Wrong file offset. File must have been corrupted.")]
    WrongFileOffset,
    /// An error that came from the waldir crate
    #[fail(display = "Walkdir error: {}.", _0)]
    Walkdir(#[cause] walkdir::Error),
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

impl From<walkdir::Error> for KvStoreError {
    fn from(error: walkdir::Error) -> Self {
        Self::Walkdir(error)
    }
}

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
        let new_index = LogCmdIndex {
            log_file_path: self.curr_log_file_path.clone(),
            offset: self.curr_log_offset,
        };
        self.write_cmd_to_curr_log(Command::Set {
            key: key.clone(),
            value,
        })?;

        self.data_index.insert(key, new_index);

        if self.should_run_compaction() {
            self.do_compaction()?;
        }

        if self.should_create_new_file() {
            self.do_create_new_file()?;
        }

        Ok(())
    }

    fn should_run_compaction(&self) -> bool {
        self.total_cmd_counter > CMDS_THRESHOLD
            && ((self.total_cmd_counter / (self.data_index.len() as u64)) as f64) > CMD_KEY_FACTOR
    }

    fn should_create_new_file(&self) -> bool {
        self.curr_log_offset > CURR_FILE_OFFSET_THRESHOLD
    }

    fn do_create_new_file(&mut self) -> Result<()> {
        self.old_log_files_data.push(LogFileData {
            path: self.curr_log_file_path.clone(),
            cmd_counter: self.curr_log_cmd_counter,
        });
        self.curr_log_file_path = Rc::new(self.log_dir_path.join(format!(
            "{}{}{}",
            LOG_FILE_PREFIX,
            Utc::now().format("%Y%m%d%H%M%S%f"),
            LOG_FILE_SUFFIX
        )));
        self.curr_log_cmd_counter = 0;
        self.curr_log_offset = 0;
        self.curr_log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.curr_log_file_path.as_path())?;
        Ok(())
    }

    fn do_compaction(&mut self) -> Result<()> {
        let mut keys_by_file = self
            .data_index
            .iter()
            .sorted_by(|(_, i1), (_, i2)| {
                Ord::cmp(i1.log_file_path.as_path(), i2.log_file_path.as_path())
            })
            .group_by(|(_, i1)| i1.log_file_path.as_path().to_path_buf())
            .into_iter()
            .map(|(p, i)| (p, i.map(|(k, _)| k.clone()).collect_vec()))
            .collect::<HashMap<PathBuf, Vec<String>>>();

        let mut all_file_keys = self
            .old_log_files_data
            .iter()
            .map(|log_file_data| FileKeys {
                log_file_path: log_file_data.path.clone(),
                keys: keys_by_file
                    .remove(log_file_data.path.as_path())
                    .unwrap_or(vec![]),
                cmd_counter: log_file_data.cmd_counter,
            })
            .collect::<Vec<_>>();

        all_file_keys.sort_by(|fk1, fk2| {
            let fk1_cmd_key_factor = match fk1.keys.len() {
                0 => std::u64::MAX,
                _ => fk1.cmd_counter / (fk1.keys.len() as u64),
            };
            let fk2_cmd_key_factor = match fk2.keys.len() {
                0 => std::u64::MAX,
                _ => fk2.cmd_counter / (fk2.keys.len() as u64),
            };
            Ord::cmp(&fk2_cmd_key_factor, &fk1_cmd_key_factor)
        });

        for fk in all_file_keys {
            {
                let mut rdr = BufReader::new(
                    OpenOptions::new()
                        .read(true)
                        .create(false)
                        .open(fk.log_file_path.as_path())?,
                );
                for k in fk.keys.iter() {
                    rdr.seek(SeekFrom::Start(self.data_index.get(k).unwrap().offset))?;
                    let cmd = bincode::deserialize_from::<_, Command>(&mut rdr)?;
                    let new_index = LogCmdIndex {
                        log_file_path: self.curr_log_file_path.clone(),
                        offset: self.curr_log_offset,
                    };
                    self.write_cmd_to_curr_log(cmd)?;
                    self.data_index.insert(k.into(), new_index);

                    if self.should_create_new_file() {
                        self.do_create_new_file()?;
                    }
                }

                self.total_cmd_counter -= fk.cmd_counter;
                if let Ok(log_files_data_index) = self.old_log_files_data.binary_search_by(|lfd| {
                    Ord::cmp(lfd.path.as_path(), fk.log_file_path.as_path())
                }) {
                    self.old_log_files_data.remove(log_files_data_index);
                }
            }
            fs::remove_file(fk.log_file_path.as_path())?;

            if !self.should_run_compaction() {
                break;
            }
        }

        Ok(())
    }

    fn write_cmd_to_curr_log(&mut self, cmd: Command) -> Result<()> {
        let cmd_serialized = bincode::serialize(&cmd)?;
        self.curr_log_file.write_all(&*cmd_serialized)?;
        self.total_cmd_counter += 1;
        self.curr_log_cmd_counter += 1;
        self.curr_log_offset += cmd_serialized.len() as u64;
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
        match self.data_index.get(&key).map(|lci| lci.clone()) {
            Some(lci) => Ok(Some(
                self.read_value_from_log_at(lci.log_file_path.as_path(), lci.offset)?,
            )),
            None => Ok(None),
        }
    }

    fn read_value_from_log_at<P>(&mut self, file_path: P, offset: u64) -> Result<String>
    where
        P: AsRef<Path>,
    {
        let mut rdr = BufReader::new(
            OpenOptions::new()
                .read(true)
                .create(false)
                .open(file_path.as_ref())?,
        );
        rdr.seek(SeekFrom::Start(offset))?;
        let cmd = bincode::deserialize_from::<_, Command>(rdr)?;
        if let Command::Set { key: _, value } = cmd {
            Ok(value)
        } else {
            Err(KvStoreError::WrongFileOffset)
        }
    }

    fn build_index<P>(dir_path: P) -> Result<(HashMap<String, LogCmdIndex>, u64, Vec<LogFileData>)>
    where
        P: AsRef<Path>,
    {
        let mut data_index = HashMap::new();
        let log_file_entries = WalkDir::new(&dir_path)
            .min_depth(1)
            .max_depth(1)
            .sort_by(|e1, e2| e1.file_name().cmp(e2.file_name()))
            .into_iter()
            .filter_entry(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.starts_with(LOG_FILE_PREFIX) && s.ends_with(LOG_FILE_SUFFIX))
                    .unwrap_or(false)
            });

        let mut total_cmds_counter = 0;
        let mut log_files_data = vec![];
        for log_file_entry in log_file_entries {
            let mut log_file_cmd_counter: u64 = 0;
            let log_file_path =
                Rc::new(dir_path.as_ref().to_path_buf().join(log_file_entry?.path()));
            let mut log_file = OpenOptions::new()
                .read(true)
                .open(log_file_path.as_path())?;
            let mut reader = BufReader::new(&mut log_file);

            loop {
                let offset = reader.seek(SeekFrom::Current(0))?;
                let res = bincode::deserialize_from::<_, Command>(&mut reader);
                match res {
                    Ok(Command::Set { key, value: _ }) => {
                        total_cmds_counter += 1;
                        log_file_cmd_counter += 1;
                        data_index.insert(
                            key,
                            LogCmdIndex {
                                log_file_path: log_file_path.clone(),
                                offset,
                            },
                        );
                    }
                    Ok(Command::Remove { key }) => {
                        total_cmds_counter += 1;
                        log_file_cmd_counter += 1;
                        data_index.remove(&*key);
                    }
                    Err(err) => match *err {
                        bincode::ErrorKind::Io(ref bincode_io_err) => match bincode_io_err.kind() {
                            std::io::ErrorKind::UnexpectedEof => {
                                log_files_data.push(LogFileData {
                                    path: log_file_path,
                                    cmd_counter: log_file_cmd_counter,
                                });
                                break;
                            }
                            _ => return Err(KvStoreError::from(err)),
                        },
                        _ => return Err(KvStoreError::from(err)),
                    },
                }
            }
        }
        Ok((data_index, total_cmds_counter, log_files_data))
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
        if let None = self.data_index.remove(&key) {
            Err(KvStoreError::RemoveNonExistentKey)
        } else {
            self.write_cmd_to_curr_log(Command::Remove { key: key.clone() })?;

            if self.should_run_compaction() {
                self.do_compaction()?;
            }

            if self.should_create_new_file() {
                self.do_create_new_file()?;
            }
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
        let path = (path.into() as PathBuf).canonicalize()?.join("");

        let (data_index, total_cmd_counter, mut old_log_files_data) =
            KvStore::build_index(path.clone())?;

        if let Some(LogFileData {
            path: curr_log_file_path,
            cmd_counter: curr_log_cmd_counter,
        }) = old_log_files_data.pop()
        {
            // There already exists a file, just open the last (current) one
            let mut curr_log_file = OpenOptions::new()
                .append(true)
                .create(false)
                .open(curr_log_file_path.as_path())?;
            let curr_log_offset = curr_log_file.seek(SeekFrom::End(0))?;
            Ok(KvStore {
                data_index,
                log_dir_path: path.clone(),
                old_log_files_data,
                curr_log_file_path,
                curr_log_file,
                curr_log_cmd_counter,
                curr_log_offset,
                total_cmd_counter,
            })
        } else {
            // Create new log file, because there is none
            let log_dir_path = path.clone();
            let curr_log_file_path = Rc::new(log_dir_path.join(format!(
                "{}{}{}",
                LOG_FILE_PREFIX,
                Utc::now().format("%Y%m%d%H%M%S%f"),
                LOG_FILE_SUFFIX
            )));

            Ok(KvStore {
                data_index,
                log_dir_path: path.into(),
                old_log_files_data,
                curr_log_file_path: curr_log_file_path.clone(),
                curr_log_file: OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(curr_log_file_path.as_path())?,
                curr_log_cmd_counter: 0,
                curr_log_offset: 0,
                total_cmd_counter,
            })
        }
    }
}
