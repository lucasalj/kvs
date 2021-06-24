use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    rc::Rc,
    result,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use chrono::Utc;
use walkdir::WalkDir;

use super::*;

use kvsengine::KvsEngine;

/// Log file names follow the pattern: `LOG_FILE_PREFIX` || date.fmt(%Y%m%d%H%M%S%f) || `LOG_FILE_SUFFIX`
const LOG_FILE_PREFIX: &str = "db";
const LOG_FILE_SUFFIX: &str = ".log";

/// The KvStore uses the constant `CMD_KEY_FACTOR` as a threshold to indicate the maximum proportion of commands
/// written to log files before triggering the compaction algorithm. The constant `CMDS_THRESHOLD` is used as a lower bound
/// on the total number of commands that must have been written to log files before trigerring compaction.
const CMD_KEY_FACTOR: f64 = 1.5;
const CMDS_THRESHOLD: u64 = 2000;

/// The constant `CURR_FILE_OFFSET_THRESHOLD` is used as a lower bound for the file write offset
/// to trigger the creation of a new file.
const CURR_FILE_OFFSET_THRESHOLD: u64 = 1073741824;

/// Data structure that implements a persistent key-value store
#[derive(Debug)]
pub struct KvStore {
    storage_index: HashMap<String, CommandIndex>,
    log_dir_path: PathBuf,
    old_logs: Vec<OldLogInfo>,
    curr_log: LogFile,
    total_cmd_counter: u64,
}

/// An alias for the result type that includes the common error type
pub type Result<T> = result::Result<T, KvStoreError>;

/// The type that is used to save the API calls: `set` and `remove` to the log files.
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// The location of the command in a log file
#[derive(Debug, Clone)]
struct CommandIndex {
    log_path: Rc<PathBuf>,
    offset: u64,
}

/// Information about a log file to be used by the compaction algorithm
#[derive(Debug)]
struct LogCompactionInfo {
    log_path: Rc<PathBuf>,
    keys: Vec<String>,
    cmd_counter: u64,
}

/// General information about the old log files
#[derive(Debug)]
struct OldLogInfo {
    path: Rc<PathBuf>,
    cmd_counter: u64,
}

/// Information about the current log file
#[derive(Debug)]
struct LogFile {
    path: Rc<PathBuf>,
    file: File,
    cmd_counter: u64,
    offset: u64,
}

impl KvStore {
    /// Open the KvStore at a given `path`.
    /// Return the KvStore.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kvs::KvStore;
    /// let dictionary = KvStore::open("./").unwrap();
    /// ```
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: Into<PathBuf>,
    {
        let path = (path.into() as PathBuf).canonicalize()?.join("");

        let (data_index, total_cmd_counter, mut old_log_files_data) =
            KvStore::build_index(path.clone())?;

        if let Some(OldLogInfo {
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
                storage_index: data_index,
                log_dir_path: path.clone(),
                old_logs: old_log_files_data,
                curr_log: LogFile {
                    path: curr_log_file_path,
                    file: curr_log_file,
                    cmd_counter: curr_log_cmd_counter,
                    offset: curr_log_offset,
                },
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
                storage_index: data_index,
                log_dir_path: path.into(),
                old_logs: old_log_files_data,
                curr_log: LogFile {
                    path: curr_log_file_path.clone(),
                    file: OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(curr_log_file_path.as_path())?,
                    cmd_counter: 0,
                    offset: 0,
                },
                total_cmd_counter,
            })
        }
    }

    /// Check if it should run compaction algorithm
    fn should_run_compaction(&self) -> bool {
        self.total_cmd_counter > CMDS_THRESHOLD
            && ((self.total_cmd_counter / (self.storage_index.len() as u64)) as f64)
                > CMD_KEY_FACTOR
    }

    /// Check if it should create a new log file
    fn should_create_new_file(&self) -> bool {
        self.curr_log.offset > CURR_FILE_OFFSET_THRESHOLD
            || self.curr_log.cmd_counter > (CMDS_THRESHOLD / 2)
    }

    /// Create a new log following the format for log file name and save the current log file information
    /// in memory.
    fn do_create_new_file(&mut self) -> Result<()> {
        self.old_logs.push(OldLogInfo {
            path: self.curr_log.path.clone(),
            cmd_counter: self.curr_log.cmd_counter,
        });
        let new_file_path = Rc::new(self.log_dir_path.join(format!(
            "{}{}{}",
            LOG_FILE_PREFIX,
            Utc::now().format("%Y%m%d%H%M%S%f"),
            LOG_FILE_SUFFIX
        )));
        self.curr_log = LogFile {
            path: new_file_path.clone(),
            file: OpenOptions::new()
                .append(true)
                .create(true)
                .open(new_file_path.as_path())?,
            cmd_counter: 0,
            offset: 0,
        };
        Ok(())
    }

    /// Runs the following compaction strategy:
    ///     1. Collect all keys that map to the current active values and the file names for the values.
    ///     2. Using the previous information, collect all log files information (except for the current one), which are their:
    ///        file name, active keys and their total number of commands written.
    ///     3. Sort the log files information by their number of active keys divided by their total number of commands.
    ///     4. For each file read all of its active keys, write them down to the current log and after that delete the file and
    ///        its information stored in memory.
    fn do_compaction(&mut self) -> Result<()> {
        let keys_by_file = self
            .storage_index
            .iter()
            .sorted_by(|(_, i1), (_, i2)| Ord::cmp(i1.log_path.as_path(), i2.log_path.as_path()))
            .group_by(|(_, i1)| i1.log_path.as_path())
            .into_iter()
            .map(|(p, i)| (p, i.map(|(k, _)| k.as_str()).collect_vec()))
            .collect::<HashMap<&Path, Vec<&str>>>();

        let mut files_compaction_info = self
            .old_logs
            .iter()
            .map(|log_file_data| LogCompactionInfo {
                log_path: log_file_data.path.clone(),
                keys: keys_by_file
                    .get(log_file_data.path.as_path())
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|s| String::from(*s))
                    .collect_vec(),
                cmd_counter: log_file_data.cmd_counter,
            })
            .collect::<Vec<_>>();

        files_compaction_info.sort_by(|fk1, fk2| {
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

        for fk in files_compaction_info {
            {
                let mut rdr = BufReader::new(
                    OpenOptions::new()
                        .read(true)
                        .create(false)
                        .open(fk.log_path.as_path())?,
                );
                for k in fk.keys.iter() {
                    rdr.seek(SeekFrom::Start(self.storage_index.get(k).unwrap().offset))?;
                    let cmd = bincode::deserialize_from::<_, Command>(&mut rdr)?;
                    let new_index = CommandIndex {
                        log_path: self.curr_log.path.clone(),
                        offset: self.curr_log.offset,
                    };
                    self.write_cmd_to_curr_log(cmd)?;
                    self.storage_index.insert(k.into(), new_index);

                    if self.should_create_new_file() {
                        self.do_create_new_file()?;
                    }
                }

                self.total_cmd_counter -= fk.cmd_counter;
                if let Ok(log_files_data_index) = self
                    .old_logs
                    .binary_search_by(|lfd| Ord::cmp(lfd.path.as_path(), fk.log_path.as_path()))
                {
                    self.old_logs.remove(log_files_data_index);
                }
            }
            fs::remove_file(fk.log_path.as_path())?;

            if !self.should_run_compaction() {
                break;
            }
        }

        Ok(())
    }

    /// Serializes the command using bincode crate and write it down to the current log
    fn write_cmd_to_curr_log(&mut self, cmd: Command) -> Result<()> {
        let cmd_serialized = bincode::serialize(&cmd)?;
        let mut writer = std::io::BufWriter::new(&mut self.curr_log.file);
        writer.write_all(&*cmd_serialized)?;
        self.total_cmd_counter += 1;
        self.curr_log.cmd_counter += 1;
        self.curr_log.offset += cmd_serialized.len() as u64;
        Ok(())
    }

    /// Given a file name and an offset, access that position in the log file and returns the value if found.
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

    /// Given a directory path, finds and reads all the log files and returns the storage index,
    /// the total number of commands written in the log files and a vec with information to be used
    /// later by the compaction algorithm about each log file.
    fn build_index<P>(dir_path: P) -> Result<(HashMap<String, CommandIndex>, u64, Vec<OldLogInfo>)>
    where
        P: AsRef<Path>,
    {
        let mut storage_index = HashMap::new();
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
                        storage_index.insert(
                            key,
                            CommandIndex {
                                log_path: log_file_path.clone(),
                                offset,
                            },
                        );
                    }
                    Ok(Command::Remove { key }) => {
                        total_cmds_counter += 1;
                        log_file_cmd_counter += 1;
                        storage_index.remove(&*key);
                    }
                    Err(err) => match *err {
                        bincode::ErrorKind::Io(ref bincode_io_err) => match bincode_io_err.kind() {
                            std::io::ErrorKind::UnexpectedEof => {
                                log_files_data.push(OldLogInfo {
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
        Ok((storage_index, total_cmds_counter, log_files_data))
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let new_index = CommandIndex {
            log_path: self.curr_log.path.clone(),
            offset: self.curr_log.offset,
        };

        let cmd = Command::Set {
            key: key.clone(),
            value,
        };

        self.write_cmd_to_curr_log(cmd)?;

        self.storage_index.insert(key, new_index);

        if self.should_run_compaction() {
            self.do_compaction()?;
        }

        if self.should_create_new_file() {
            self.do_create_new_file()?;
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.storage_index.get(&key).map(|lci| lci.clone()) {
            Some(lci) => Ok(Some(
                self.read_value_from_log_at(lci.log_path.as_path(), lci.offset)?,
            )),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.storage_index.remove(&key) {
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
}
