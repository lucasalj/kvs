use super::*;
use itertools::Itertools;
use kvsengine::KvsEngine;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    result,
    sync::Arc,
};
use walkdir::WalkDir;

/// Log file names follow the pattern: `LOG_FILE_PREFIX` || id || `LOG_FILE_SUFFIX`
const LOG_FILE_PREFIX: &str = "db";
const LOG_FILE_SUFFIX: &str = ".log";

/// The KvStore uses the constant `CMD_KEY_FACTOR` as a threshold to indicate the maximum proportion of commands
/// written to log files before triggering the compaction algorithm.
const CMD_KEY_FACTOR: f64 = 1.5;
/// The constant `CMDS_THRESHOLD` is used as a lower bound on the total number of commands
/// that must have been written to log files before trigerring compaction. Also, `CMDS_THRESHOLD/2` is also used
/// to trigger the creation of a new file.
const CMDS_THRESHOLD: u64 = 10000;

/// The constant `CURR_FILE_OFFSET_THRESHOLD` is used as a lower bound for the file write offset
/// to trigger the creation of a new file.
const CURR_FILE_OFFSET_THRESHOLD: u64 = 1073741824;

/// Data structure that implements a persistent key-value store
#[derive(Debug, Clone)]
pub struct KvStore {
    db: Arc<Mutex<Box<KvStoreDb>>>,
}

#[derive(Debug)]
struct KvStoreDb {
    storage_index: HashMap<String, CommandIndex>,
    log_dir_path: PathBuf,
    old_logs: Vec<OldLogInfo>,
    curr_log_w: LogFileWriter,
    curr_log_r: LogFileReader,
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
    log_id: u64,
    offset: u64,
}

/// Information about a log file to be used by the compaction algorithm
#[derive(Debug)]
struct LogCompactionInfo {
    log_id: u64,
    keys: Vec<String>,
    cmd_counter: u64,
}

/// General information about the old log files
#[derive(Debug)]
struct OldLogInfo {
    id: u64,
    cmd_counter: u64,
}

/// Information about the current log file writer
#[derive(Debug)]
struct LogFileWriter {
    id: u64,
    writer: BufWriter<File>,
    cmd_counter: u64,
    offset: u64,
}

/// Information about the current log file reader
#[derive(Debug)]
struct LogFileReader {
    id: u64,
    reader: BufReader<File>,
    offset: u64,
}

impl KvStoreDb {
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
        let log_dir_path = (path.into() as PathBuf).canonicalize()?.join("");

        let (storage_index, total_cmd_counter, mut old_logs) =
            KvStoreDb::build_index(log_dir_path.as_path())?;

        if let Some(OldLogInfo { id, cmd_counter }) = old_logs.pop() {
            // There already exists a file, just open the last (current) one
            let file_path = KvStoreDb::format_log_path(log_dir_path.as_path(), id);
            let mut writer = BufWriter::new(
                OpenOptions::new()
                    .append(true)
                    .create(false)
                    .open(file_path.as_path())?,
            );
            let offset = writer.seek(SeekFrom::End(0))?;
            let reader = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .create(false)
                    .open(file_path.as_path())?,
            );

            Ok(KvStoreDb {
                storage_index,
                log_dir_path,
                old_logs,
                curr_log_w: LogFileWriter {
                    id,
                    writer,
                    cmd_counter,
                    offset,
                },
                curr_log_r: LogFileReader {
                    id,
                    reader,
                    offset: 0,
                },
                total_cmd_counter,
            })
        } else {
            // Create new log file, because there is none
            let file_path = KvStoreDb::format_log_path(log_dir_path.as_path(), 0);
            let writer = BufWriter::new(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(file_path.as_path())?,
            );
            let reader = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .create(false)
                    .open(file_path.as_path())?,
            );

            Ok(KvStoreDb {
                storage_index,
                log_dir_path,
                old_logs,
                curr_log_w: LogFileWriter {
                    id: 0,
                    writer,
                    cmd_counter: 0,
                    offset: 0,
                },
                curr_log_r: LogFileReader {
                    id: 0,
                    reader,
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
        self.curr_log_w.offset > CURR_FILE_OFFSET_THRESHOLD
            || self.curr_log_w.cmd_counter > (CMDS_THRESHOLD / 2)
    }

    /// Create a new log following the format for log file name and save the current log file information
    /// in memory.
    fn do_create_new_file(&mut self) -> Result<()> {
        self.old_logs.push(OldLogInfo {
            id: self.curr_log_w.id,
            cmd_counter: self.curr_log_w.cmd_counter,
        });
        let new_file_id = self.curr_log_w.id + 1;
        let new_file_path = KvStoreDb::format_log_path(self.log_dir_path.as_path(), new_file_id);
        self.curr_log_w = LogFileWriter {
            id: new_file_id,
            writer: BufWriter::new(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(new_file_path.as_path())?,
            ),
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
        let keys_by_file_id = self
            .storage_index
            .iter()
            .sorted_by(|(_, i1), (_, i2)| Ord::cmp(&i1.log_id, &i2.log_id))
            .group_by(|(_, i1)| i1.log_id)
            .into_iter()
            .map(|(p, i)| (p, i.map(|(k, _)| k.as_str()).collect_vec()))
            .collect::<HashMap<u64, Vec<&str>>>();

        let mut files_compaction_info = self
            .old_logs
            .iter()
            .map(|log_file_data| LogCompactionInfo {
                log_id: log_file_data.id,
                keys: keys_by_file_id
                    .get(&log_file_data.id)
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
            let log_path = KvStoreDb::format_log_path(self.log_dir_path.as_path(), fk.log_id);
            {
                let mut rdr = BufReader::new(
                    OpenOptions::new()
                        .read(true)
                        .create(false)
                        .open(log_path.as_path())?,
                );
                for k in fk.keys.iter() {
                    rdr.seek(SeekFrom::Start(self.storage_index.get(k).unwrap().offset))?;
                    let cmd = bincode::deserialize_from::<_, Command>(&mut rdr)?;
                    let new_index = CommandIndex {
                        log_id: self.curr_log_w.id,
                        offset: self.curr_log_w.offset,
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
                    .binary_search_by(|lfd| Ord::cmp(&lfd.id, &fk.log_id))
                {
                    self.old_logs.remove(log_files_data_index);
                }
            }
            fs::remove_file(log_path.as_path())?;

            if !self.should_run_compaction() {
                break;
            }
        }

        Ok(())
    }

    /// Serializes the command using bincode crate and write it down to the current log
    fn write_cmd_to_curr_log(&mut self, cmd: Command) -> Result<()> {
        let cmd_size = bincode::serialized_size(&cmd)?;
        bincode::serialize_into(&mut self.curr_log_w.writer, &cmd)?;
        self.total_cmd_counter += 1;
        self.curr_log_w.cmd_counter += 1;
        self.curr_log_w.offset += cmd_size;
        Ok(())
    }

    /// Given a file name and an offset, access that position in the log file and returns the value if found.
    fn read_value_from_log_at(&mut self, log_id: u64, offset: u64) -> Result<String> {
        if log_id != self.curr_log_r.id {
            let file_path = KvStoreDb::format_log_path(self.log_dir_path.as_path(), log_id);
            let file_reader = BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .create(false)
                    .open(file_path)?,
            );
            self.curr_log_r = LogFileReader {
                id: log_id,
                reader: file_reader,
                offset: 0,
            };
        }
        if offset != self.curr_log_r.offset {
            self.curr_log_r.reader.seek(SeekFrom::Start(offset))?;
            self.curr_log_r.offset = offset;
        }
        let cmd = bincode::deserialize_from::<_, Command>(&mut self.curr_log_r.reader)?;
        self.curr_log_r.offset += bincode::serialized_size(&cmd)?;
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
            let log_file_path = dir_path.as_ref().to_path_buf().join(log_file_entry?.path());
            let file_name_str = log_file_path
                .file_name()
                .ok_or(KvStoreError::WrongFileNameFormat)?
                .to_str()
                .ok_or(KvStoreError::WrongFileNameFormat)?;
            let log_id = file_name_str
                [LOG_FILE_PREFIX.len()..(file_name_str.len() - LOG_FILE_SUFFIX.len())]
                .parse::<u64>()
                .map_err(|_| KvStoreError::WrongFileNameFormat)?;
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
                        storage_index.insert(key, CommandIndex { log_id, offset });
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
                                    id: log_id,
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

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let new_index = CommandIndex {
            log_id: self.curr_log_w.id,
            offset: self.curr_log_w.offset,
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

        self.curr_log_w.writer.flush()?;

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self
            .storage_index
            .get(&key)
            .map(|lci| (lci.log_id, lci.offset))
        {
            Some((id, offset)) => Ok(Some(self.read_value_from_log_at(id, offset)?)),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.storage_index.remove(&key) {
            Err(KvStoreError::RemoveNonExistentKey)
        } else {
            self.write_cmd_to_curr_log(Command::Remove { key })?;

            if self.should_run_compaction() {
                self.do_compaction()?;
            }

            if self.should_create_new_file() {
                self.do_create_new_file()?;
            }

            self.curr_log_w.writer.flush()?;

            Ok(())
        }
    }

    fn format_log_path<P: Into<PathBuf>>(log_dir: P, log_id: u64) -> PathBuf {
        log_dir
            .into()
            .join(format!("{}{}{}", LOG_FILE_PREFIX, log_id, LOG_FILE_SUFFIX))
    }
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
        Ok(KvStore {
            db: Arc::new(Mutex::new(Box::new(KvStoreDb::open(path)?))),
        })
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.lock().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.db.lock().get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.lock().remove(key)
    }
}
