use super::*;
use itertools::Itertools;
use kvsengine::KvsEngine;
use parking_lot::Mutex;
use positioned_io::ReadAt;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use std::{
    collections::{HashMap as StdHashMap, HashSet as StdHashSet},
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
    storage_index: StdHashMap<String, CommandIndex>,
    log_dir_path: PathBuf,
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
    len: u64,
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
    reader: File,
}

impl LogFileReader {
    fn new<P>(id: u64, log_path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            id,
            reader: OpenOptions::new()
                .read(true)
                .create(false)
                .open(log_path.as_ref())?,
        })
    }

    fn read_cmd_at(&self, offset: u64, len: u64) -> Result<Command> {
        let mut buf: SmallVec<[u8; 512]> = smallvec![0u8; len as usize];
        self.reader.read_exact_at(offset, &mut buf[..])?;
        Ok(bincode::deserialize(&buf)?)
    }

    /// Get the log file reader's id.
    fn id(&self) -> u64 {
        self.id
    }
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
            let curr_log_r = LogFileReader::new(id, file_path.as_path())?;

            Ok(KvStoreDb {
                storage_index,
                log_dir_path,
                curr_log_w: LogFileWriter {
                    id,
                    writer,
                    cmd_counter,
                    offset,
                },
                curr_log_r,
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
            let curr_log_r = LogFileReader::new(0, file_path.as_path())?;

            Ok(KvStoreDb {
                storage_index,
                log_dir_path,
                curr_log_w: LogFileWriter {
                    id: 0,
                    writer,
                    cmd_counter: 0,
                    offset: 0,
                },
                curr_log_r,
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
    ///     1. Collect all command offsets related to each active entry from the storage index map and rearrange them in a map where
    ///        the keys are the log file ids and the value is a set of offsets of each active entry in that corresponding file.
    ///     2. List all the log file names and ids in the database directory sorted by name.
    ///     3. For each file read all of its commands, if the command is a set and refers to an active entry, write it down to
    ///        the current log and after processing all of the file commands, delete the file.
    fn do_compaction(&mut self) -> Result<()> {
        let active_file_id_offsets_map = self.get_active_file_id_offsets_map();
        let curr_log_id = self.curr_log_w.id;

        let log_ids_files = KvStoreDb::list_log_ids_files_sorted(self.log_dir_path.as_path())
            .filter(|(id, _)| *id < curr_log_id)
            .collect::<Vec<_>>();

        for (log_id, log_path) in log_ids_files {
            {
                let mut reader = BufReader::new(
                    OpenOptions::new()
                        .read(true)
                        .create(false)
                        .open(log_path.as_path())?,
                );
                let mut log_file_cmd_counter = 0;
                let active_offsets = active_file_id_offsets_map.get(&log_id);
                loop {
                    let offset = reader.seek(SeekFrom::Current(0))?;
                    let res = bincode::deserialize_from::<_, Command>(&mut reader);
                    let is_active_entry = active_offsets.and_then(|hs| hs.get(&offset)).is_some();
                    match res {
                        Ok(Command::Set { key, value }) => {
                            log_file_cmd_counter += 1;
                            if is_active_entry {
                                self.insert_entry(key, value)?;
                            }
                        }
                        Ok(Command::Remove { key: _ }) => {
                            log_file_cmd_counter += 1;
                        }
                        Err(err) => match *err {
                            bincode::ErrorKind::Io(ref bincode_io_err) => {
                                match bincode_io_err.kind() {
                                    std::io::ErrorKind::UnexpectedEof => break,
                                    _ => return Err(KvStoreError::from(err)),
                                }
                            }
                            _ => return Err(KvStoreError::from(err)),
                        },
                    }
                }
                self.total_cmd_counter -= log_file_cmd_counter;
            }
            fs::remove_file(log_path.as_path())?;
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
    fn read_value_from_log_at(&mut self, log_id: u64, offset: u64, len: u64) -> Result<String> {
        if log_id != self.curr_log_r.id() {
            let file_path = KvStoreDb::format_log_path(self.log_dir_path.as_path(), log_id);
            self.curr_log_r = LogFileReader::new(log_id, file_path)?;
        }
        let cmd = self.curr_log_r.read_cmd_at(offset, len)?;
        if let Command::Set { key: _, value } = cmd {
            Ok(value)
        } else {
            Err(KvStoreError::WrongFileOffset)
        }
    }

    /// Given a directory path, finds and reads all the log files and returns the storage index,
    /// the total number of commands written in the log files and a vec with information to be used
    /// later by the compaction algorithm about each log file.
    fn build_index<P>(
        dir_path: P,
    ) -> Result<(StdHashMap<String, CommandIndex>, u64, Vec<OldLogInfo>)>
    where
        P: AsRef<Path>,
    {
        let mut storage_index = StdHashMap::new();
        let mut total_cmds_counter = 0;
        let mut log_files_data = vec![];

        for (log_id, log_file_path) in KvStoreDb::list_log_ids_files_sorted(dir_path.as_ref()) {
            let mut log_file_cmd_counter: u64 = 0;
            let mut log_file = OpenOptions::new()
                .read(true)
                .open(log_file_path.as_path())?;
            let mut reader = BufReader::new(&mut log_file);

            loop {
                let offset = reader.seek(SeekFrom::Current(0))?;
                let res = bincode::deserialize_from::<_, Command>(&mut reader);
                match res {
                    Ok(Command::Set { key, value }) => {
                        total_cmds_counter += 1;
                        log_file_cmd_counter += 1;
                        let len = bincode::serialized_size(&Command::Set {
                            key: key.clone(),
                            value,
                        })?;
                        storage_index.insert(
                            key,
                            CommandIndex {
                                log_id,
                                offset,
                                len,
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
        self.insert_entry(key, value)?;

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
        match self.storage_index.get(&key).cloned() {
            Some(ci) => Ok(Some(
                self.read_value_from_log_at(ci.log_id, ci.offset, ci.len)?,
            )),
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
        log_dir.into().join(format!(
            "{}{:012}{}",
            LOG_FILE_PREFIX, log_id, LOG_FILE_SUFFIX
        ))
    }

    fn list_log_ids_files_sorted<P: Into<PathBuf>>(
        log_dir: P,
    ) -> impl Iterator<Item = (u64, PathBuf)> {
        let log_dir = log_dir.into();
        WalkDir::new(log_dir.as_path())
            .min_depth(1)
            .max_depth(1)
            .sort_by(|e1, e2| Ord::cmp(&e1.file_name(), &e2.file_name()))
            .into_iter()
            .filter_entry(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.starts_with(LOG_FILE_PREFIX) && s.ends_with(LOG_FILE_SUFFIX))
                    .unwrap_or(false)
            })
            .filter_map(move |r| {
                if let Ok(entry) = r {
                    entry.file_name().to_str().and_then(|s| {
                        if let Ok(id) = s[LOG_FILE_PREFIX.len()..(s.len() - LOG_FILE_SUFFIX.len())]
                            .parse::<u64>()
                        {
                            Some((id, log_dir.join(s)))
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
    }

    fn get_active_file_id_offsets_map(&self) -> StdHashMap<u64, StdHashSet<u64>> {
        self.storage_index
            .values()
            .sorted_by(|i1, i2| Ord::cmp(&i1.log_id, &i2.log_id))
            .group_by(|i1| i1.log_id)
            .into_iter()
            .map(|(p, i)| (p, i.map(|ci| ci.offset).collect::<StdHashSet<_>>()))
            .collect::<StdHashMap<_, _>>()
    }

    fn insert_entry(&mut self, key: String, value: String) -> Result<()> {
        let log_id = self.curr_log_w.id;
        let offset = self.curr_log_w.offset;
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let len = bincode::serialized_size(&cmd)?;
        self.storage_index.insert(
            key,
            CommandIndex {
                log_id,
                offset,
                len,
            },
        );
        self.write_cmd_to_curr_log(cmd)
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
