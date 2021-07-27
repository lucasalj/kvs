use super::*;
use crossbeam_epoch::{Atomic, Owned};
use flurry::{epoch::Guard, HashMap as FlurryHashMap};
use itertools::Itertools;
use kvsengine::KvsEngine;
use parking_lot::{Mutex, MutexGuard};
use positioned_io::ReadAt;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use std::{
    collections::{HashMap as StdHashMap, HashSet as StdHashSet},
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, SeekFrom, Write},
    iter::FromIterator,
    path::{Path, PathBuf},
    result,
    sync::{atomic::AtomicI64, atomic::Ordering, Arc},
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
    storage_index: Arc<FlurryHashMap<String, CommandIndex>>,
    log_dir_path: PathBuf,
    writer_ctrl: Arc<Mutex<WriterControlData>>,
    curr_log_r: Atomic<LogFileReader>,
    last_collected_file_index: Arc<AtomicI64>,
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

/// Data structure managed by writers
#[derive(Debug)]
struct WriterControlData {
    curr_log: LogFileWriter,
    total_cmd_counter: u64,
}

/// Information about the current log file writer
#[derive(Debug)]
struct LogFileWriter {
    id: u64,
    writer: File,
    cmd_counter: u64,
    offset: u64,
}

/// Information about the current log file reader
#[derive(Debug)]
struct LogFileReader {
    id: u64,
    reader: File,
}

impl LogFileWriter {
    fn open<P>(id: u64, log_path: P, cmd_counter: u64) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path.as_ref())?;
        let offset = writer.seek(SeekFrom::End(0))?;
        Ok(Self {
            id,
            writer,
            cmd_counter,
            offset,
        })
    }

    fn open_another<P>(&mut self, id: u64, log_path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        self.id = id;
        self.writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path.as_ref())?;
        self.cmd_counter = 0;
        self.offset = 0;
        Ok(())
    }

    /// Get the log file writer's id.
    fn id(&self) -> u64 {
        self.id
    }

    /// Get the log file writer's cmd counter.
    fn cmd_counter(&self) -> u64 {
        self.cmd_counter
    }

    /// Get the log file writer's offset.
    fn offset(&self) -> u64 {
        self.offset
    }

    fn append_cmd(&mut self, cmd: Command) -> Result<u64> {
        let cmd_serialized = bincode::serialize(&cmd)?;
        let cmd_len = cmd_serialized.len() as u64;
        self.writer.write_all(&cmd_serialized)?;
        self.writer.flush()?;
        self.offset += cmd_len;
        self.cmd_counter += 1;
        Ok(cmd_len)
    }
}

impl LogFileReader {
    fn open<P>(id: u64, log_path: P) -> Result<Self>
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

impl WriterControlData {
    fn new(curr_log: LogFileWriter, total_cmd_counter: u64) -> Self {
        Self {
            curr_log,
            total_cmd_counter,
        }
    }

    /// Get a reference to the writer control data's total cmd counter.
    fn total_cmd_counter(&self) -> u64 {
        self.total_cmd_counter
    }

    /// Get a mutable reference to the writer control data's curr log.
    fn curr_log_mut(&mut self) -> &mut LogFileWriter {
        &mut self.curr_log
    }

    fn inc_total_cmd_counter(&mut self) {
        self.total_cmd_counter += 1;
    }

    fn sub_total_cmd_counter(&mut self, val: u64) {
        self.total_cmd_counter -= val;
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
        let log_dir_path = (path.into() as PathBuf).canonicalize()?.join("");

        let (storage_index, total_cmd_counter, log_id, cmd_counter) =
            KvStore::build_index(log_dir_path.as_path())?;

        let file_path = KvStore::format_log_path(log_dir_path.as_path(), log_id);
        let curr_log_w = LogFileWriter::open(log_id, file_path.as_path(), cmd_counter)?;
        let writer_ctrl = Arc::new(Mutex::new(WriterControlData::new(
            curr_log_w,
            total_cmd_counter,
        )));
        let curr_log_r = Atomic::new(LogFileReader::open(log_id, file_path.as_path())?);
        let storage_index = Arc::new(FlurryHashMap::from_iter(storage_index.into_iter()));
        let last_collected_file_index = Arc::new(AtomicI64::new(log_id as i64 - 1));

        Ok(KvStore {
            storage_index,
            log_dir_path,
            writer_ctrl,
            curr_log_r,
            last_collected_file_index,
        })
    }

    /// Check if it should run compaction algorithm
    fn should_run_compaction<'g>(
        &self,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> bool {
        writer_ctrl.total_cmd_counter() > CMDS_THRESHOLD
            && ((writer_ctrl.total_cmd_counter() / (self.storage_index.len() as u64)) as f64)
                > CMD_KEY_FACTOR
    }

    /// Check if it should create a new log file
    fn should_create_new_file<'g>(
        &self,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> bool {
        let curr_log = writer_ctrl.curr_log_mut();
        curr_log.offset() > CURR_FILE_OFFSET_THRESHOLD
            || curr_log.cmd_counter() > (CMDS_THRESHOLD / 2)
    }

    /// Create a new log following the format for log file name and save the current log file information
    /// in memory.
    fn do_create_new_file<'g>(
        &self,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> Result<()> {
        let curr_log = writer_ctrl.curr_log_mut();
        let new_file_id = curr_log.id() + 1;
        let new_file_path = KvStore::format_log_path(self.log_dir_path.as_path(), new_file_id);
        curr_log.open_another(new_file_id, new_file_path)?;
        Ok(())
    }

    /// Runs the following compaction strategy:
    ///     1. Collect all command offsets related to each active entry from the storage index map and rearrange them in a map where
    ///        the keys are the log file ids and the value is a set of offsets of each active entry in that corresponding file.
    ///     2. List all the log file names and ids in the database directory sorted by name.
    ///     3. For each file read all of its commands, if the command is a set and refers to an active entry, write it down to
    ///        the current log and after processing all of the file commands, delete the file.
    fn do_compaction<'g>(
        &self,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> Result<()> {
        let index_guard = &self.storage_index.guard();
        let curr_log = writer_ctrl.curr_log_mut();
        let active_file_id_offsets_map = self.get_active_file_id_offsets_map(index_guard);
        let curr_log_id = curr_log.id();

        let last_collected_file_index = self.last_collected_file_index.load(Ordering::SeqCst);

        let log_ids_files = KvStore::list_log_ids_files_sorted(self.log_dir_path.as_path())
            .filter(|(id, _)| *id < curr_log_id && (*id as i64) > last_collected_file_index)
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
                                self.insert_entry(key, value, writer_ctrl)?;
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
                writer_ctrl.sub_total_cmd_counter(log_file_cmd_counter);
            }
            self.last_collected_file_index
                .store(log_id as i64, Ordering::SeqCst);
            index_guard.defer(move || fs::remove_file(log_path.as_path()).unwrap());
        }

        Ok(())
    }

    /// Serializes the command using bincode crate and write it down to the current log
    fn write_cmd_to_curr_log<'g>(
        &self,
        cmd: Command,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> Result<u64> {
        let curr_log = writer_ctrl.curr_log_mut();
        let nbytes = curr_log.append_cmd(cmd)?;
        writer_ctrl.inc_total_cmd_counter();
        Ok(nbytes)
    }

    /// Given a file name and an offset, access that position in the log file and returns the value if found.
    fn read_value_from_log_at(
        &self,
        log_id: u64,
        offset: u64,
        len: u64,
        guard: &'_ Guard,
    ) -> Result<String> {
        let curr_log_r = unsafe { self.curr_log_r.load(Ordering::SeqCst, guard).deref() };
        if log_id != curr_log_r.id() {
            let file_path = KvStore::format_log_path(self.log_dir_path.as_path(), log_id);
            self.curr_log_r.store(
                Owned::new(LogFileReader::open(log_id, file_path)?),
                Ordering::SeqCst,
            );
        }
        let cmd = curr_log_r.read_cmd_at(offset, len)?;
        if let Command::Set { key: _, value } = cmd {
            Ok(value)
        } else {
            Err(KvStoreError::WrongFileOffset)
        }
    }

    /// Given a directory path, finds and reads all the log files and returns the storage index,
    /// the total number of commands written in the log files and a vec with information to be used
    /// later by the compaction algorithm about each log file.
    fn build_index<P>(dir_path: P) -> Result<(StdHashMap<String, CommandIndex>, u64, u64, u64)>
    where
        P: AsRef<Path>,
    {
        let mut storage_index = StdHashMap::new();
        let mut total_cmds_counter = 0;
        let mut curr_log_id = 0;
        let mut cmd_counter = 0;

        for (log_id, log_file_path) in KvStore::list_log_ids_files_sorted(dir_path.as_ref()) {
            curr_log_id = log_id;
            cmd_counter = 0;
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
                        cmd_counter += 1;
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
                        cmd_counter += 1;
                        storage_index.remove(&*key);
                    }
                    Err(err) => match *err {
                        bincode::ErrorKind::Io(ref bincode_io_err) => match bincode_io_err.kind() {
                            std::io::ErrorKind::UnexpectedEof => break,
                            _ => return Err(KvStoreError::from(err)),
                        },
                        _ => return Err(KvStoreError::from(err)),
                    },
                }
            }
        }
        Ok((storage_index, total_cmds_counter, curr_log_id, cmd_counter))
    }

    fn _set(&self, key: String, value: String) -> Result<()> {
        let writer_ctrl = &mut self.writer_ctrl.lock();
        self.insert_entry(key, value, writer_ctrl)?;

        Ok(())
    }

    fn _get(&self, key: String) -> Result<Option<String>> {
        let index_guard = &self.storage_index.guard();
        match self.storage_index.get(&key, index_guard).cloned() {
            Some(ci) => Ok(Some(self.read_value_from_log_at(
                ci.log_id,
                ci.offset,
                ci.len,
                index_guard,
            )?)),
            None => Ok(None),
        }
    }

    fn _remove(&self, key: String) -> Result<()> {
        let curr_low_w = &mut self.writer_ctrl.lock();
        let index_guard = &self.storage_index.guard();
        if let None = self.storage_index.remove(&key, index_guard) {
            Err(KvStoreError::RemoveNonExistentKey)
        } else {
            self.write_cmd_to_curr_log(Command::Remove { key }, curr_low_w)?;
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

    fn get_active_file_id_offsets_map(
        &self,
        index_guard: &'_ Guard,
    ) -> StdHashMap<u64, StdHashSet<u64>> {
        self.storage_index
            .values(index_guard)
            .sorted_by(|i1, i2| Ord::cmp(&i1.log_id, &i2.log_id))
            .group_by(|i1| i1.log_id)
            .into_iter()
            .map(|(p, i)| (p, i.map(|ci| ci.offset).collect::<StdHashSet<_>>()))
            .collect::<StdHashMap<_, _>>()
    }

    fn insert_entry<'g>(
        &self,
        key: String,
        value: String,
        writer_ctrl: &'_ mut MutexGuard<'g, WriterControlData>,
    ) -> Result<()> {
        let curr_log = writer_ctrl.curr_log_mut();
        let log_id = curr_log.id();
        let offset = curr_log.offset();
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let len = self.write_cmd_to_curr_log(cmd, writer_ctrl)?;
        let index_guard = &self.storage_index.guard();
        self.storage_index.insert(
            key,
            CommandIndex {
                log_id,
                offset,
                len,
            },
            index_guard,
        );
        Ok(())
    }

    fn _compact(&self) -> Result<()> {
        let writer_ctrl = &mut self.writer_ctrl.lock();
        if self.should_create_new_file(writer_ctrl) {
            self.do_create_new_file(writer_ctrl)?;
        }

        if self.should_run_compaction(writer_ctrl) {
            self.do_compaction(writer_ctrl)?;
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self._set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self._get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self._remove(key)
    }
}

impl KvsCompactor for KvStore {
    fn compact(&self) -> Result<()> {
        self._compact()
    }
}
