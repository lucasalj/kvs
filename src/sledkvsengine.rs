use super::{KvStoreError, KvsEngine, Result};
use itertools::Itertools;
use sled::{Config, Db};

/// Encaspulates the sled database engine
#[derive(Debug, Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    /// Open the SledKvsEngine at a given `path`.
    /// Return the SledKvsEngine.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kvs::SledKvsEngine;
    /// let dictionary = SledKvsEngine::open("./").unwrap();
    /// ```
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<SledKvsEngine> {
        let db = Config::new()
            .path(path)
            .open()
            .map_err(KvStoreError::from)?;
        Ok(SledKvsEngine { db })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db
            .insert(key.as_bytes(), value.as_bytes())
            .map(|_| ())
            .map_err(KvStoreError::from)?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.db.get(key).map_err(KvStoreError::from).map(|v| {
            v.and_then(|iv| String::from_utf8(iv.iter().map(Clone::clone).collect_vec()).ok())
        })
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db
            .remove(key)
            .map_err(KvStoreError::from)
            .and_then(|v| match v {
                Some(_) => Ok(()),
                None => Err(KvStoreError::RemoveNonExistentKey),
            })?;
        self.db.flush()?;
        Ok(())
    }
}
