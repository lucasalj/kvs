use failure::Fail;

/// An error type for describing any kind of error returned by the KvStore API
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
    /// An error that came from the walkdir crate
    #[fail(display = "Walkdir error: {}.", _0)]
    Walkdir(#[cause] walkdir::Error),
    /// An error that came from the sled crate
    #[fail(display = "Sled error: {}.", _0)]
    Sled(#[cause] sled::Error),
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

impl From<KvStoreError> for std::boxed::Box<dyn std::error::Error> {
    fn from(err: KvStoreError) -> Self {
        err.compat().into()
    }
}

impl From<sled::Error> for KvStoreError {
    fn from(error: sled::Error) -> Self {
        Self::Sled(error)
    }
}
