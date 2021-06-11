//! Deals with errors that happen while serializing to and deserializing from KVSCP data format

use std::string::FromUtf8Error;

/// An error type returned by protocol (De)Serialization operations
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// Custom error
    Message(String),

    /// Error from an IO operation
    IoError(String),

    /// Error in conversion between raw bytes and UTF-8
    InvalidUtf8Encoding(std::str::Utf8Error),

    /// Error when there is not enough space in writer buffer to serialize the object
    NotEnoughSpaceInBuffer,

    /// Unexpected end-of-file encountered while deserializing
    Eof,
}

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Message(msg) => formatter.write_str(msg),
            Error::IoError(msg) => formatter.write_fmt(format_args!("Io Error: {}", msg)),
            Error::Eof => formatter.write_str("unexpected end of input"),
            Error::InvalidUtf8Encoding(e) => {
                formatter.write_fmt(format_args!("Utf8Error Error: {}", e))
            }
            Error::NotEnoughSpaceInBuffer => {
                formatter.write_str("there is not enough space in buffer")
            }
        }
    }
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err.to_string())
    }
}

impl std::convert::From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::InvalidUtf8Encoding(err.utf8_error())
    }
}

impl std::error::Error for Error {}
