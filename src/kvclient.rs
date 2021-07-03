use crate::cp::*;
use smallvec::{smallvec, SmallVec};
use std::{
    convert,
    fmt::{self},
    io::{self, prelude::*},
    net::{SocketAddr, TcpStream},
};

/// KVS store system tcp client
pub struct KvClient {
    server_address: SocketAddr,
}

/// Error return by the kvs client api
#[derive(Debug)]
pub enum KvClientError<'a> {
    /// server address is invalid
    InvalidServerAddress {
        /// Actual address
        addr: &'a str,
        /// Internal cause
        cause: String,
    },

    /// io operation failed
    IoError(io::Error),

    /// Communication protocol error
    CommunicationProtocolError(crate::cp::error::Error),

    /// The key was not present at the database
    KeyNotFound,

    /// An internal server error ocurred
    ServerError,

    /// A specific kind of error happend for the communication protocol:
    ///   The client received a request message back from the server
    CommunicationProtocolMessageWrongKind,
}

impl<'a> fmt::Display for KvClientError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvClientError::InvalidServerAddress { addr, cause } => f.write_fmt(format_args!(
                "Invalid socket address: {}. Error: {}",
                addr, cause
            )),
            KvClientError::IoError(err) => f.write_fmt(format_args!("Io error: {}", err)),
            KvClientError::CommunicationProtocolError(err) => {
                f.write_fmt(format_args!("KVS Communication protocol error: {}", err))
            }
            KvClientError::KeyNotFound => f.write_str("Key not found"),
            KvClientError::ServerError => f.write_str("Internal server error"),
            KvClientError::CommunicationProtocolMessageWrongKind => {
                f.write_str("KVS Communication protocol error: client received a request message")
            }
        }
    }
}

impl<'a> std::error::Error for KvClientError<'a> {}

impl<'a> convert::From<io::Error> for KvClientError<'a> {
    fn from(err: io::Error) -> Self {
        KvClientError::IoError(err)
    }
}

impl<'a> convert::From<error::Error> for KvClientError<'a> {
    fn from(err: error::Error) -> Self {
        KvClientError::CommunicationProtocolError(err)
    }
}

impl KvClient {
    /// Creates a new instance of a KVClient given the server address
    pub fn new<'a>(server_addr: &'a str) -> Result<Self, KvClientError<'a>> {
        Ok(Self {
            server_address: match server_addr.parse::<SocketAddr>() {
                Ok(addr) => addr,
                Err(err) => {
                    return Err(KvClientError::InvalidServerAddress {
                        addr: server_addr,
                        cause: err.to_string(),
                    })
                }
            },
        })
    }

    /// Sends a command set, given the `key` and `value`, to the server over a tcp connection and get the ok
    /// result back if the operation completed sucessfully or the error if it failed
    pub fn send_cmd_set(&self, key: String, value: String) -> Result<(), KvClientError<'static>> {
        let mut stream = std::net::TcpStream::connect(&self.server_address)?;
        let msg = RequestSet::new_message(key, value);
        KvClient::send_request(&msg, &mut stream)?;
        match KvClient::recv_payload(&mut stream)? {
            MessagePayload::Response(Response::Set(r)) => match r.code() {
                StatusCode::KeyNotFound => Err(KvClientError::KeyNotFound),
                StatusCode::FatalError => Err(KvClientError::ServerError),
                StatusCode::Ok => Ok(()),
            },
            _ => Err(KvClientError::CommunicationProtocolMessageWrongKind),
        }
    }

    /// Sends a command get, given the `key`, to the server over a tcp connection and get the ok result back
    /// if the operation completed sucessfully with the `key`'s `value` or the error if it failed
    pub fn send_cmd_get(&self, key: String) -> Result<Option<String>, KvClientError<'static>> {
        let mut stream = std::net::TcpStream::connect(&self.server_address)?;
        let msg = RequestGet::new_message(key);
        KvClient::send_request(&msg, &mut stream)?;
        match KvClient::recv_payload(&mut stream)? {
            MessagePayload::Response(Response::Get(r)) => match r.code() {
                StatusCode::KeyNotFound => Ok(None),
                StatusCode::FatalError => Err(KvClientError::ServerError),
                StatusCode::Ok => match r.value() {
                    Some(s) => Ok(Some(s.clone())),
                    None => Ok(None),
                },
            },
            _ => Err(KvClientError::CommunicationProtocolMessageWrongKind),
        }
    }

    /// Sends a command rm, given the `key`, to the server over a tcp connection and get the ok
    /// result back if the operation completed sucessfully or the error if it failed
    pub fn send_cmd_rm(&self, key: String) -> Result<(), KvClientError<'static>> {
        let mut stream = std::net::TcpStream::connect(&self.server_address)?;
        let msg = RequestRemove::new_message(key);
        KvClient::send_request(&msg, &mut stream)?;
        match KvClient::recv_payload(&mut stream)? {
            MessagePayload::Response(Response::Remove(r)) => match r.code() {
                StatusCode::KeyNotFound => Err(KvClientError::KeyNotFound),
                StatusCode::FatalError => Err(KvClientError::ServerError),
                StatusCode::Ok => Ok(()),
            },
            _ => Err(KvClientError::CommunicationProtocolMessageWrongKind),
        }
    }

    fn send_request(msg: &Message, stream: &mut TcpStream) -> Result<(), error::Error> {
        let mut buf = SmallVec::<[u8; 256]>::new();
        buf.resize(ser::calc_len(msg)?, 0u8);
        ser::to_bytes(msg, &mut buf[..])?;
        stream.write_all(&buf[..])?;
        Ok(())
    }

    fn recv_payload(stream: &mut TcpStream) -> Result<MessagePayload, error::Error> {
        let mut header_buf = [0u8; HEADER_SIZE];
        stream.read_exact(&mut header_buf)?;
        let header: Result<Header, _> = de::from_bytes(&header_buf);
        let header = header?;

        let mut payload_buf: SmallVec<[u8; 256]> = smallvec![0; header.payload_length() as usize];
        stream.read_exact(&mut payload_buf)?;
        de::from_bytes(&payload_buf)
    }
}
