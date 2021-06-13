//! Key-Value Store Communication Protocol (KVSCP)
//! The messages communicated in this protocol follows the format.
//! Byte index(es) from the MSB to LSB: Meaning
//! 0: ProtocolHeader
//! 1: MessageType (Bit7 => 0: Request, 1: Response; Bits[0..6] => Command)
//! 2-: The actual payload content

//! Note: All the length fields in the protocol are read as unsigned 32 bit integers.
//! All the numeric values are (de)serialized in big endian format.

pub mod de;
pub mod error;
pub mod ser;

use num_traits::{FromPrimitive, ToPrimitive};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize};

/// Every message in the protocol must start with the following byte
pub const PROTOCOL_VERSION: u8 = 0xC1;

/// The fixed size of the header for every message of the protocol
pub const HEADER_SIZE: usize = 5;

/// Message format for commands used in communication between kvs server and client
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Message {
    payload: MessagePayload,
}

/// The header of the message. It includes the version and total payloand length
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Header {
    protocol_version: u8,
    payload_length: u32,
}

impl Header {
    /// Constructs a new instance of Header
    pub fn new() -> Self {
        Header {
            protocol_version: PROTOCOL_VERSION,
            payload_length: 0,
        }
    }

    /// Get header's protocol version.
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Get header's payload length.
    pub fn payload_length(&self) -> u32 {
        self.payload_length
    }
}

/// A enum to distinguish between messages sent from the client to the server (requests)
/// and messages sent from the serer to the client (responses)
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum MessagePayload {
    /// Messages in the direction client to server have a `Request` payload
    Request(Request),

    /// Messages in the direction server to client have a `Response` payload
    Response(Response),
}

/// The payload of a `Request` message
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Request {
    /// Request of the type `Set` Command
    Set(RequestSet),

    /// Request of the type `Get` Command
    Get(RequestGet),

    /// Request of the type `Remove` Command
    Remove(RequestRemove),
}

/// A Request for a `Set` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestSet {
    key: String,
    value: String,
}

/// A Request for a `Get` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestGet {
    key: String,
}

/// A Request for a `Remove` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestRemove {
    key: String,
}

/// The payload of a `Response` message
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Response {
    /// Response of the type `Set` Command
    Set(ResponseSet),

    /// Response of the type `Get` Command
    Get(ResponseGet),

    /// Response of the type `Remove` Command
    Remove(ResponseRemove),
}

/// A Response for a `Set` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseSet {
    code: StatusCode,
}

/// A Response for a `Get` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseGet {
    code: StatusCode,
    value: Option<String>,
}

/// A Response for a `Remove` Command
#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseRemove {
    code: StatusCode,
}

/// A Status code to be used in response messages to indicate if the command executed sucessfully or failed with which kind of error
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Primitive)]
#[repr(u8)]
pub enum StatusCode {
    /// Success status
    Ok = 0,

    /// The operation failed because no such key was found in the database
    KeyNotFound = 1,

    /// The operation failed with a fatal error on the server
    FatalError = 2,
}

impl Message {
    /// payload field getter
    pub fn payload(&self) -> &MessagePayload {
        &self.payload
    }
}

impl std::convert::From<RequestSet> for MessagePayload {
    fn from(req: RequestSet) -> Self {
        MessagePayload::Request(Request::Set(req))
    }
}

impl std::convert::From<RequestGet> for MessagePayload {
    fn from(req: RequestGet) -> Self {
        MessagePayload::Request(Request::Get(req))
    }
}

impl std::convert::From<RequestRemove> for MessagePayload {
    fn from(req: RequestRemove) -> Self {
        MessagePayload::Request(Request::Remove(req))
    }
}

impl std::convert::From<ResponseSet> for MessagePayload {
    fn from(req: ResponseSet) -> Self {
        MessagePayload::Response(Response::Set(req))
    }
}

impl std::convert::From<ResponseGet> for MessagePayload {
    fn from(req: ResponseGet) -> Self {
        MessagePayload::Response(Response::Get(req))
    }
}

impl std::convert::From<ResponseRemove> for MessagePayload {
    fn from(req: ResponseRemove) -> Self {
        MessagePayload::Response(Response::Remove(req))
    }
}

impl RequestSet {
    /// Instantiate a new request message for the `Set` command
    pub fn new_message(key: String, value: String) -> Message {
        Message {
            payload: MessagePayload::Request(Request::Set(RequestSet { key, value })),
        }
    }

    /// Get a reference to the request set's key.
    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Get a reference to the request set's value.
    pub fn value(&self) -> &str {
        self.value.as_str()
    }
}

impl RequestGet {
    /// Instantiate a new request message for the `Get` command
    pub fn new_message(key: String) -> Message {
        Message {
            payload: MessagePayload::Request(Request::Get(RequestGet { key })),
        }
    }

    /// Get a reference to the request get's key.
    pub fn key(&self) -> &str {
        self.key.as_str()
    }
}

impl RequestRemove {
    /// Instantiate a new request message for the `Remove` command
    pub fn new_message(key: String) -> Message {
        Message {
            payload: MessagePayload::Request(Request::Remove(RequestRemove { key })),
        }
    }

    /// Get a reference to the request remove's key.
    pub fn key(&self) -> &str {
        self.key.as_str()
    }
}

impl ResponseSet {
    /// Instantiate a new reponse message for the `Set` command
    pub fn new_message(code: StatusCode) -> Message {
        Message {
            payload: MessagePayload::Response(Response::Set(ResponseSet { code })),
        }
    }

    /// Get a reference to the response set's code.
    pub fn code(&self) -> &StatusCode {
        &self.code
    }
}

impl ResponseGet {
    /// Instantiate a new reponse message for the `Get` command
    pub fn new_message(code: StatusCode, value: Option<String>) -> Message {
        Message {
            payload: MessagePayload::Response(Response::Get(ResponseGet { code, value })),
        }
    }

    /// Get a reference to the response get's code.
    pub fn code(&self) -> &StatusCode {
        &self.code
    }

    /// Get a reference to the response get's value.
    pub fn value(&self) -> Option<&String> {
        self.value.as_ref()
    }
}

impl ResponseRemove {
    /// Instantiate a new reponse message for the `Remove` command
    pub fn new_message(code: StatusCode) -> Message {
        Message {
            payload: MessagePayload::Response(Response::Remove(ResponseRemove { code })),
        }
    }

    /// Get a reference to the response remove's code.
    pub fn code(&self) -> &StatusCode {
        &self.code
    }
}

impl<T> std::convert::From<&std::result::Result<T, super::KvStoreError>> for StatusCode
where
    T: std::fmt::Debug,
{
    fn from(res: &std::result::Result<T, super::KvStoreError>) -> Self {
        if res.is_ok() {
            StatusCode::Ok
        } else {
            match res.as_ref().unwrap_err() {
                super::KvStoreError::RemoveNonExistentKey => StatusCode::KeyNotFound,
                _ => StatusCode::FatalError,
            }
        }
    }
}

#[derive(Primitive)]
#[repr(u8)]
enum MessageType {
    ReqSet = 0,
    ReqGet = 1,
    ReqRemove = 2,
    RespSet = 0x80,
    RespGet = 0x81,
    RespRemove = 0x82,
}

fn serialize_content<T, S>(
    content: &T,
    msg_type: MessageType,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T: serde::Serialize,
    S: serde::Serializer,
{
    let mut s = serializer.serialize_tuple(2)?;
    s.serialize_element(&(msg_type as u8))?;
    s.serialize_element(content)?;
    s.end()
}

impl serde::ser::Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let payload_length =
            ser::calc_len(&self.payload).map_err(|e| serde::ser::Error::custom(e.to_string()))?;
        let mut s = serializer.serialize_tuple(3)?;
        s.serialize_element(&PROTOCOL_VERSION)?;
        s.serialize_element(&(payload_length as u32))?;
        s.serialize_element(&self.payload)?;
        s.end()
    }
}

impl<'de> serde::de::Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> serde::de::Visitor<'de> for MessageVisitor {
            type Value = Message;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a struct Message")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                seq.next_element::<Header>()?
                    .ok_or(serde::de::Error::missing_field(
                        "an protocol version number (u8)",
                    ))?;
                let payload = seq.next_element::<MessagePayload>()?.ok_or(
                    serde::de::Error::missing_field("a payload length number (u32)"),
                )?;
                Ok(Message { payload })
            }
        }

        deserializer.deserialize_tuple(2, MessageVisitor {})
    }
}

impl serde::ser::Serialize for MessagePayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MessagePayload::Request(Request::Set(c)) => {
                serialize_content(c, MessageType::ReqSet, serializer)
            }
            MessagePayload::Request(Request::Get(c)) => {
                serialize_content(c, MessageType::ReqGet, serializer)
            }
            MessagePayload::Request(Request::Remove(c)) => {
                serialize_content(c, MessageType::ReqRemove, serializer)
            }
            MessagePayload::Response(Response::Set(c)) => {
                serialize_content(c, MessageType::RespSet, serializer)
            }
            MessagePayload::Response(Response::Get(c)) => {
                serialize_content(c, MessageType::RespGet, serializer)
            }
            MessagePayload::Response(Response::Remove(c)) => {
                serialize_content(c, MessageType::RespRemove, serializer)
            }
        }
    }
}

fn deserialize_payload<'de, V, A, T>(visitor: &V, mut seq: A) -> Result<T, A::Error>
where
    T: serde::ser::Serialize + serde::de::Deserialize<'de>,
    V: serde::de::Visitor<'de>,
    A: serde::de::SeqAccess<'de>,
{
    let val = seq
        .next_element::<T>()
        .map_err(serde::de::Error::custom)
        .and_then(|o| {
            o.ok_or(serde::de::Error::invalid_type(
                serde::de::Unexpected::Other("?"),
                &*visitor,
            ))
        })?;
    Ok(val)
}

impl<'de> serde::de::Deserialize<'de> for MessagePayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MessagePayloadVisitor;

        impl<'de> serde::de::Visitor<'de> for MessagePayloadVisitor {
            type Value = MessagePayload;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a struct MessagePayload")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let discriminant = seq.next_element::<u8>()?;
                if let Some(i) = discriminant {
                    let msg_type: Option<MessageType> = FromPrimitive::from_u8(i);
                    return match msg_type.ok_or(serde::de::Error::invalid_type(
                        serde::de::Unexpected::Other("?"),
                        &"an message type discriminant",
                    ))? {
                        MessageType::ReqSet => {
                            let val: Result<RequestSet, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::ReqGet => {
                            let val: Result<RequestGet, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::ReqRemove => {
                            let val: Result<RequestRemove, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespSet => {
                            let val: Result<ResponseSet, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespGet => {
                            let val: Result<ResponseGet, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespRemove => {
                            let val: Result<ResponseRemove, _> = deserialize_payload(&self, seq);
                            Ok(MessagePayload::from(val?))
                        }
                    };
                }
                return Err(serde::de::Error::missing_field(
                    "an message type discriminant",
                ));
            }
        }
        deserializer.deserialize_tuple(2, MessagePayloadVisitor {})
    }
}

impl serde::ser::Serialize for StatusCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u8(
            ToPrimitive::to_u8(self)
                .ok_or(serde::ser::Error::custom("Invalid StatusCode value"))?,
        )
    }
}

impl<'de> serde::de::Deserialize<'de> for StatusCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StatusCodeVisitor;

        impl<'de> serde::de::Visitor<'de> for StatusCodeVisitor {
            type Value = StatusCode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a u8")
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(
                    FromPrimitive::from_u8(v).ok_or(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(v as u64),
                        &self,
                    ))?,
                )
            }
        }

        deserializer.deserialize_u8(StatusCodeVisitor {})
    }
}

#[test]
fn test_serde_request_set() {
    let cmd = RequestSet::new_message("key".to_owned(), "value".to_owned());
    let expected_serialized = vec![
        0xC1, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y', 0x00, 0x00,
        0x00, 0x05, b'v', b'a', b'l', b'u', b'e',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_request_get() {
    let cmd = RequestGet::new_message("key".to_owned());
    let expected_serialized = vec![
        0xC1, 0x00, 0x00, 0x00, 0x08, 0x01, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_request_rm() {
    let cmd = RequestRemove::new_message("key".to_owned());
    let expected_serialized = vec![
        0xC1, 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_set() {
    let cmd = ResponseSet::new_message(StatusCode::Ok);
    let expected_serialized = vec![0xC1, 0x00, 0x00, 0x00, 0x02, 0x80, 0x00];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_get() {
    let cmd = ResponseGet::new_message(StatusCode::Ok, Some("value".to_string()));
    let expected_serialized = vec![
        0xC1, 0x00, 0x00, 0x00, 0x0C, 0x81, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05, b'v', b'a', b'l',
        b'u', b'e',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_rm() {
    let cmd = ResponseRemove::new_message(StatusCode::Ok);
    let expected_serialized = vec![0xC1, 0x00, 0x00, 0x00, 0x02, 0x82, 0x00];

    let mut write_buf = Vec::new();
    let cmd_len = ser::calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = ser::to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = de::from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}
