//! Key-Value Store Communication Protocol (KVSCP)
pub use de::from_bytes;
pub use ser::{calc_len, to_bytes};

mod de;
mod error;
mod ser;

use serde::{Deserialize, Serialize};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use serde::ser::SerializeTuple;

const PROTOCOL_HEADER: u8 = 0xC1;

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct Message {
    header: u8,
    payload: MessagePayload,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum MessagePayload {
    Request(Request),
    Response(Response),
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

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Request {
    Set(RequestSet),
    Get(RequestGet),
    Remove(RequestRemove),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestSet {
    key: String,
    value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestGet {
    key: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct RequestRemove {
    key: String,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Response {
    Set(ResponseSet),
    Get(ResponseGet),
    Remove(ResponseRemove),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseSet {
    code: StatusCode,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseGet {
    code: StatusCode,
    value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResponseRemove {
    code: StatusCode,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, ToPrimitive, FromPrimitive)]
#[repr(u8)]
pub enum StatusCode {
    Ok,
    KeyNotFound,
    FatalError,
}

impl RequestSet {
    pub fn new_message(key: String, value: String) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Request(Request::Set(RequestSet { key, value })),
        }
    }
}

impl RequestGet {
    pub fn new_message(key: String) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Request(Request::Get(RequestGet { key })),
        }
    }
}

impl RequestRemove {
    pub fn new_message(key: String) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Request(Request::Remove(RequestRemove { key })),
        }
    }
}

impl ResponseSet {
    pub fn new_message(code: StatusCode) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Response(Response::Set(ResponseSet { code })),
        }
    }
}

impl ResponseGet {
    pub fn new_message(code: StatusCode, value: String) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Response(Response::Get(ResponseGet { code, value })),
        }
    }
}

impl ResponseRemove {
    pub fn new_message(code: StatusCode) -> Message {
        Message {
            header: PROTOCOL_HEADER,
            payload: MessagePayload::Response(Response::Remove(ResponseRemove { code })),
        }
    }
}
#[derive(FromPrimitive)]
#[repr(u8)]
enum MessageType {
    ReqSet = 0,
    ReqGet,
    ReqRemove,
    RespSet = (1u8 << 7),
    RespGet,
    RespRemove,
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
    let payload_len = calc_len(content).map_err(serde::ser::Error::custom)? as u32;
    let mut s = serializer.serialize_tuple(3)?;
    s.serialize_element(&(msg_type as u8))?;
    s.serialize_element(&payload_len)?;
    s.serialize_element(content)?;
    s.end()
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
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<RequestSet, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::ReqGet => {
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<RequestGet, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::ReqRemove => {
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<RequestRemove, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespSet => {
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<ResponseSet, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespGet => {
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<ResponseGet, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
                            Ok(MessagePayload::from(val?))
                        }
                        MessageType::RespRemove => {
                            let next_bytes = seq
                                .next_element::<Vec<u8>>()
                                .map_err(serde::de::Error::custom)
                                .and_then(|o| {
                                    o.ok_or(serde::de::Error::invalid_type(
                                        serde::de::Unexpected::Other("?"),
                                        &self,
                                    ))
                                })?;

                            let val: Result<ResponseRemove, _> = from_bytes(&next_bytes[..])
                                .map_err(|err| serde::de::Error::custom(err.to_string()));
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
        0xC1, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y', 0x00, 0x00,
        0x00, 0x05, b'v', b'a', b'l', b'u', b'e',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_request_get() {
    let cmd = RequestGet::new_message("key".to_owned());
    let expected_serialized = vec![
        0xC1, 0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_request_rm() {
    let cmd = RequestRemove::new_message("key".to_owned());
    let expected_serialized = vec![
        0xC1, 0x02, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x03, b'k', b'e', b'y',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_set() {
    let cmd = ResponseSet::new_message(StatusCode::Ok);
    let expected_serialized = vec![0xC1, 0x80, 0x00, 0x00, 0x00, 0x01, 0x00];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_get() {
    let cmd = ResponseGet::new_message(StatusCode::Ok, "value".to_string());
    let expected_serialized = vec![
        0xC1, 0x81, 0x00, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x05, b'v', b'a', b'l', b'u',
        b'e',
    ];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}

#[test]
fn test_serde_response_rm() {
    let cmd = ResponseRemove::new_message(StatusCode::Ok);
    let expected_serialized = vec![0xC1, 0x82, 0x00, 0x00, 0x00, 0x01, 0x00];

    let mut write_buf = Vec::new();
    let cmd_len = calc_len(&cmd);
    assert!(cmd_len.is_ok());
    write_buf.resize(cmd_len.unwrap(), 0);

    let write_res = to_bytes(&cmd, &mut write_buf[..]);
    assert!(write_res.is_ok());

    assert_eq!(write_buf, expected_serialized);
    let cmd_deserialized: Result<Message, _> = from_bytes(&write_buf[..]);
    assert!(cmd_deserialized.is_ok());
    assert_eq!(cmd_deserialized.unwrap(), cmd);
}
