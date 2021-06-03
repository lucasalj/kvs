use serde::de::{DeserializeSeed, EnumAccess, IntoDeserializer, VariantAccess, Visitor};

use super::error::Error;
use std::str::FromStr;

pub struct Deserializer<'de> {
    buf: &'de [u8],
    rd_idx: usize,
}

impl<'de> Deserializer<'de> {
    pub fn new(buf: &'de [u8]) -> Self {
        Deserializer { buf, rd_idx: 0 }
    }
}

impl<'de> Deserializer<'de> {
    fn read_byte(&mut self) -> Result<u8, Error> {
        let next_idx = self.rd_idx + 1;
        if next_idx <= self.buf.len() {
            let val = self.buf[self.rd_idx];
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_bytes(&mut self, len: usize) -> Result<&[u8], Error> {
        let next_idx = self.rd_idx + len;
        if next_idx <= self.buf.len() {
            let val = &self.buf[self.rd_idx..next_idx];
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_u16(&mut self) -> Result<u16, Error> {
        let next_idx = self.rd_idx + std::mem::size_of::<u16>();
        if next_idx <= self.buf.len() {
            let mut buf = [0u8; std::mem::size_of::<u16>()];
            buf.copy_from_slice(&self.buf[self.rd_idx..next_idx]);
            let val = u16::from_be_bytes(buf);
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_u32(&mut self) -> Result<u32, Error> {
        let next_idx = self.rd_idx + std::mem::size_of::<u32>();
        if next_idx <= self.buf.len() {
            let mut buf = [0u8; std::mem::size_of::<u32>()];
            buf.copy_from_slice(&self.buf[self.rd_idx..next_idx]);
            let val = u32::from_be_bytes(buf);
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        let next_idx = self.rd_idx + std::mem::size_of::<u64>();
        if next_idx <= self.buf.len() {
            let mut buf = [0u8; std::mem::size_of::<u64>()];
            buf.copy_from_slice(&self.buf[self.rd_idx..next_idx]);
            let val = u64::from_be_bytes(buf);
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_f32(&mut self) -> Result<f32, Error> {
        let next_idx = self.rd_idx + std::mem::size_of::<f32>();
        if next_idx <= self.buf.len() {
            let mut buf = [0u8; std::mem::size_of::<f32>()];
            buf.copy_from_slice(&self.buf[self.rd_idx..next_idx]);
            let val = f32::from_be_bytes(buf);
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }

    fn read_f64(&mut self) -> Result<f64, Error> {
        let next_idx = self.rd_idx + std::mem::size_of::<f64>();
        if next_idx <= self.buf.len() {
            let mut buf = [0u8; std::mem::size_of::<f64>()];
            buf.copy_from_slice(&self.buf[self.rd_idx..next_idx]);
            let val = f64::from_be_bytes(buf);
            self.rd_idx = next_idx;
            Ok(val)
        } else {
            Err(Error::Eof)
        }
    }
}

pub fn from_bytes<'de, T>(bytes: &'de [u8]) -> Result<T, Error>
where
    T: serde::Deserialize<'de>,
{
    let mut d = Deserializer::new(bytes);
    T::deserialize(&mut d)
}

impl<'de: 'a, 'a> serde::de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.read_byte()? == 0 {
            visitor.visit_bool(false)
        } else {
            visitor.visit_bool(true)
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.read_byte()? as i8)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.read_u16()? as i16)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.read_u32()? as i32)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.read_u64()? as i64)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.read_byte()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.read_u16()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.read_u32()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.read_u64()?)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.read_f32()?)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f64(self.read_f64()?)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.read_u32()? as usize;
        let buf = self.read_bytes(len)?;

        let string = std::str::from_utf8(buf).map_err(|e| Error::InvalidUtf8Encoding(e))?;

        visitor.visit_str(string)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.read_u32()? as usize;
        let buf = self.read_bytes(len)?;
        let s =
            String::from_str(std::str::from_utf8(buf).map_err(|e| Error::InvalidUtf8Encoding(e))?)
                .map_err(|e| Error::Message(e.to_string()))?;
        visitor.visit_string(s)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.read_u32()? as usize;
        let buf = self.read_bytes(len)?;
        visitor.visit_bytes(buf)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.read_u32()? as usize;
        let buf = Vec::from(self.read_bytes(len)?);

        visitor.visit_byte_buf(buf)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.read_byte()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(&mut *self),
            _ => Err(Error::Message("Invalid option tag".to_string())),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let len = self.read_u32()? as usize;
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        struct Access<'de: 'a, 'a> {
            deserializer: &'a mut Deserializer<'de>,
            len: usize,
        }

        impl<'de: 'a, 'a> serde::de::SeqAccess<'de> for Access<'de, 'a> {
            type Error = Error;

            fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
            where
                T: DeserializeSeed<'de>,
            {
                if self.len > 0 {
                    self.len -= 1;
                    let value =
                        serde::de::DeserializeSeed::deserialize(seed, &mut *self.deserializer)?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }

            fn size_hint(&self) -> Option<usize> {
                Some(self.len)
            }
        }

        visitor.visit_seq(Access {
            deserializer: self,
            len,
        })
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }
}

impl<'de: 'a, 'a> EnumAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let idx = self.read_u32()? as usize;
        let val: Result<_, Error> = seed.deserialize(idx.into_deserializer());
        Ok((val?, self))
    }
}

impl<'de: 'a, 'a> VariantAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        DeserializeSeed::deserialize(seed, self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        serde::de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}
