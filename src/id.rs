use crate::error::DbError;
use serde::{de, ser};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

pub const ID_SIZE: usize = 16;

/// Unique item ID
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Id(pub [u8; ID_SIZE]);

impl Id {
    pub fn from_str(s: &str) -> Result<Id, DbError> {
        let b = s.as_bytes();
        if b.len() == ID_SIZE {
            let mut arr: [u8; ID_SIZE] = Default::default();
            arr.copy_from_slice(b);
            Ok(Id(arr))
        } else {
            Err(DbError::InvalidId)
        }
    }

    pub fn to_str(&self) -> Result<&str, DbError> {
        std::str::from_utf8(&self.0).map_err(|_| DbError::InvalidId)
    }
}

impl Serialize for Id {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Ok(s) = self.to_str() {
            serializer.serialize_str(s)
        } else {
            Err(ser::Error::custom("invalid id"))
        }
    }
}

struct IdVisitor;

impl<'de> de::Visitor<'de> for IdVisitor {
    type Value = Id;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 16-character hexadecimal string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(id) = Id::from_str(s) {
            Ok(id)
        } else {
            Err(de::Error::invalid_value(de::Unexpected::Str(s), &self))
        }
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Id, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(IdVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_serialize() {
        let id = Id::from_str("0123456789abcdef").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"0123456789abcdef\"");
    }

    #[test]
    fn test_id_deserialize() {
        let id: Id = serde_json::from_str("\"0123456789abcdef\"").unwrap();
        assert_eq!(id, Id::from_str("0123456789abcdef").unwrap());
    }
}
