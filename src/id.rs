use crate::error::DbError;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{de, ser};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

const ID_SIZE: usize = 16;

/// Unique item ID
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Id([u8; ID_SIZE]);

impl Id {
    /// Generate a random ID
    pub fn random() -> Id {
        let chars = b"0123456789abcdef";
        let mut rng = thread_rng();
        let mut arr: [u8; ID_SIZE] = Default::default();
        for elem in &mut arr {
            *elem = *chars.choose(&mut rng).unwrap();
        }
        Id(arr)
    }

    /// Convert an ID to a 16-character hexadecimal string
    pub fn to_str(&self) -> Result<&str, DbError> {
        std::str::from_utf8(&self.0).map_err(|_| DbError::InvalidId)
    }
}

impl std::str::FromStr for Id {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let b = s.as_bytes();
        if b.len() == ID_SIZE {
            let mut arr: [u8; ID_SIZE] = Default::default();
            arr.copy_from_slice(b);
            Ok(Id(arr))
        } else {
            Err(DbError::InvalidId)
        }
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(s) = self.to_str() {
            f.write_str(s)
        } else {
            Err(fmt::Error)
        }
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
        if let Ok(id) = s.parse::<Id>() {
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
        let id = "0123456789abcdef".parse::<Id>().unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"0123456789abcdef\"");
    }

    #[test]
    fn test_id_deserialize() {
        let id: Id = serde_json::from_str("\"0123456789abcdef\"").unwrap();
        assert_eq!(id, "0123456789abcdef".parse::<Id>().unwrap());
    }
}
