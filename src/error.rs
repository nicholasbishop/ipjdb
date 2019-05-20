use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum DbError {
    InvalidId,
    IoError(io::Error),
    JsonError(serde_json::error::Error),
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> Self {
        DbError::IoError(error)
    }
}

impl From<serde_json::error::Error> for DbError {
    fn from(error: serde_json::error::Error) -> Self {
        DbError::JsonError(error)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            DbError::InvalidId => write!(f, "InvalidId"),
            DbError::IoError(e) => write!(f, "IoError: {}", e),
            DbError::JsonError(e) => write!(f, "JsonError: {}", e),
        }
    }
}

impl Error for DbError {}
