use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    InvalidId,
    IoError(io::Error),
    JsonError(serde_json::error::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::IoError(error)
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(error: serde_json::error::Error) -> Self {
        Error::JsonError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Error::InvalidId => write!(f, "InvalidId"),
            Error::IoError(e) => write!(f, "IoError: {}", e),
            Error::JsonError(e) => write!(f, "JsonError: {}", e),
        }
    }
}

impl std::error::Error for Error {}
