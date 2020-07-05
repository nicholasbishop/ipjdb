use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid id")]
    InvalidId,

    #[error("io error: {0}")]
    IoError(#[from] io::Error),

    #[error("invalid json: {0}")]
    JsonError(#[from] serde_json::error::Error),
}
