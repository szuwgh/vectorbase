use std::io;
use std::io::Error as IOError;
use thiserror::Error;

pub type GyResult<T> = Result<T, GyError>;

#[derive(Error, Debug)]
pub enum GyError {
    #[error("Unexpected: {0}, {1}")]
    UnexpectIO(String, io::Error),
    #[error("Unexpected: {0}")]
    Unexpected(String),
    #[error("db open fail: {0}")]
    DBOpenFail(io::Error),
    #[error("invalid database")]
    ErrInvalid,
    #[error("version mismatch")]
    ErrVersionMismatch,
    #[error("checksum error")]
    ErrChecksum,
    #[error("incompatible value")]
    ErrIncompatibleValue,
}

impl From<&str> for GyError {
    fn from(e: &str) -> Self {
        GyError::Unexpected(e.to_string())
    }
}

impl From<(&str, io::Error)> for GyError {
    fn from(e: (&str, io::Error)) -> Self {
        GyError::UnexpectIO(e.0.to_string(), e.1)
    }
}

impl From<String> for GyError {
    fn from(e: String) -> Self {
        GyError::Unexpected(e)
    }
}

impl From<IOError> for GyError {
    fn from(e: IOError) -> Self {
        GyError::Unexpected(e.to_string())
    }
}

impl From<GyError> for String {
    fn from(e: GyError) -> Self {
        format!("{}", e)
    }
}
