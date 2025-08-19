use std::io;
use thiserror::Error;

pub type VBResult<T> = Result<T, VBError>;

#[derive(Error, Debug)]
pub enum VBError {
    #[error("io EOF")]
    EOF,
    #[error("Unexpected io: {0}, {1}")]
    UnexpectIO(String, io::Error),
    #[error("Unexpected: {0}")]
    Unexpected(String),
}

impl From<&str> for VBError {
    fn from(e: &str) -> Self {
        VBError::Unexpected(e.to_string())
    }
}

impl From<(&str, io::Error)> for VBError {
    fn from(e: (&str, io::Error)) -> Self {
        VBError::UnexpectIO(e.0.to_string(), e.1)
    }
}

impl From<String> for VBError {
    fn from(e: String) -> Self {
        VBError::Unexpected(e)
    }
}
