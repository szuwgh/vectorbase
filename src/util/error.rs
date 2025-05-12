use crate::PathBuf;
use fst::Error as FstError;
use jiebars::Jieba;
use std::io;
use std::io::Error as IOError;
use std::sync::{PoisonError, TryLockError};
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use wwml::error::GError;
pub type GyResult<T> = Result<T, GyError>;

#[derive(Error, Debug)]
pub enum GyError {
    #[error("io EOF")]
    EOF,
    #[error("Wal io EOF")]
    WalEOF,
    #[error("Unexpected io: {0}, {1}")]
    UnexpectIO(String, io::Error),
    #[error("Unexpected: {0}")]
    Unexpected(String),
    #[error("db open fail: {0}")]
    DBOpenFail(io::Error),
    #[error("data path is not dir: {0}")]
    DataPathNotDir(PathBuf),
    #[error("index not found: {0}")]
    IndexDirNotExist(PathBuf),
    #[error("invalid database")]
    ErrInvalid,
    #[error("invalid database footer")]
    ErrFooter,
    #[error("version mismatch")]
    ErrVersionMismatch,
    #[error("checksum error")]
    ErrChecksum,
    #[error("incompatible value")]
    ErrIncompatibleValue,
    #[error("invalid lock")]
    ErrInvalidLock,
    #[error("wal overflow")]
    ErrWalOverflow,
    #[error("document overflow")]
    ErrDocumentOverflow,
    #[error("document not found")]
    ErrDocumentNotFound,
    #[error("invalid value type")]
    ErrInvalidValueType,
    #[error("invalid fst: {0}")]
    ErrInvalidFst(FstError),
    #[error("bad magic number")]
    ErrBadMagicNumber,
    #[error("serde json err: {0}")]
    ErrSerdeJson(serde_json::Error),
    #[error("not found from bloom {0}")]
    ErrNotFoundTermFromBloom(String),
    #[error("collection wal invalid")]
    ErrCollectionWalInvalid,
    #[error("err send invalid")]
    ErrSendInvalid(String),
    #[error("term not found:{0}")]
    ErrTermNotFound(String),
    #[error("Gerror:{0}")]
    ErrGError(GError),
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

impl From<FstError> for GyError {
    fn from(e: FstError) -> Self {
        GyError::ErrInvalidFst(e)
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

impl<T> From<PoisonError<T>> for GyError {
    fn from(e: PoisonError<T>) -> Self {
        GyError::ErrInvalidLock
    }
}

impl<T> From<TryLockError<T>> for GyError {
    fn from(e: TryLockError<T>) -> Self {
        GyError::ErrInvalidLock
    }
}

impl From<serde_json::Error> for GyError {
    fn from(e: serde_json::Error) -> Self {
        GyError::ErrSerdeJson(e)
    }
}

impl<T> From<SendError<T>> for GyError {
    fn from(e: SendError<T>) -> Self {
        GyError::ErrSendInvalid(e.to_string())
    }
}

impl From<GError> for GyError {
    fn from(e: GError) -> Self {
        GyError::ErrGError(e)
    }
}
