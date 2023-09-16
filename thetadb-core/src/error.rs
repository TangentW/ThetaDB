use std::fmt::{Display, Formatter};

use crate::{medium, meta, tx};

/// A `Result` type that all API calls in ThetaDB will return.
pub type Result<T> = std::result::Result<T, Error>;

/// All possible error cases that can be return by API calls in ThetaDB.
#[derive(Debug, Clone, Copy)]
pub enum ErrorCode {
    /// An error occured during an I/O operation.
    IO,
    /// The input (e.g., key, value) is invalid.
    InputInvalid,
    /// The database file is not in the expected format or state.
    FileUnexpected,
    /// The database is corrupted.
    DatabaseCorrupted,
}

#[derive(Debug)]
pub struct Error {
    code: ErrorCode,
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl Display for ErrorCode {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::IO => "IO error",
            Self::InputInvalid => "invalid input argument",
            Self::FileUnexpected => "unexpected database file",
            Self::DatabaseCorrupted => "database is corrupted",
        })
    }
}

impl Error {
    #[inline]
    pub fn code(&self) -> ErrorCode {
        self.code
    }
}

impl Display for Error {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.source)
    }
}

impl std::error::Error for Error {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<medium::file::Error> for Error {
    #[inline]
    fn from(value: medium::file::Error) -> Self {
        Self {
            code: ErrorCode::IO,
            source: Box::new(value),
        }
    }
}

impl From<tx::InputInvalid> for Error {
    #[inline]
    fn from(value: tx::InputInvalid) -> Self {
        Self {
            code: ErrorCode::InputInvalid,
            source: Box::new(value),
        }
    }
}

impl From<meta::ValidationError> for Error {
    #[inline]
    fn from(value: meta::ValidationError) -> Self {
        Self {
            code: ErrorCode::FileUnexpected,
            source: Box::new(value),
        }
    }
}

impl From<medium::mapping::Error> for Error {
    #[inline]
    fn from(value: medium::mapping::Error) -> Self {
        Self {
            code: ErrorCode::DatabaseCorrupted,
            source: Box::new(value),
        }
    }
}
