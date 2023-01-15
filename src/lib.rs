#[macro_use]

pub mod filter;
pub mod sort;
pub mod sql;
pub mod url_query;

pub use url_query::UrlQuery;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    InvalidSort,
    InvalidSortBy,
    InvalidFilter,
    InvalidCondition,
    InvalidField,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidSort => write!(f, "invalid sort"),
            ParseError::InvalidSortBy => write!(f, "invalid sort by"),
            ParseError::InvalidFilter => write!(f, "invalid filter"),
            ParseError::InvalidCondition => write!(f, "invalid filter condition"),
            ParseError::InvalidField => write!(f, "invalid field"),
        }
    }
}

impl std::error::Error for ParseError {}
