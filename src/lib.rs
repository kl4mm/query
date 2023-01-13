pub mod filter;
pub mod sort;
pub mod sql;
pub mod url_query;

use std::collections::HashSet;

pub use url_query::UrlQuery;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    InvalidSort,
    InvalidSortBy,
    InvalidFilter,
    InvalidCondition,
    InvalidField,
}

fn check_allowed_fields(field: &str, allowed_fields: &HashSet<&str>) -> Result<(), ParseError> {
    if !allowed_fields.contains(field) {
        Err(ParseError::InvalidField)?
    }

    Ok(())
}
