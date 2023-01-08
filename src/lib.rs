pub mod filter;
pub mod query;
pub mod sort;
pub mod sql;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    InvalidSort,
    InvalidSortBy,
    InvalidFilter,
    InvalidCondition,
    InvalidField,
}
