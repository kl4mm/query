pub mod filter;
pub mod query;
pub mod sort;
pub mod sql;

#[derive(Debug)]
pub enum ParseError {
    InvalidSort,
    InvalidSortBy,
    InvalidFilter,
    InvalidCondition,
}
