pub mod filter;
pub mod query;
pub mod sort;

#[derive(Debug)]
pub enum ParseError {
    InvalidSort,
    InvalidSortBy,
    InvalidFilter,
    InvalidCondition,
}
