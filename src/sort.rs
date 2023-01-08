use std::{collections::HashSet, str::FromStr};

use convert_case::{Case, Casing};

use crate::ParseError;

// sort=field-desc
#[derive(Debug, PartialEq)]
pub struct Sort {
    pub field: String,
    pub sort_by: SortBy,
}

impl Sort {
    pub fn new(str: &str, fields: &HashSet<&str>) -> Result<Self, ParseError> {
        let (field, sort_by) = str
            .split_once("-")
            .map(|(f, s)| (f.to_owned(), s))
            .ok_or_else(|| ParseError::InvalidSort)?;

        if !fields.contains(field.as_str()) {
            Err(ParseError::InvalidField)?
        }

        let sort_by = SortBy::from_str(sort_by)?;

        Ok(Sort { field, sort_by })
    }

    pub fn to_string(&self) -> String {
        let mut sort = String::new();
        sort.push_str(&self.field);
        sort.push_str(" ");
        sort.push_str(self.sort_by.as_str());

        sort
    }

    pub fn to_camel_string(&self) -> String {
        let mut sort = String::new();
        sort.push_str(&self.field.to_case(Case::Snake));
        sort.push_str(" ");
        sort.push_str(self.sort_by.as_str());

        sort
    }
}

#[derive(Debug, PartialEq)]
pub enum SortBy {
    ASC,
    DESC,
}

impl FromStr for SortBy {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "asc" => Ok(Self::ASC),
            "desc" => Ok(Self::DESC),
            _ => Err(ParseError::InvalidSortBy),
        }
    }
}

impl SortBy {
    pub fn as_str(&self) -> &str {
        match self {
            Self::ASC => "ASC",
            Self::DESC => "DESC",
        }
    }
}
