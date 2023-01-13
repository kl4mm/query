use std::{collections::HashSet, str::FromStr};

use convert_case::{Case, Casing};

use crate::ParseError;

#[derive(Debug, PartialEq)]
pub enum Condition {
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE,
}

impl FromStr for Condition {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "eq" => Ok(Condition::EQ),
            "ne" => Ok(Condition::NE),
            "gt" => Ok(Condition::GT),
            "ge" => Ok(Condition::GE),
            "lt" => Ok(Condition::LT),
            "le" => Ok(Condition::LE),
            _ => Err(ParseError::InvalidCondition),
        }
    }
}

impl Condition {
    pub fn as_str(&self) -> &str {
        match self {
            Condition::EQ => "=",
            Condition::NE => "!=",
            Condition::GT => ">",
            Condition::GE => ">=",
            Condition::LT => "<",
            Condition::LE => "<=",
        }
    }
}

// filter[]=field-gr-0 -> some_value > 0
#[derive(Debug, PartialEq)]
pub struct Filter {
    pub field: String,
    pub condition: Condition,
    pub value: String,
}

impl Filter {
    pub fn new(str: &str, allowed_fields: &HashSet<&str>) -> Result<Self, ParseError> {
        let split: Vec<&str> = str.split('-').collect();
        if split.len() != 3 {
            Err(ParseError::InvalidFilter)?
        }

        if !allowed_fields.contains(split[0]) {
            Err(ParseError::InvalidField)?
        }

        let condition: Condition = split[1].parse()?;

        Ok(Self {
            field: split[0].into(),
            condition,
            value: split[2].into(),
        })
    }

    pub fn from_key_value(key: &str, value: &str, condition: Condition) -> Self {
        Self {
            field: key.into(),
            condition,
            value: value.into(),
        }
    }

    pub fn to_string(&self) -> String {
        let mut filter = String::new();
        filter.push_str(&self.field);
        filter.push_str(" ");
        filter.push_str(self.condition.as_str());
        filter.push_str(" ");
        filter.push_str(&self.value);

        filter
    }

    fn to_sql(&self, mut filter: String, idx: usize, case: Option<Case>) -> String {
        match case {
            Some(case) => filter.push_str(&self.field.to_case(case)),
            None => filter.push_str(&self.field),
        }
        filter.push_str(" ");
        filter.push_str(self.condition.as_str());
        filter.push_str(" ");
        filter.push_str("$");
        filter.push_str(&idx.to_string());

        filter
    }

    pub fn to_sql_map_table(&self, idx: usize, table: Option<&&str>, case: Option<Case>) -> String {
        let mut filter = String::new();
        if let Some(table) = table {
            filter.push_str(table);
            filter.push_str(".")
        }

        self.to_sql(filter, idx, case)
    }
}
