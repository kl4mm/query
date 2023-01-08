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
    pub fn new(str: &str, fields: &HashSet<&str>) -> Result<Self, ParseError> {
        let split: Vec<&str> = str.split('-').collect();
        if split.len() != 3 {
            Err(ParseError::InvalidFilter)?
        }

        if !fields.contains(split[0]) {
            Err(ParseError::InvalidField)?
        }

        let condition: Condition = split[1].parse()?;

        Ok(Self {
            field: split[0].into(),
            condition,
            value: split[2].into(),
        })
    }

    pub fn to_string(&self) -> String {
        let mut res = String::new();
        res.push_str(&self.field);
        res.push_str(" ");
        res.push_str(self.condition.as_str());
        res.push_str(" ");
        res.push_str(&self.value);

        res
    }

    pub fn to_camel_string(&self) -> String {
        let mut res = String::new();
        res.push_str(&self.field.to_case(Case::Snake));
        res.push_str(" ");
        res.push_str(self.condition.as_str());
        res.push_str(" ");
        res.push_str(&self.value);

        res
    }

    pub fn to_camel_psql_string(&self, idx: usize) -> String {
        let mut res = String::new();
        res.push_str(&self.field.to_case(Case::Snake));
        res.push_str(" ");
        res.push_str(self.condition.as_str());
        res.push_str(" ");
        res.push_str("$");
        res.push_str(&idx.to_string());

        res
    }
}
