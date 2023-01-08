use std::{collections::BTreeMap, str::FromStr};

use crate::{filter::Filter, sort::Sort, ParseError};

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query: BTreeMap<String, String>,
    pub filters: Vec<Filter>,
    pub sort: Option<Sort>,
    pub limit_offset: (Option<String>, Option<String>),
}

impl FromStr for Query {
    type Err = ParseError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let mut query: BTreeMap<String, String> = BTreeMap::new();

        let queries: Vec<&str> = str.split("&").collect();
        let mut filters = Vec::new();
        let mut sort = None;
        let mut limit_offset = (None, None);

        for q in queries {
            let (k, v) = match q.split_once("=") {
                Some(kv) => kv,
                None => continue,
            };

            if k == "filter[]" {
                filters.push(v.parse()?);
                continue;
            }

            if k == "sort" {
                sort = Some(v.parse()?);
                continue;
            }

            if k == "limit" {
                limit_offset.0 = Some(v.to_owned());
                continue;
            }

            if k == "offset" {
                limit_offset.1 = Some(v.to_owned());
                continue;
            }

            query.insert(k.into(), v.into());
        }

        Ok(Self {
            query,
            filters,
            sort,
            limit_offset,
        })
    }
}

impl Query {
    pub fn check_valid(&self, required: Vec<&str>) -> Result<(), String> {
        for r in required {
            if let None = self.query.get(r) {
                let mut res = String::new();
                res.push_str(r);
                res.push_str(" is required");
                Err(res)?
            };
        }

        Ok(())
    }

    pub fn check_limit_and_offset(&self) -> Result<(&str, &str), String> {
        if let None = self.limit_offset.0 {
            Err(String::from("limit is required"))?;
        };
        if let None = self.limit_offset.1 {
            Err(String::from("offset is required"))?;
        };

        Ok((
            self.limit_offset.0.as_ref().unwrap(),
            self.limit_offset.1.as_ref().unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        filter::{Condition, Filter},
        query::Query,
        sort::{Sort, SortBy},
    };

    #[test]
    fn test_parse_query() {
        let query = "userId=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed: Query = query.parse().unwrap();

        let mut query: BTreeMap<String, String> = BTreeMap::new();
        query.insert("userId".into(), "bob".into());

        let expected = Query {
            query,
            filters: vec![
                Filter {
                    field: "orderId".into(),
                    condition: Condition::EQ,
                    value: "1".into(),
                },
                Filter {
                    field: "price".into(),
                    condition: Condition::GE,
                    value: "200".into(),
                },
            ],
            sort: Some(Sort {
                field: String::from("price"),
                sort_by: SortBy::DESC,
            }),
            limit_offset: (None, None),
        };

        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_query_empty() {
        let query = "";

        let parsed: Query = query.parse().unwrap();

        let expected = Query {
            query: BTreeMap::default(),
            filters: vec![],
            sort: None,
            limit_offset: (None, None),
        };

        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_query_limit_offset() {
        let query = "limit=10&offset=0";

        let parsed: Query = query.parse().unwrap();

        let expected = Query {
            query: BTreeMap::default(),
            filters: vec![],
            sort: None,
            limit_offset: (Some("10".into()), Some("0".into())),
        };

        assert_eq!(parsed, expected);
        assert!(parsed.check_limit_and_offset().is_ok());
    }

    #[test]
    fn test_is_valid() {
        let query = "userId=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed: Query = query.parse().unwrap();

        let v1 = parsed.check_valid(vec!["userId"]);
        assert!(v1.is_ok());

        let v1 = parsed.check_valid(vec!["userId", "limit", "offset"]);
        assert!(v1.is_err());
    }
}
