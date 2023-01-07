use std::{collections::BTreeMap, str::FromStr};

use crate::{filter::Filter, sort::Sort, ParseError};

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query: BTreeMap<String, String>,
    pub filters: Vec<Filter>,
    pub sort: Option<Sort>,
}

impl FromStr for Query {
    type Err = ParseError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let mut query: BTreeMap<String, String> = BTreeMap::new();

        let queries: Vec<&str> = str.split("&").collect();
        let mut filters = Vec::new();
        let mut sort = None;

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

            query.insert(k.into(), v.into());
        }

        Ok(Self {
            query,
            filters,
            sort,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{filter::Condition, sort::SortBy};

    use super::*;

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
        };

        assert_eq!(parsed, expected);
    }
}
