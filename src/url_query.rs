use std::collections::HashSet;

use crate::{
    filter::{Condition, Filter},
    sort::Sort,
    ParseError,
};

#[derive(Debug, PartialEq)]
pub struct UrlQuery {
    pub params: HashSet<String>,
    pub filters: Vec<Filter>,
    pub group: Option<String>,
    pub sort: Option<Sort>,
    pub limit_offset: (Option<String>, Option<String>),
}

impl UrlQuery {
    pub fn new(str: &str, allowed_fields: &HashSet<&str>) -> Result<Self, ParseError> {
        let mut params = HashSet::new();

        let queries: Vec<&str> = str.split("&").collect();
        let mut filters = Vec::new();
        let mut group = None;
        let mut sort = None;
        let mut limit_offset = (None, None);

        for q in queries {
            let (k, v) = match q.split_once("=") {
                Some(kv) => kv,
                None => continue,
            };

            if k == "filter[]" {
                filters.push(Filter::new(v, allowed_fields)?);
                continue;
            }

            if k == "group" {
                group = Some(v.to_owned());
                continue;
            }

            if k == "sort" {
                sort = Some(Sort::new(v, allowed_fields)?);
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

            if !allowed_fields.contains(k) {
                Err(ParseError::InvalidField)?
            }

            // To check required:
            params.insert(k.into());

            filters.push(Filter::from_key_value(k, v, Condition::EQ));
        }

        Ok(Self {
            params,
            filters,
            group,
            sort,
            limit_offset,
        })
    }
}

impl UrlQuery {
    pub fn check_required(&self, required: Vec<&str>) -> Result<(), String> {
        for r in required {
            if let None = self.params.get(r) {
                let mut res = String::new();
                res.push_str(r);
                res.push_str(" is required");
                Err(res)?
            };
        }

        Ok(())
    }

    pub fn check_limit(&self) -> Result<&str, String> {
        match self.limit_offset.0 {
            Some(ref limit) => Ok(limit),
            None => Err(String::from("limit is required")),
        }
    }

    pub fn check_offset(&self) -> Result<&str, String> {
        match self.limit_offset.1 {
            Some(ref offset) => Ok(offset),
            None => Err(String::from("offset is required")),
        }
    }

    pub fn check_limit_and_offset(&self) -> Result<(&str, &str), String> {
        let limit = self.check_limit()?;
        let offset = self.check_offset()?;

        Ok((limit, offset))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        filter::{Condition, Filter},
        sort::{Sort, SortBy},
        ParseError, UrlQuery,
    };

    #[test]
    fn test_parse_query() {
        let query =
            "userId=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc&group=orderId";

        let allowed = HashSet::from(["userId", "orderId", "price"]);
        let parsed = UrlQuery::new(query, &allowed).unwrap();

        let mut params = HashSet::new();
        params.insert("userId".to_owned());

        let expected = UrlQuery {
            params,
            filters: vec![
                Filter {
                    field: "userId".into(),
                    condition: Condition::EQ,
                    value: "bob".into(),
                },
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
            group: Some(String::from("orderId")),
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

        let parsed = UrlQuery::new(query, &HashSet::from([])).unwrap();

        let expected = UrlQuery {
            params: HashSet::default(),
            filters: vec![],
            group: None,
            sort: None,
            limit_offset: (None, None),
        };

        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_parse_query_limit_offset() {
        let query = "limit=10&offset=0";

        let parsed = UrlQuery::new(query, &HashSet::from([])).unwrap();

        let expected = UrlQuery {
            params: HashSet::default(),
            filters: vec![],
            group: None,
            sort: None,
            limit_offset: (Some("10".into()), Some("0".into())),
        };

        assert_eq!(parsed, expected);
        assert!(parsed.check_limit_and_offset().is_ok());
    }

    #[test]
    fn test_is_valid() {
        let query = "userId=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed = UrlQuery::new(query, &HashSet::from(["userId", "orderId", "price"])).unwrap();

        let v1 = parsed.check_required(vec!["userId"]);
        assert!(v1.is_ok());

        let v1 = parsed.check_required(vec!["userId", "limit", "offset"]);
        assert!(v1.is_err());
    }

    #[test]
    fn test_allowed_field() {
        let query = "userId=bob&filter[]=orderId-eq-1";

        let allowed = HashSet::from(["userId"]);
        let result = UrlQuery::new(query, &allowed);

        assert_eq!(result, Err(ParseError::InvalidField))
    }
}
