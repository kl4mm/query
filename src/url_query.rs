use std::collections::HashSet;

use crate::{
    filter::{Condition, Filter},
    sort::Sort,
    ParseError,
};

fn check_allowed_fields(field: &str, allowed_fields: &HashSet<&str>) -> Result<(), ParseError> {
    if !allowed_fields.contains(field) {
        Err(ParseError::InvalidField)?
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct UrlQuery {
    pub params: HashSet<String>,
    pub filters: Vec<Filter>,
    pub group: Option<String>,
    pub sort: Option<Sort>,
    pub limit_offset: (Option<String>, Option<String>),
}

impl UrlQuery {
    pub fn new<'a>(
        str: &str,
        allowed_fields: impl Into<HashSet<&'a str>>,
    ) -> Result<Self, ParseError> {
        let allowed_fields: HashSet<&str> = allowed_fields.into();

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
                let filter = Filter::new(v)?;
                check_allowed_fields(&filter.field, &allowed_fields)?;
                filters.push(filter);
                continue;
            }

            if k == "group" {
                check_allowed_fields(v, &allowed_fields)?;
                group = Some(v.to_owned());
                continue;
            }

            if k == "sort" {
                sort = Some(Sort::new(v)?);
                check_allowed_fields(&sort.as_ref().unwrap().field, &allowed_fields)?;
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

            check_allowed_fields(k, &allowed_fields)?;
            filters.push(Filter::from_key_value(k, v, Condition::EQ));

            // To check required:
            params.insert(k.into());
        }

        Ok(Self {
            params,
            filters,
            group,
            sort,
            limit_offset,
        })
    }

    pub fn check_required<'a, T>(&self, required: T) -> Result<(), String>
    where
        T: IntoIterator<Item = &'a str>,
    {
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

    pub fn filters_mut(&mut self) -> &mut Vec<Filter> {
        &mut self.filters
    }

    pub fn group_mut(&mut self) -> &mut Option<String> {
        &mut self.group
    }

    pub fn sort_mut(&mut self) -> &mut Option<Sort> {
        &mut self.sort
    }

    pub fn limit_offset_mut(&mut self) -> &mut (Option<String>, Option<String>) {
        &mut self.limit_offset
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

        let parsed = UrlQuery::new(query, ["userId", "orderId", "price"]).unwrap();

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

        let parsed = UrlQuery::new(query, []).unwrap();

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

        let parsed = UrlQuery::new(query, []).unwrap();

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
    fn test_required() {
        let query = "userId=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed = UrlQuery::new(query, ["userId", "orderId", "price"]).unwrap();

        let v1 = parsed.check_required(["userId"]);
        assert!(v1.is_ok());

        let v1 = parsed.check_required(["userId", "limit", "offset"]);
        assert!(v1.is_err());
    }

    #[test]
    fn test_allowed_fields() {
        let query = "userId=bob&filter[]=orderId-eq-1";

        let result = UrlQuery::new(query, ["userId"]);

        assert_eq!(result, Err(ParseError::InvalidField))
    }
}
