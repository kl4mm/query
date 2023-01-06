use std::{collections::BTreeMap, str::FromStr};

use convert_case::{Case, Casing};

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

impl Query {
    pub fn gen_sql(&self, table: &str, fields: Vec<&str>, joins: Vec<&str>) -> String {
        let fields = fields.join(", ");
        let mut sql = String::from("SELECT ");
        sql.push_str(&fields);
        sql.push_str(" FROM ");
        sql.push_str(table);

        for join in joins {
            sql.push_str(" ");
            sql.push_str(join)
        }

        // Required fields from the query:
        let mut queryv = Vec::new();
        for (i, key) in self.query.keys().enumerate() {
            let mut query = String::new();
            query.push_str(&key.to_case(Case::Snake));
            query.push_str(" = ");
            query.push_str("$");
            let i = i + 1;
            query.push_str(&i.to_string());

            queryv.push(query);
        }
        let query = queryv.join(" AND ");

        // Filters:
        let mut filterv = Vec::new();
        for filter in &self.filters {
            filterv.push(filter.to_camel_string());
        }
        let filter = filterv.join(" AND ");

        if queryv.len() > 0 {
            sql.push_str(" WHERE ");
            dbg!(&query);
            sql.push_str(&query);
            if filterv.len() > 0 {
                sql.push_str(" AND ");
                sql.push_str(&filter);
            }
        }

        // Sort:
        if let Some(ref sort) = self.sort {
            sql.push_str(" ORDER BY ");
            sql.push_str(&sort.to_camel_string());
        }

        sql
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

    #[test]
    fn test_gen_sql_no_filters_or_sort() {
        let query = "userId=123&userName=bob";

        let parsed: Query = query.parse().unwrap();

        let sql = parsed.gen_sql("orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_gen_sql_no_sort() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1";

        let parsed: Query = query.parse().unwrap();

        let sql = parsed.gen_sql("orders", vec!["id", "status"], vec![]);

        let expected =
            "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = 1";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_gen_sql() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed: Query = query.parse().unwrap();

        let sql = parsed.gen_sql("orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = 1 AND price >= 200 ORDER BY price DESC";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_gen_sql_with_join() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed: Query = query.parse().unwrap();

        let sql = parsed.gen_sql(
            "orders",
            vec!["id", "status"],
            vec!["JOIN users ON users.id = order.user_id"],
        );

        let expected = "SELECT id, status FROM orders JOIN users ON users.id = order.user_id WHERE user_id = $1 AND user_name = $2 AND order_id = 1 AND price >= 200 ORDER BY price DESC";

        assert_eq!(sql, expected);
    }
}
