use std::collections::BTreeMap;

use convert_case::{Case, Casing};

use crate::UrlQuery;

/// Generates SQL statement with params.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use query::{UrlQuery, sql};
///
/// let query = "userId=123&userName=bob";
///
/// let parsed = UrlQuery::new(query, &HashSet::from(["userId", "userName"])).unwrap();
///
/// let (sql, params) = sql::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);
///
/// assert_eq!(sql, "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2");
/// assert_eq!(params.len(), 2);
/// ```
pub fn gen_psql<'a>(
    input: &'a UrlQuery,
    table: &str,
    columns: Vec<&str>,
    joins: Vec<&str>,
) -> (String, BTreeMap<&'a str, &'a str>) {
    let mut params: BTreeMap<&str, &str> = BTreeMap::new();

    // Fields:
    let columns = columns.join(", ");
    let mut sql = String::from("SELECT ");
    sql.push_str(&columns);
    sql.push_str(" FROM ");
    sql.push_str(table);

    // Joins:
    for join in joins {
        sql.push_str(" ");
        sql.push_str(join)
    }

    // Required fields from the query:
    let mut queryv = Vec::new();
    for key in input.query.keys() {
        let mut query = String::new();
        query.push_str(&key.to_case(Case::Snake));
        query.push_str(" = ");
        query.push_str("$");
        query.push_str(&(params.len() + 1).to_string());

        queryv.push(query);
        params.insert(key, input.query.get(key).unwrap());
    }
    let query = queryv.join(" AND ");

    // Filters:
    let mut filterv = Vec::new();
    for filter in input.filters.iter() {
        filterv.push(filter.to_camel_psql_string(params.len() + 1));
        params.insert(&filter.field, &filter.value);
    }
    let filter = filterv.join(" AND ");

    // WHERE clause
    let has_query = queryv.len() > 0;
    let has_filter = filterv.len() > 0;
    if has_query {
        sql.push_str(" WHERE ");
        sql.push_str(&query);
    }

    if has_filter && !has_query {
        sql.push_str(" WHERE ");
        sql.push_str(&filter);
    } else if has_filter {
        sql.push_str(" AND ");
        sql.push_str(&filter);
    }

    // Group:
    if let Some(ref group) = input.group {
        sql.push_str(" GROUP BY ");
        sql.push_str(&group.to_case(Case::Camel))
    }

    // Sort:
    if let Some(ref sort) = input.sort {
        sql.push_str(" ORDER BY ");
        sql.push_str(&sort.to_camel_string());
    }

    // Limit & offset:
    if let Ok((limit, offset)) = input.check_limit_and_offset() {
        sql.push_str(" LIMIT ");
        sql.push_str(limit);

        sql.push_str(" OFFSET ");
        sql.push_str(offset);
    }

    (sql, params)
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::UrlQuery;

    #[test]
    fn test_gen_sql_no_filters_or_sort() {
        let query = "userId=123&userName=bob";

        let parsed = UrlQuery::new(query, &HashSet::from(["userId", "userName"])).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_gen_sql_no_sort() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected =
            "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_gen_sql() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 AND price >= $4 ORDER BY price DESC";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_gen_sql_limit_offset() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1&limit=10&offset=0";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 LIMIT 10 OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    #[ignore]
    fn test_gen_sql_ordering() {
        let query = "limit=10&offset=0&filter[]=orderId-eq-1&userId=123&userName=bob";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE order_id = $1 AND user_id = $2 AND user_name = $3 LIMIT 10 OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 5);
    }

    #[test]
    fn test_gen_sql_no_params() {
        let query = "limit=10&offset=0&filter[]=orderId-eq-1&filter[]=userId-eq-1";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected =
            "SELECT id, status FROM orders WHERE order_id = $1 AND user_id = $2 LIMIT 10 OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_gen_sql_with_join() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec!["JOIN users ON users.id = order.user_id"],
        );

        let expected = "SELECT id, status FROM orders JOIN users ON users.id = order.user_id WHERE user_id = $1 AND user_name = $2 AND order_id = $3 AND price >= $4 ORDER BY price DESC";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_gen_sql_group() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1&group=id";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected =
            "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 GROUP BY id";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }
}