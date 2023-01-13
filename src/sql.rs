use std::collections::{BTreeMap, HashMap};

use convert_case::{Case, Casing};

use crate::{filter::Filter, sort::Sort, UrlQuery};

/// Generates Postgres SQL statement with params.
///
/// # Examples
///
/// ```
/// use std::collections::{HashMap, HashSet};
/// use query::{UrlQuery, sql};
///
/// let query = "userId=123&userName=bob";
///
/// let parsed = UrlQuery::new(query, &HashSet::from(["userId", "userName"])).unwrap();
///
/// let (sql, params) = sql::gen_psql(&parsed, "orders", vec!["id", "status"], vec![], HashMap::default());
///
/// assert_eq!(sql, "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2");
/// assert_eq!(params.len(), 2);
/// ```
pub fn gen_psql<'a>(
    input: &'a UrlQuery,
    table: &str,
    columns: Vec<&str>,
    joins: Vec<&str>,
    map_columns: HashMap<&str, &str>,
) -> (String, BTreeMap<&'a str, &'a str>) {
    let mut sql = gen_sql_select(table, columns);

    // Joins:
    append_joins(&mut sql, joins);

    // WHERE clause, returns bind args
    let args = append_where(&mut sql, &input.filters, &map_columns);

    // Group:
    if let Some(ref group) = input.group {
        append_group(&mut sql, group, &map_columns);
    }

    // Sort:
    if let Some(ref sort) = input.sort {
        append_sort(&mut sql, sort, &map_columns);
    }

    // Limit & offset:
    if let Ok(limit) = input.check_limit() {
        append_limit(&mut sql, limit);

        if let Ok(offset) = input.check_offset() {
            append_offset(&mut sql, offset);
        }
    }

    (sql, args)
}

fn gen_sql_select(table: &str, columns: Vec<&str>) -> String {
    let mut sql = String::from("SELECT ");
    let columns = columns.join(", ");
    sql.push_str(&columns);
    sql.push_str(" FROM ");
    sql.push_str(table);
    sql
}

fn append_joins(sql: &mut String, joins: Vec<&str>) {
    for join in joins {
        sql.push_str(" ");
        sql.push_str(join)
    }
}

fn append_where<'a>(
    sql: &mut String,
    filters: &'a Vec<Filter>,
    map_columns: &HashMap<&str, &str>,
) -> BTreeMap<&'a str, &'a str> {
    let mut args: BTreeMap<&str, &str> = BTreeMap::new();

    // Filters:
    let mut filterv = Vec::new();
    for filter in filters.iter() {
        let table = map_columns.get(filter.field.as_str());
        filterv.push(filter.to_camel_psql_string(args.len() + 1, table));
        args.insert(&filter.field, &filter.value);
    }
    let filter = filterv.join(" AND ");

    // WHERE clause
    if filterv.len() > 0 {
        sql.push_str(" WHERE ");
        sql.push_str(&filter);
    }

    args
}

fn append_group(sql: &mut String, group: &str, map_columns: &HashMap<&str, &str>) {
    sql.push_str(" GROUP BY ");
    if let Some(table) = map_columns.get(group) {
        sql.push_str(table);
        sql.push_str(".");
    }
    sql.push_str(&group.to_case(Case::Camel))
}

fn append_sort(sql: &mut String, sort: &Sort, map_columns: &HashMap<&str, &str>) {
    let table = map_columns.get(sort.field.as_str());
    sql.push_str(" ORDER BY ");
    sql.push_str(&sort.to_camel_string(table));
}

fn append_limit(sql: &mut String, limit: &str) {
    sql.push_str(" LIMIT ");
    sql.push_str(limit);
}

fn append_offset(sql: &mut String, offset: &str) {
    sql.push_str(" OFFSET ");
    sql.push_str(offset);
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use crate::UrlQuery;

    #[test]
    fn test_gen_sql_no_filters_or_sort() {
        let query = "userId=123&userName=bob";

        let parsed = UrlQuery::new(query, &HashSet::from(["userId", "userName"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_gen_sql_no_sort() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

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

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 AND price >= $4 ORDER BY price DESC";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_gen_sql_limit() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1&limit=10";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 LIMIT 10";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_gen_sql_limit_offset() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1&limit=10&offset=0";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 LIMIT 10 OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_gen_sql_ordering() {
        let query = "limit=10&offset=0&filter[]=orderId-eq-1&userId=123&userName=bob";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected = "SELECT id, status FROM orders WHERE order_id = $1 AND user_id = $2 AND user_name = $3 LIMIT 10 OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_gen_sql_no_params() {
        let query = "limit=10&offset=0&filter[]=orderId-eq-1&filter[]=userId-eq-1";

        let parsed =
            UrlQuery::new(query, &HashSet::from(["userId", "userName", "orderId"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

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
            HashMap::default(),
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

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec!["id", "status"],
            vec![],
            HashMap::default(),
        );

        let expected =
            "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 GROUP BY id";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_gen_sql_map_columns() {
        let query = "id=1&group=id&sort=createdAt-desc";

        let parsed = UrlQuery::new(query, &HashSet::from(["id", "createdAt"])).unwrap();

        let (sql, params) = super::gen_psql(
            &parsed,
            "orders",
            vec![
                "orders.id",
                "user_id",
                "status",
                "address_id",
                "orders.created_at",
            ],
            vec![
                "JOIN order_items ON orders.id = order_items.order_id",
                "JOIN inventory ON order_items.inventory_id = inventory.id",
            ],
            HashMap::from([("id", "orders"), ("createdAt", "orders")]),
        );

        let expected =
            "SELECT orders.id, user_id, status, address_id, orders.created_at FROM orders \
             JOIN order_items ON orders.id = order_items.order_id \
             JOIN inventory ON order_items.inventory_id = inventory.id \
             WHERE orders.id = $1 GROUP BY orders.id ORDER BY orders.created_at DESC";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 1);
    }
}
