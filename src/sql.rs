use convert_case::{Case, Casing};

use crate::query::Query;

/// Generates SQL statement with params.
///
/// # Examples
///
/// ```
/// use query::sql::gen_psql;
///
/// let query = "userId=123&userName=bob";
///
/// let parsed = query.parse().unwrap();
///
/// let (sql, params) = gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);
///
/// assert_eq!(sql, "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2");
/// assert_eq!(params.len(), 2);
/// ```
pub fn gen_psql<'a>(
    input: &'a Query,
    table: &str,
    fields: Vec<&str>,
    joins: Vec<&str>,
) -> (String, Vec<&'a String>) {
    let mut params = Vec::new();

    // Fields:
    // TODO: empty = *
    let fields = fields.join(", ");
    let mut sql = String::from("SELECT ");
    sql.push_str(&fields);
    sql.push_str(" FROM ");
    sql.push_str(table);

    // Joins:
    for join in joins {
        sql.push_str(" ");
        sql.push_str(join)
    }

    let mut param_count = 0;

    // Required fields from the query:
    let mut queryv = Vec::new();
    for (i, key) in input.query.keys().enumerate() {
        let mut query = String::new();
        query.push_str(&key.to_case(Case::Snake));
        query.push_str(" = ");
        query.push_str("$");
        let i = i + 1;
        param_count += 1;
        query.push_str(&i.to_string());

        queryv.push(query);
        params.push(input.query.get(key).unwrap())
    }
    let query = queryv.join(" AND ");

    // Filters:
    let mut filterv = Vec::new();
    for (i, filter) in input.filters.iter().enumerate() {
        filterv.push(filter.to_camel_psql_string(param_count + i + 1));
        params.push(&filter.value)
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
    if let Some(ref sort) = input.sort {
        sql.push_str(" ORDER BY ");
        sql.push_str(&sort.to_camel_string());
    }

    (sql, params)
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::query::Query;

    #[test]
    fn test_gen_sql_no_filters_or_sort() {
        let query = "userId=123&userName=bob";

        let parsed = Query::from_str(query).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_gen_sql_no_sort() {
        let query = "userId=123&userName=bob&filter[]=orderId-eq-1";

        let parsed = Query::from_str(query).unwrap();

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

        let parsed = Query::from_str(query).unwrap();

        let (sql, params) = super::gen_psql(&parsed, "orders", vec!["id", "status"], vec![]);

        let expected = "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2 AND order_id = $3 AND price >= $4 ORDER BY price DESC";

        assert_eq!(sql, expected);
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn test_gen_sql_with_join() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc";

        let parsed = Query::from_str(query).unwrap();

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
}
