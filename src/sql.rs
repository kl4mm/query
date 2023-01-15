use std::collections::HashMap;

use convert_case::{Case, Casing};

use crate::{filter::Filter, sort::Sort, UrlQuery};

pub enum Database {
    Postgres,
}

/// Generates an SQL query
///
/// # Examples
///
/// ```
/// use std::collections::{HashMap, HashSet};
/// use query::{UrlQuery, sql::{Database, QueryBuilder}};
///
/// let query = "userId=123&userName=bob";
///
/// let parsed = UrlQuery::new(query, &HashSet::from(["userId", "userName"])).unwrap();
///
/// let (sql, args) = QueryBuilder::from_str("SELECT id, status FROM orders", parsed, Database::Postgres).build();
///
/// assert_eq!(sql, "SELECT id, status FROM orders WHERE user_id = $1 AND user_name = $2");
/// assert_eq!(args.len(), 2);
/// ```
pub struct QueryBuilder<'a> {
    url_query: UrlQuery,
    _database: Database,
    map_columns: HashMap<&'a str, &'a str>,
    sql: String,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(table: &str, columns: Vec<&str>, url_query: UrlQuery, database: Database) -> Self {
        let sql = gen_sql_select(table, columns);

        Self {
            url_query,
            _database: database,
            map_columns: HashMap::default(),
            sql,
        }
    }

    pub fn from_str(sql: &str, url_query: UrlQuery, database: Database) -> Self {
        Self {
            url_query,
            _database: database,
            map_columns: HashMap::default(),
            sql: sql.into(),
        }
    }

    pub fn append(mut self, sql: &str) -> Self {
        self.sql.push_str(" ");
        self.sql.push_str(sql);

        self
    }

    pub fn map_columns(mut self, map_columns: HashMap<&'a str, &'a str>) -> Self {
        self.map_columns = map_columns;

        self
    }

    pub fn build(mut self) -> (String, Vec<(String, String)>) {
        // WHERE clause, returns bind args
        let args = append_where(&mut self.sql, &self.url_query.filters, &self.map_columns);

        // Group:
        if let Some(ref group) = self.url_query.group {
            append_group(&mut self.sql, group, &self.map_columns);
        }

        // Sort:
        if let Some(ref sort) = self.url_query.sort {
            append_sort(&mut self.sql, sort, &self.map_columns);
        }

        // Limit & offset:
        if let Ok(limit) = self.url_query.check_limit() {
            append_limit(&mut self.sql, limit);

            if let Ok(offset) = self.url_query.check_offset() {
                append_offset(&mut self.sql, offset);
            }
        }

        (self.sql, args)
    }
}

fn gen_sql_select(table: &str, columns: Vec<&str>) -> String {
    let mut sql = String::from("SELECT ");
    let columns = columns.join(", ");
    sql.push_str(&columns);
    sql.push_str(" FROM ");
    sql.push_str(table);
    sql
}

fn append_where(
    sql: &mut String,
    filters: &Vec<Filter>,
    map_columns: &HashMap<&str, &str>,
) -> Vec<(String, String)> {
    let mut args: Vec<(String, String)> = Vec::new();

    // Filters:
    let mut filterv = Vec::new();
    for filter in filters.iter() {
        let table = map_columns.get(filter.field.as_str());
        filterv.push(filter.to_sql_map_table(args.len() + 1, table, Some(Case::Snake)));
        args.push((filter.field.to_owned(), filter.value.to_owned()));
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
    sql.push_str(&group.to_case(Case::Snake))
}

fn append_sort(sql: &mut String, sort: &Sort, map_columns: &HashMap<&str, &str>) {
    let table = map_columns.get(sort.field.as_str());
    sql.push_str(" ORDER BY ");
    sql.push_str(&sort.to_sql_map_table(table, Some(Case::Snake)));
}

fn append_limit(sql: &mut String, limit: &str) {
    sql.push_str(" LIMIT ");
    sql.push_str(limit);
}

fn append_offset(sql: &mut String, offset: &str) {
    sql.push_str(" OFFSET ");
    sql.push_str(offset);
}

/// Bind args to an sqlx query with the required types.
///
/// ```ignore
/// pub async fn get_orders(
///     pool: &PgPool,
///     query: UrlQuery,
/// ) -> Result<Vec<Order>, Either<sqlx::Error, ParseError>> {
///     let (sql, args) = QueryBuilder::from_str(
///         "SELECT * FROM orders",
///         query,
///         Postgres,
///     )
///     .build();
///
///     let mut query = sqlx::query_as(&sql);
///
///     bind!(
///         args => query,
///         error: Either::Right(ParseError),
///         "id" => Uuid,
///         "userId" => i64
///     );
///
///     Ok(query.fetch_all(pool).await.map_err(|e| Either::Left(e))?)
/// }
/// ```
#[macro_export]
macro_rules! sqlx_bind {
    ( $args:ident => $query:ident, error: $error:expr, $( $x:expr => $t:ty ),* ) => {
        {
            for (column, arg) in $args {
                match column.as_str() {
                    $(
                        $x => {
                            let parsed: $t = arg.parse().map_err(|_| {
                                $error
                            })?;
                            $query = $query.bind(parsed);
                        }
                    )*
                    _ => {}
                }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use crate::{
        filter::{Condition, Filter},
        UrlQuery,
    };

    use super::{Database, QueryBuilder};

    #[test]
    fn test_query_builder_from_str() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc&limit=10&offset=0";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, args) =
            QueryBuilder::from_str("SELECT * FROM orders", parsed, Database::Postgres).build();

        let expected = "SELECT * FROM orders \
        WHERE user_id = $1 AND user_name = $2 \
        AND order_id = $3 AND price >= $4 \
        ORDER BY price DESC \
        LIMIT 10 \
        OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(args.len(), 4);
    }

    #[test]
    fn test_query_builder_new() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc&limit=10&offset=0";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, args) =
            QueryBuilder::new("orders", vec!["id", "status"], parsed, Database::Postgres).build();

        let expected = "SELECT id, status FROM orders \
        WHERE user_id = $1 AND user_name = $2 \
        AND order_id = $3 AND price >= $4 \
        ORDER BY price DESC \
        LIMIT 10 \
        OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(args.len(), 4);
    }

    #[test]
    fn test_query_builder_new_append_joins() {
        let query =
            "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc&limit=10&offset=0";

        let parsed = UrlQuery::new(
            query,
            &HashSet::from(["userId", "userName", "orderId", "price"]),
        )
        .unwrap();

        let (sql, args) =
            QueryBuilder::new("orders", vec!["id", "status"], parsed, Database::Postgres)
                .append("JOIN users ON users.id = order.user_id")
                .append("JOIN inventory ON inventory.id = order.inventory_id")
                .build();

        let expected = "SELECT id, status FROM orders \
        JOIN users ON users.id = order.user_id \
        JOIN inventory ON inventory.id = order.inventory_id \
        WHERE user_id = $1 AND user_name = $2 \
        AND order_id = $3 AND price >= $4 \
        ORDER BY price DESC \
        LIMIT 10 \
        OFFSET 0";

        assert_eq!(sql, expected);
        assert_eq!(args.len(), 4);
    }

    #[test]
    fn test_query_builder_new_map_columns() {
        let query = "id=1&group=id&sort=createdAt-desc";

        let parsed = UrlQuery::new(query, &HashSet::from(["id", "createdAt"])).unwrap();

        let (sql, args) = QueryBuilder::from_str(
            "SELECT orders.id, user_id, status, address_id, orders.created_at FROM orders",
            parsed,
            Database::Postgres,
        )
        .append("JOIN order_items ON orders.id = order_items.order_id")
        .append("JOIN inventory ON order_items.inventory_id = inventory.id")
        .map_columns(HashMap::from([("id", "orders"), ("createdAt", "orders")]))
        .build();

        let expected =
            "SELECT orders.id, user_id, status, address_id, orders.created_at FROM orders \
             JOIN order_items ON orders.id = order_items.order_id \
             JOIN inventory ON order_items.inventory_id = inventory.id \
             WHERE orders.id = $1 GROUP BY orders.id ORDER BY orders.created_at DESC";

        assert_eq!(sql, expected);
        assert_eq!(args.len(), 1);
    }

    #[test]
    fn test_append_where() {
        let filters = vec![
            Filter {
                field: "userId".into(),
                condition: Condition::EQ,
                value: "1".into(),
            },
            Filter {
                field: "id".into(),
                condition: Condition::EQ,
                value: "829a202f-0e2a-4e8e-9947-938594f9ff26".into(),
            },
        ];

        let mut args =
            super::append_where(&mut String::new(), &filters, &HashMap::default()).into_iter();

        let user_id = args.next().unwrap().1;
        assert_eq!(user_id, "1");

        let id = args.next().unwrap().1;
        assert_eq!(id, "829a202f-0e2a-4e8e-9947-938594f9ff26");
    }
}
