# query

Query is a library which can parse and validate query parameters. You can then used the parsed parameters 
to generate a WHERE clause for your database query.

## Example

```rust
use query::{sql::QueryBuilder, sqlx_bind, UrlQuery};

let query = "userId=123&userName=bob&filter[]=orderId-eq-1&filter[]=price-ge-200&sort=price-desc&limit=10&offset=0";

let allowed = ["userId", "userName", "orderId", "price"];

// This will return an error if it couldn't parse a parameter, eg filter[]=orderId-zz-1, or if one
// of the query parameters weren't included in the allowed.
let parsed = UrlQuery::new(query, allowed).unwrap();

// You can require certain fields:
parsed.check_required(["userId"]).unwrap();

// You can check if limit and offset are included:
let (limit, offset) = parsed.check_limit_and_offset().unwrap();

// This returns the complete SQL query along with the args to bind:
let (sql, args) = QueryBuilder::from_str("SELECT * FROM orders", parsed)
    .convert_case(Case::Snake)
    .build();

let expected = "SELECT * FROM orders \
    WHERE user_id = $1 AND user_name = $2 \
    AND order_id = $3 AND price >= $4 \
    ORDER BY price DESC \
    LIMIT 10 \
    OFFSET 0";

assert_eq!(sql, expected);

let mut query = sqlx::query_as(&sql);

// This macro binds args to the query. You need to pass it an error to map to since it uses ? 
// inside when converting the types. You should include every field from the allowed array.
sqlx_bind!(
    args => query,
    error: Either::Right(ParseError),
    "userId" => i64,
    "orderId" => Uuid,
    "username" => String,
    "price" => i32,
);

let result: Vec<Order> = query.fetch_all(pool).await.map_err(|e| Either::Left(e))?;
```
