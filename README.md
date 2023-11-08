# `bqb`

A query builder for BigQuery.

## Why use this?

The most common way to write a BigQuery query is to combine a query string with a `[]bigquery.QueryParameter` slice which contains the query parameter values. But that has some big downsides:

- `[]bigquery.QueryParameter` gets out of sync with the parameters in the query string. For example, you might have a query param in your query string that isn't in your `[]bigquery.QueryParameter`.
- No type checking for `[]bigquery.QueryParameter` values.
- Without a query builder, dynamic queries require error-prone string manipulation.

## Examples

### Basic

To create this query:

```sql
SELECT id, name, age,
FROM my_table
WHERE age > 50
  AND name = 'bob'
```

Write this code:

```go
query, err := Build(
    &client,
    Query(
        Select("id", "name", "age"),
        From("my_table"),
        Where("age > 50"),
        Where("name = 'bob'"),
    ),
)
```

### Parameterized queries

BigQuery supports parameterized queries. Parameter names have an `@` prefix in the query (e.g. `@age`) and must have matching `bigquery.QueryParameter` object.

To create this query:

```sql
SELECT id, name, age,
FROM my_table
WHERE age > @age
  AND name = @name
```

Write this code:

```go
query, err := Build(
    &client,
    Query(
        Select("id", "name", "age"),
        From("my_table"),
        Where("age > @age"),
        Where("name = @name"),
    ),
    ParamInt("age", 50),
    ParamStr("name", "bob"),
)
```

The `Build` function will automatically populate `query.Parameters` and check that all parameters in the query have been specified (e.g. using `ParamInt`).
