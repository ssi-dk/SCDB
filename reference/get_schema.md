# Get the current schema/catalog of a database-related objects

Get the current schema/catalog of a database-related objects

## Usage

``` r
get_catalog(obj, ...)

# S3 method for class '`Microsoft SQL Server`'
get_catalog(obj, temporary = FALSE, ...)

get_schema(obj, ...)

# S3 method for class 'PqConnection'
get_schema(obj, temporary = FALSE, ...)

# S3 method for class 'SQLiteConnection'
get_schema(obj, temporary = FALSE, ...)
```

## Arguments

- obj:

  (`DBIConnection(1)`, `tbl_dbi(1)`, `Id(1)`)\
  The object from which to retrieve a schema/catalog.

- ...:

  Further arguments passed to methods.

- temporary:

  (`logical(1)`)\
  Should the reference be to the temporary schema/catalog?

## Value

The catalog is extracted from `obj` depending on the type of input:

- For `get_catalog.Microsoft SQL Server`, the current database context
  of the connection or "tempdb" if `temporary = TRUE`.

- For `get_schema.tbl_dbi` the catalog is determined via
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md).

- For `get_catalog.\\*`, `NULL` is returned.

The schema is extracted from `obj` depending on the type of input:

- For `get_schema.DBIConnection()`, the current schema of the connection
  if `temporary = FALSE`. See "Default schema" for more. If
  `temporary = TRUE`, the temporary schema of the connection is
  returned.

- For `get_schema.tbl_dbi()` the schema is determined via
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md).

- For `get_schema.Id()`, the schema is extracted from the `Id`
  specification.

## Default schema

In some backends, it is possible to modify settings so that when a
schema is not explicitly stated in a query, the backend searches for the
table in this schema by default. For Postgres databases, this can be
shown with `SELECT CURRENT_SCHEMA()` (defaults to `public`) and modified
with `SET search_path TO { schema }`.

For SQLite databases, a `temp` schema for temporary tables always exists
as well as a `main` schema for permanent tables. Additional databases
may be attached to the connection with a named schema, but as the
attachment must be made after the connection is established,
`get_schema` will never return any of these, as the default schema will
always be `main`.

## Examples

``` r
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)

  get_schema(conn)
#> [1] "main"
  get_schema(get_table(conn, id("mtcars", conn = conn)))
#> [1] "main"

  get_catalog(conn)
#> NULL
  get_catalog(get_table(conn, id("mtcars", conn = conn)))
#> NULL

  close_connection(conn)
```
