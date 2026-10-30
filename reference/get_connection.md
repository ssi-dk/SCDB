# Opens connection to the database

This is a convenience wrapper for DBI::dbConnect() for different
database backends.

Connects to the specified dbname of host:port using user and password
from given arguments (if applicable). Certain drivers may use
credentials stored in a file, such as ~/.pgpass (PostgreSQL).

## Usage

``` r
get_connection(drv, ...)

# S3 method for class 'SQLiteDriver'
get_connection(
  drv,
  dbname = ":memory:",
  ...,
  bigint = c("integer", "bigint64", "numeric", "character")
)

# S3 method for class 'PqDriver'
get_connection(
  drv,
  dbname = NULL,
  host = NULL,
  port = NULL,
  password = NULL,
  user = NULL,
  ...,
  bigint = c("integer", "bigint64", "numeric", "character"),
  check_interrupts = TRUE,
  timezone = Sys.timezone(),
  timezone_out = Sys.timezone()
)

# S3 method for class 'OdbcDriver'
get_connection(
  drv,
  dsn = NULL,
  ...,
  bigint = c("integer", "bigint64", "numeric", "character"),
  timezone = Sys.timezone(),
  timezone_out = Sys.timezone()
)

# S3 method for class 'duckdb_driver'
get_connection(
  drv,
  dbdir = ":memory:",
  ...,
  bigint = c("numeric", "character"),
  timezone_out = Sys.timezone()
)

# Default S3 method
get_connection(drv, ...)
```

## Arguments

- drv:

  (`DBIDriver(1)` or `DBIConnection(1)`)\
  The driver for the connection (defaults to `SQLiteDriver`).

- ...:

  Additional parameters sent to DBI::dbConnect().

- dbname:

  (`character(1)`)\
  Name of the database located at the host.

- bigint:

  (`character(1)`)\
  The datatype to convert integers to. Support depends on the database
  backend.

- host:

  (`character(1)`)\
  The ip of the host to connect to.

- port:

  (`numeric(1)` or `character(1)`)\
  Host port to connect to.

- password:

  (`character(1)`)\
  Password to login with.

- user:

  (`character(1)`)\
  Username to login with.

- check_interrupts:

  (`logical(1)`)\
  Should user interrupts be checked during the query execution?

- timezone:

  (`character(1)`)\
  Sets the timezone of DBI::dbConnect(). Must be in
  [`OlsonNames()`](https://rdrr.io/r/base/timezones.html).

- timezone_out:

  (`character(1)`)\
  Sets the timezone_out of DBI::dbConnect(). Must be in
  [`OlsonNames()`](https://rdrr.io/r/base/timezones.html).

- dsn:

  (`character(1)`)\
  The data source name to connect to.

- dbdir:

  (`character(1)`)\
  The directory where the database is located.

## Value

An object that inherits from `DBIConnection` driver specified in `drv`.

## See also

[RSQLite::SQLite](https://rsqlite.r-dbi.org/reference/SQLite.html)

[RPostgres::Postgres](https://rpostgres.r-dbi.org/reference/Postgres.html)

odbc::odbc

[duckdb::duckdb](https://r.duckdb.org/reference/duckdb.html)

## Examples

``` r
  conn <- get_connection(drv = RSQLite::SQLite(), dbname = ":memory:")

  DBI::dbIsValid(conn) # TRUE
#> [1] TRUE

  close_connection(conn)

  DBI::dbIsValid(conn) # FALSE
#> [1] FALSE
```
