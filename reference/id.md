# Convenience function for DBI::Id

Convenience function for DBI::Id

## Usage

``` r
id(db_table, ...)

# S3 method for class 'Id'
id(db_table, conn = NULL, ...)

# S3 method for class 'character'
id(db_table, conn = NULL, allow_table_only = TRUE, ...)

# S3 method for class 'data.frame'
id(db_table, ...)
```

## Arguments

- db_table:

  (`id-like object(1)`)\
  A table specification (coercible by `id()`).

- ...:

  Further arguments passed to methods.

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- allow_table_only:

  (`logical(1)`)\
  If `TRUE`, allows for returning an
  [`DBI::Id`](https://dbi.r-dbi.org/reference/Id.html) with `table` =
  "myschema.table" if schema "myschema" is not found in `conn`. If
  `FALSE`, the function will raise an error if the implied schema cannot
  be found in `conn`.

## Value

A [`DBI::Id`](https://dbi.r-dbi.org/reference/Id.html) object parsed
from db_table (see details).

## Details

The given `db_table` is parsed to a DBI::Id depending on the type of
input:

- `character`: db_table is parsed to a DBI::Id object using an
  assumption of "schema.table" syntax with corresponding schema (if
  found in `conn`) and table values. If no schema is implied, the
  default schema of `conn` will be used.

- [`DBI::Id`](https://dbi.r-dbi.org/reference/Id.html): if schema is not
  specified in `Id`, the schema is set to the default schema for `conn`
  (if given).

- `tbl_sql`: the remote name is used to resolve the table
  identification.

- `data.frame`: A Id is built from the data.frame (columns `catalog`,
  `schema`, and `table`). Can be used in conjunction with
  `get_tables(conn, pattern)`.

## See also

[DBI::Id](https://dbi.r-dbi.org/reference/Id.html) which this function
wraps.

## Examples

``` r
  id("schema.table")
#> <Id> "schema"."table"
```
