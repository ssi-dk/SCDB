# Generate sql_on statements for na joins

This function generates a much faster SQL statement for NA join compared
to dbplyr's \_join with na_matches = "na".

## Usage

``` r
join_na_sql(x, by, na_by)

# S3 method for class 'tbl_dbi'
join_na_sql(x, by, na_by)

# S3 method for class '`tbl_Microsoft SQL Server`'
join_na_sql(x, by, na_by)
```

## Arguments

- x:

  (`lazy query`)\
  The object to generate SQL for.

- by:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Columns for which NA values should not match (default join behavior).

- na_by:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Columns for which NA values should match.

## Value

A sql_on statement to join by such that "NA" are matched with "NA" given
the columns listed in "by" and "na_by".

## Examples

``` r
  table <- dbplyr::tbl_lazy(mtcars, con = dbplyr::simulate_mssql())
  join_na_sql(table, by = "cyl", na_by = NULL)
#> [1] "\"LHS\".\"cyl\" = \"RHS\".\"cyl\""
  join_na_sql(table, by = NULL, na_by = "cyl")
#> [1] "(\"LHS\".\"cyl\" IS NULL AND \"RHS\".\"cyl\" IS NULL OR \"LHS\".\"cyl\" = \"RHS\".\"cyl\")"
```
