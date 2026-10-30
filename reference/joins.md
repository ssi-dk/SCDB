# SQL Joins

Overloads the dplyr `*_join` to accept an `na_by` argument. By default,
joining using SQL does not match on `NA` / `NULL`. dbplyr `*_join`s has
the option "na_matches = na" to match on `NA` / `NULL` but this is very
inefficient in some cases. This function does the matching more
efficiently: If a column contains `NA` / `NULL`, the names of these
columns can be passed via the `na_by` argument and efficiently match as
if "na_matches = na". If no `na_by` argument is given is given, the
function defaults to using `dplyr::*_join`.

## Usage

``` r
# S3 method for class 'tbl_sql'
inner_join(x, y, by = NULL, ...)

# S3 method for class 'tbl_sql'
left_join(x, y, by = NULL, ...)

# S3 method for class 'tbl_sql'
right_join(x, y, by = NULL, ...)

# S3 method for class 'tbl_sql'
full_join(x, y, by = NULL, ...)

# S3 method for class 'tbl_sql'
semi_join(x, y, by = NULL, ...)

# S3 method for class 'tbl_sql'
anti_join(x, y, by = NULL, ...)
```

## Arguments

- x, y:

  A pair of lazy data frames backed by database queries.

- by:

  A join specification created with
  [`join_by()`](https://dplyr.tidyverse.org/reference/join_by.html), or
  a character vector of variables to join by.

  If `NULL`, the default, `*_join()` will perform a natural join, using
  all variables in common across `x` and `y`. A message lists the
  variables so that you can check they're correct; suppress the message
  by supplying `by` explicitly.

  To join on different variables between `x` and `y`, use a
  [`join_by()`](https://dplyr.tidyverse.org/reference/join_by.html)
  specification. For example, `join_by(a == b)` will match `x$a` to
  `y$b`.

  To join by multiple variables, use a
  [`join_by()`](https://dplyr.tidyverse.org/reference/join_by.html)
  specification with multiple expressions. For example,
  `join_by(a == b, c == d)` will match `x$a` to `y$b` and `x$c` to
  `y$d`. If the column names are the same between `x` and `y`, you can
  shorten this by listing only the variable names, like `join_by(a, c)`.

  [`join_by()`](https://dplyr.tidyverse.org/reference/join_by.html) can
  also be used to perform inequality, rolling, and overlap joins. See
  the documentation at
  [?join_by](https://dplyr.tidyverse.org/reference/join_by.html) for
  details on these types of joins.

  For simple equality joins, you can alternatively specify a character
  vector of variable names to join by. For example, `by = c("a", "b")`
  joins `x$a` to `y$a` and `x$b` to `y$b`. If variable names differ
  between `x` and `y`, use a named character vector like
  `by = c("x_a" = "y_a", "x_b" = "y_b")`.

  To perform a cross-join, generating all combinations of `x` and `y`,
  see
  [`cross_join()`](https://dplyr.tidyverse.org/reference/cross_join.html).

- ...:

  Other parameters passed onto methods.

## Value

Another `tbl_lazy`. Use
[`show_query()`](https://dplyr.tidyverse.org/reference/explain.html) to
see the generated query, and use
[`collect()`](https://dbplyr.tidyverse.org/reference/collect.tbl_sql.html)
to execute the query and return data to R.

## See also

[dplyr::mutate-joins](https://dplyr.tidyverse.org/reference/mutate-joins.html)
which this function wraps.

[dbplyr::join.tbl_sql](https://dbplyr.tidyverse.org/reference/join.tbl_sql.html)
which this function wraps.

[dplyr::show_query](https://dplyr.tidyverse.org/reference/explain.html)

## Examples

``` r
  library(dplyr, warn.conflicts = FALSE)
  library(dbplyr, warn.conflicts = FALSE)

  band_db <- tbl_memdb(dplyr::band_members)
#> Error in db_copy_to(dest$con, name, df, overwrite = overwrite, types = types,     temporary = temporary, unique_indexes = unique_indexes, indexes = indexes,     analyze = analyze, in_transaction = in_transaction, ...): Can't copy data to table `dplyr::band_members`.
#> Caused by error in `dbplyr_write_table()`:
#> ! Can't write table `dplyr::band_members`.
#> Caused by error:
#> ! Table `dplyr::band_members` exists in database, and both overwrite and append are FALSE
  instrument_db <- tbl_memdb(dplyr::band_instruments)
#> Error in db_copy_to(dest$con, name, df, overwrite = overwrite, types = types,     temporary = temporary, unique_indexes = unique_indexes, indexes = indexes,     analyze = analyze, in_transaction = in_transaction, ...): Can't copy data to table `dplyr::band_instruments`.
#> Caused by error in `dbplyr_write_table()`:
#> ! Can't write table `dplyr::band_instruments`.
#> Caused by error:
#> ! Table `dplyr::band_instruments` exists in database, and both overwrite and append are FALSE

  left_join(band_db, instrument_db) |>
    show_query()
#> Error: object 'band_db' not found

  # Can join with local data frames by setting copy = TRUE
  left_join(band_db, dplyr::band_instruments, copy = TRUE)
#> Error: object 'band_db' not found

  # Unlike R, joins in SQL don't usually match NAs (NULLs)
  db <- memdb_frame(x = c(1, 2, NA))
  label <- memdb_frame(x = c(1, NA), label = c("one", "missing"))
  left_join(db, label, by = "x")
#> # A query:  ?? x 2
#> # Database: sqlite 3.53.3 [:memory:]
#>       x label
#>   <dbl> <chr>
#> 1     1 one  
#> 2     2 NA   
#> 3    NA NA   

  # But you can activate R's usual behaviour with the na_matches argument
  left_join(db, label, by = "x", na_matches = "na")
#> # A query:  ?? x 2
#> # Database: sqlite 3.53.3 [:memory:]
#>       x label  
#>   <dbl> <chr>  
#> 1     1 one    
#> 2     2 NA     
#> 3    NA missing

  # By default, joins are equijoins, but you can use `sql_on` to
  # express richer relationships
  db1 <- memdb_frame(x = 1:5)
  db2 <- memdb_frame(x = 1:3, y = letters[1:3])

  left_join(db1, db2) |> show_query()
#> Joining with `by = join_by(x)`
#> <SQL>
#> SELECT `dbplyr_tmp_WOj6CFIknj`.`x` AS `x`, `y`
#> FROM `dbplyr_tmp_WOj6CFIknj`
#> LEFT JOIN `dbplyr_tmp_syZIWa3Zvm`
#>   ON (`dbplyr_tmp_WOj6CFIknj`.`x` = `dbplyr_tmp_syZIWa3Zvm`.`x`)
  left_join(db1, db2, sql_on = "LHS.x < RHS.x") |> show_query()
#> <SQL>
#> SELECT `LHS`.`x` AS `x.x`, `RHS`.`x` AS `x.y`, `y`
#> FROM `dbplyr_tmp_WOj6CFIknj` AS `LHS`
#> LEFT JOIN `dbplyr_tmp_syZIWa3Zvm` AS `RHS`
#>   ON (LHS.x < RHS.x)
```
