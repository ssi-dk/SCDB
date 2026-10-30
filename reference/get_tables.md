# List the available tables on the connection

List the available tables on the connection

## Usage

``` r
get_tables(conn, pattern = NULL, show_temporary = TRUE)
```

## Arguments

- conn:

  (`DBIConnection(1)`)\
  Connection object.

- pattern:

  (`character(1)`)\
  Regex pattern with which to subset the returned tables.

- show_temporary:

  (`logical(1)`)\
  Should temporary tables be listed?

## Value

A data.frame containing table names including schema (and catalog when
available) in the database.

## Examples

``` r
  conn <- get_connection()

  dplyr::copy_to(conn, mtcars, name = "my_test_table_1", temporary = FALSE)
  dplyr::copy_to(conn, mtcars, name = "my_test_table_2")

  get_tables(conn, pattern = "my_[th]est")
#>   schema           table
#> 1   main my_test_table_1
#> 2   temp my_test_table_2
  get_tables(conn, pattern = "my_[th]est", show_temporary = FALSE)
#>   schema           table
#> 1   main my_test_table_1

  close_connection(conn)
```
