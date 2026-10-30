# Create a name for a temporary table

This function is heavily inspired by the unexported dbplyr function
unique_table_name

## Usage

``` r
unique_table_name(scope = "SCDB")
```

## Arguments

- scope:

  (`character(1)`)\
  A naming scope to generate the table name within.

## Value

A character string for a table name based on the given scope parameter

## Examples

``` r
  print(unique_table_name()) # SCDB_<10 alphanumerical letters>
#> SCDB_Z6V3rqsxvy
  print(unique_table_name()) # SCDB_<10 alphanumerical letters>
#> SCDB_zieCv7vGEg

  print(unique_table_name("test")) # test_<10 alphanumerical letters>
#> test_hBs6ES5zDj
  print(unique_table_name("test")) # test_<10 alphanumerical letters>
#> test_QPkYxdaMGd
```
