# LoggerNull: The no-logging Logger

The `LoggerNull` class overwrites the functions of the `Logger` so no
logging is produced. Errors and warnings are still produced.

## Value

A new instance of the `LoggerNull`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Super class

[`Logger`](https://ssi-dk.github.io/SCDB/reference/Logger.md) -\>
`LoggerNull`

## Methods

### Public methods

- [`LoggerNull$new()`](#method-LoggerNull-initialize)

- [`LoggerNull$log_to_db()`](#method-LoggerNull-log_to_db)

- [`LoggerNull$finalize_db_entry()`](#method-LoggerNull-finalize_db_entry)

- [`LoggerNull$clone()`](#method-LoggerNull-clone)

Inherited methods

- [`Logger$log_error()`](https://ssi-dk.github.io/SCDB/reference/Logger.html#method-log_error)
- [`Logger$log_info()`](https://ssi-dk.github.io/SCDB/reference/Logger.html#method-log_info)
- [`Logger$log_warn()`](https://ssi-dk.github.io/SCDB/reference/Logger.html#method-log_warn)
- [`Logger$set_timestamp()`](https://ssi-dk.github.io/SCDB/reference/Logger.html#method-set_timestamp)

------------------------------------------------------------------------

### `LoggerNull$new()`

Create a new `LoggerNull` object

#### Usage

    LoggerNull$new(...)

#### Arguments

- `...`:

  Captures arguments given, but does nothing

------------------------------------------------------------------------

### `LoggerNull$log_to_db()`

Matches the signature of `Logger$log_to_db()`, but does nothing.

#### Usage

    LoggerNull$log_to_db(...)

#### Arguments

- `...`:

  Captures arguments given, but does nothing

------------------------------------------------------------------------

### `LoggerNull$finalize_db_entry()`

Matches the signature of `Logger$finalize_db_entry()`, but does nothing.

#### Usage

    LoggerNull$finalize_db_entry(...)

#### Arguments

- `...`:

  Captures arguments given, but does nothing

------------------------------------------------------------------------

### `LoggerNull$clone()`

The objects of this class are cloneable with this method.

#### Usage

    LoggerNull$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  logger <- LoggerNull$new()

  logger$log_info("This message will not print!")
  logger$log_to_db(message = "This message will no be written in database!")
  try(logger$log_warn("This is a warning!"))
#> Warning: 2026-10-31 00:32:07.495 - rstudio - WARNING - This is a warning!
  try(logger$log_error("This is an error!"))
#> Error : 2026-10-31 00:32:07.496 - rstudio - ERROR - This is an error!
```
