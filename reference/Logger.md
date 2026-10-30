# Logger: Complete logging to console, file and database

The `Logger` class facilitates logging to a database and/or file and to
console.

A `Logger` is associated with a specific table and timestamp which must
be supplied at initialization. This information is used to create the
log file (if a `log_path` is given) and the log entry in the database
(if a `log_table_id` and `log_conn` is given).

Logging to the database must match the fields in the log table.

## Value

A new instance of the `Logger`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Active bindings

- `output_to_console`:

  (`logical(1)`)\
  Should the Logger output to console? Read only. This can always be
  overridden by Logger\$log_info(..., output_to_console = FALSE).

- `log_path`:

  (`character(1)`)\
  The location log files are written (if this is not NULL). Defaults to
  `getOption("SCDB.log_path")`. Read only.

- `log_tbl`:

  (`tbl_dbi(1)`)\
  The database table used for logging. Class is connection-specific, but
  inherits from `tbl_dbi`. Read only.

- `start_time`:

  (`POSIXct(1)`)\
  The time at which data processing was started. Read only.

- `log_filename`:

  (`character(1)`)\
  The filename (basename) of the file that the `Logger` instance will
  output to. Read only.

- `log_realpath`:

  (`character(1)`)\
  The full path to the logger's log file. Read only.

## Methods

### Public methods

- [`Logger$new()`](#method-Logger-initialize)

- [`Logger$set_timestamp()`](#method-Logger-set_timestamp)

- [`Logger$log_info()`](#method-Logger-log_info)

- [`Logger$log_warn()`](#method-Logger-log_warn)

- [`Logger$log_error()`](#method-Logger-log_error)

- [`Logger$log_to_db()`](#method-Logger-log_to_db)

- [`Logger$finalize_db_entry()`](#method-Logger-finalize_db_entry)

- [`Logger$clone()`](#method-Logger-clone)

------------------------------------------------------------------------

### `Logger$new()`

Create a new `Logger` object

#### Usage

    Logger$new(
      db_table = NULL,
      timestamp = NULL,
      output_to_console = TRUE,
      log_table_id = getOption("SCDB.log_table_id"),
      log_conn = NULL,
      log_path = getOption("SCDB.log_path"),
      start_time = Sys.time(),
      warn = TRUE
    )

#### Arguments

- `db_table`:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)) specifying
  the table being updated.

- `timestamp`:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  A timestamp describing the data being processed (not the current
  time).

- `output_to_console`:

  (`logical(1)`)\
  Should the Logger output to console?

- `log_table_id`:

  (`id-like object(1)`)\
  A table specification (coercible by
  [`id()`](https://ssi-dk.github.io/SCDB/reference/id.md)) specifying
  the location of the log table.

- `log_conn`:

  (`DBIConnection(1)`)\
  A database connection where log table should exist.

- `log_path`:

  (`character(1)`)\
  The path where logs are stored. If `NULL`, no file logs are created.

- `start_time`:

  (`POSIXct(1)`)\
  The time at which data processing was started (defaults to
  [`Sys.time()`](https://rdrr.io/r/base/Sys.time.html)).

- `warn`:

  (`logical(1)`)\
  Should a warning be produced if no logging will be done?

------------------------------------------------------------------------

### `Logger$set_timestamp()`

Update the timestamp being logged

#### Usage

    Logger$set_timestamp(timestamp)

#### Arguments

- `timestamp`:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  A timestamp describing the data being processed (not the current
  time).

------------------------------------------------------------------------

### `Logger$log_info()`

Write a line to log (console / file).

#### Usage

    Logger$log_info(
      ...,
      tic = Sys.time(),
      output_to_console = self$output_to_console,
      log_type = "INFO",
      timestamp_format = getOption("SCDB.log_timestamp_format", "%F %R:%OS3")
    )

#### Arguments

- `...`:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Character strings to be concatenated as log message.

- `tic`:

  (`POSIXct(1)`)\
  The timestamp used by the log entry.

- `output_to_console`:

  (`logical(1)`)\
  Should the line be written to console?

- `log_type`:

  (`character(1)`)\
  The severity of the log message.

- `timestamp_format`:

  (`character(1)`)\
  The format of the timestamp used in the log message (parsable by
  [`strftime()`](https://rdrr.io/r/base/strptime.html)).

#### Returns

Returns the log message invisibly

------------------------------------------------------------------------

### `Logger$log_warn()`

Write a warning to log file and generate warning.

#### Usage

    Logger$log_warn(..., log_type = "WARNING")

#### Arguments

- `...`:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Character strings to be concatenated as log message.

- `log_type`:

  (`character(1)`)\
  The severity of the log message.

------------------------------------------------------------------------

### `Logger$log_error()`

Write an error to log file and stop execution.

#### Usage

    Logger$log_error(..., log_type = "ERROR")

#### Arguments

- `...`:

  ([`character()`](https://rdrr.io/r/base/character.html))\
  Character strings to be concatenated as log message.

- `log_type`:

  (`character(1)`)\
  The severity of the log message.

------------------------------------------------------------------------

### `Logger$log_to_db()`

Write or update log table.

#### Usage

    Logger$log_to_db(...)

#### Arguments

- `...`:

  (`Name-value pairs`)\
  Structured data written to database log table. Name indicates column
  and value indicates value to be written.

------------------------------------------------------------------------

### `Logger$finalize_db_entry()`

Auto-fills "end_time" and "duration" for the log entry and clears the
"log_file" field if no file is being written.

#### Usage

    Logger$finalize_db_entry(end_time = Sys.time())

#### Arguments

- `end_time`:

  (`POSIXct(1)`, `Date(1)`, or `character(1)`)\
  The end time for the log entry.

------------------------------------------------------------------------

### `Logger$clone()`

The objects of this class are cloneable with this method.

#### Usage

    Logger$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  logger <- Logger$new(
    db_table = "test.table",
    timestamp = "2020-01-01 09:00:00"
  )
#> Warning: `log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.
#> Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile

  logger$log_info("This is an info message")
#> 2026-10-30 23:28:20.342 - rstudio - INFO - This is an info message
  logger$log_to_db(message = "This is a message")

  try(logger$log_warn("This is a warning!"))
#> Warning: 2026-10-30 23:28:20.343 - rstudio - WARNING - This is a warning!
  try(logger$log_error("This is an error!"))
#> Error : 2026-10-30 23:28:20.344 - rstudio - ERROR - This is an error!
```
