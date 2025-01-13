#' Logger: Complete logging to console, file and database
#'
#' @description
#'   The `Logger` class facilitates logging to a database and/or file and to console.
#'
#'   A `Logger` is associated with a specific table and timestamp which must be supplied at initialization.
#'   This information is used to create the log file (if a `log_path` is given) and the log entry in the database
#'   (if a `log_table_id` and `log_conn` is given).
#'
#'   Logging to the database must match the fields in the log table.
#' @return
#'   A new instance of the `Logger` [R6][R6::R6Class] class.
#' @examples
#'   logger <- Logger$new(
#'     db_table = "test.table",
#'     timestamp = "2020-01-01 09:00:00"
#'   )
#'
#'   logger$log_info("This is an info message")
#'   logger$log_to_db(message = "This is a message")
#'
#'   try(logger$log_warn("This is a warning!"))
#'   try(logger$log_error("This is an error!"))
#' @export
#' @importFrom R6 R6Class
Logger <- R6::R6Class(                                                                                                  # nolint: object_name_linter
  classname  = "Logger",

  public = list(

    #' @description
    #'   Create a new `Logger` object
    #' @param db_table (`id-like object(1)`)\cr
    #'   A table specification (coercible by `id()`) specifying the table being updated.
    #' @param timestamp (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
    #'   A timestamp describing the data being processed (not the current time).
    #' @param output_to_console (`logical(1)`)\cr
    #'   Should the Logger output to console?
    #' @param log_table_id (`id-like object(1)`)\cr
    #'   A table specification (coercible by `id()`) specifying the location of the log table.
    #' @param log_conn (`DBIConnection(1)`)\cr
    #'   A database connection where log table should exist.
    #' @param log_path (`character(1)`)\cr
    #'   The path where logs are stored.
    #'   If `NULL`, no file logs are created.
    #' @param warn (`logical(1)`)\cr
    #'   Should a warning be produced if no logging will be done?
    #' @param start_time (`POSIXct(1)`)\cr
    #'   The time at which data processing was started (defaults to [Sys.time()]).
    initialize = function(
      # Data specification
      db_table = NULL,
      timestamp = NULL,
      # Console
      output_to_console = TRUE,
      # database
      log_table_id = getOption("SCDB.log_table_id"),
      log_conn = NULL,
      # File
      log_path = getOption("SCDB.log_path"),
      # Logger
      start_time = Sys.time(),
      warn = TRUE
    ) {

      # Initialize logger
      coll <- checkmate::makeAssertCollection()
      assert_id_like(db_table, null.ok = TRUE, add = coll)
      assert_timestamp_like(timestamp, null.ok = TRUE, add = coll)
      checkmate::assert_logical(output_to_console, add = coll)
      assert_id_like(log_table_id, null.ok = TRUE, add = coll)
      checkmate::assert_class(log_conn, "DBIConnection", null.ok = is.null(log_table_id), add = coll)
      checkmate::assert_character(log_path, null.ok = TRUE, add = coll)
      checkmate::assert_posixct(start_time, add = coll)
      checkmate::assert_logical(warn, add = coll)
      checkmate::reportAssertions(coll)

      # Store data specification
      if (!is.null(db_table)) {
        private$db_table <- id(db_table, log_conn)
      }
      private$timestamp <- timestamp

      # Store console log information
      private$.output_to_console <- output_to_console

      # Store database log information
      if (!is.null(log_table_id)) {
        private$log_table_id <- id(log_table_id, log_conn)
        private$.log_tbl <- create_logs_if_missing(log_conn, private$log_table_id)
      }
      private$log_conn <- log_conn

      # Store file log information
      private$.log_path <- log_path

      # Store Logger information
      private$.start_time <- start_time

      # Create a line in log database for Logger
      private$generate_db_entry()

      # Warn if no logging will be done
      if (warn && is.null(self$log_path) && is.null(self$log_tbl)) {
        warning(
          "`log_path` and `log_tbl` are both `NULL` and therefore NO file or database logging will be done.\n",
          "Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile"
        )
      }
    },

    #' @description
    #'   Remove generated `log_name` from database if not writing to a file.
    finalize = function() {
      self$finalize_db_entry()
    },

    #' @description
    #'   Write a line to log (console / file).
    #' @param ... (`character()`)\cr
    #'   Character strings to be concatenated as log message.
    #' @param tic (`POSIXct(1)`)\cr
    #'   The timestamp used by the log entry.
    #' @param output_to_console (`logical(1)`)\cr
    #'   Should the line be written to console?
    #' @param log_type (`character(1)`)\cr
    #'   The severity of the log message.
    #' @param timestamp_format (`character(1)`)\cr
    #'   The format of the timestamp used in the log message (parsable by [strftime()]).
    #' @return
    #'   Returns the log message invisibly
    log_info = function(..., tic = Sys.time(), output_to_console = self$output_to_console, log_type = "INFO",
                        timestamp_format = getOption("SCDB.log_timestamp_format", "%F %R:%OS3")) {

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_logical(output_to_console, add = coll)
      checkmate::assert_character(log_type, add = coll)
      checkmate::assert_character(timestamp_format, add = coll)
      tryCatch(format(Sys.time(), timestamp_format), error = function(e) coll$push("timestamp_format not valid!"))
      checkmate::reportAssertions(coll)

      msg <- private$log_format(..., tic = tic, log_type = log_type, timestamp_format = timestamp_format)

      # Write message to console and file
      if (isTRUE(output_to_console) && identical(log_type, "INFO")) {
        message(msg)
      }

      # Write message to file
      if (!is.null(self$log_path)) {
        write(msg, self$log_realpath, append = TRUE)
      }

      return(invisible(msg))
    },

    #' @description
    #'   Write a warning to log file and generate warning.
    #' @param ... (`character()`)\cr
    #'   Character strings to be concatenated as log message.
    #' @param log_type (`character(1)`)\cr
    #'   The severity of the log message.
    log_warn = function(..., log_type = "WARNING") {
      warning(self$log_info(..., log_type = log_type))
    },

    #' @description
    #'   Write an error to log file and stop execution.
    #' @param ... (`character()`)\cr
    #'   Character strings to be concatenated as log message.
    #' @param log_type (`character(1)`)\cr
    #'   The severity of the log message.
    log_error = function(..., log_type = "ERROR") {
      stop(self$log_info(..., log_type = log_type))
    },

    #' @description
    #'   Write or update log table.
    #' @param ... (`Name-value pairs`)\cr
    #'   Structured data written to database log table. Name indicates column and value indicates value to be written.
    log_to_db = function(...) {

      # If log is finalized, throw an error to the user
      if (private$finalized) {
        stop("Logger has already been finalized. Cannot write to database log table.")
      }

      # Only write if we have a valid connection
      if (!is.null(private$log_conn) && DBI::dbIsValid(private$log_conn) && !is.null(self$log_tbl) &&
            table_exists(private$log_conn, private$log_table_id)) {

        patch <- data.frame(log_file = self$log_filename)
        patch <- dplyr::copy_to(
          dest = private$log_conn,
          df = patch,
          name = unique_table_name(),
          temporary = TRUE
        )
        defer_db_cleanup(patch) # Clean up on exit

        # Mutating after copying ensures consistency in SQL translation
        dplyr::rows_patch(
          x = self$log_tbl,
          y = dplyr::mutate(patch, ...),
          by = "log_file",
          in_place = TRUE,
          unmatched = "ignore"
        )
      }
    },


    #' @description
    #'   Auto-fills "end_time" and "duration" for the log entry and clears the "log_file" field if no
    #'   file is being written.
    #' @param end_time (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
    #'   The end time for the log entry.
    finalize_db_entry = function(end_time = Sys.time()) {
      if (private$finalized) {
        return(NULL)
      }

      assert_timestamp_like(self$start_time)
      assert_timestamp_like(end_time)

      # Auto-fill log with end time and duration
      self$log_to_db(
        end_time = !!db_timestamp(end_time, private$log_conn),
        duration = !!format(round(difftime(as.POSIXct(end_time), as.POSIXct(self$start_time)), digits = 2)),
        success = TRUE
      )


      # Remove the log_file from the log table if no actual file is being written
      if (is.null(self$log_path) && !is.null(private$log_conn) && DBI::dbIsValid(private$log_conn) &&
            !is.null(self$log_tbl) && table_exists(private$log_conn, private$log_table_id)) {

        expected_rows <- self$log_tbl %>%
          dplyr::filter(log_file == !!self$log_filename) %>%
          dplyr::count() %>%
          dplyr::pull()

        query <- dbplyr::build_sql(
          "UPDATE ",
          dbplyr::as.sql(id(self$log_tbl, conn = private$log_conn), con = private$log_conn),
          " SET ",
          dbplyr::ident("log_file"),
          " = NULL WHERE ",
          dbplyr::ident("log_file"),
          " = '",
          dplyr::sql(self$log_filename),
          "'",
          con = private$log_conn
        )

        affected_rows <- DBI::dbExecute(private$log_conn, query)
        if (affected_rows != expected_rows) {
          rlang::warn("Something went wrong while finalizing Logger",
                      log_filename = self$log_filename,
                      affected_rows = affected_rows,
                      expected_rows = expected_rows)
        } else {
          private$finalized <- TRUE
        }
      }
    }
  ),

  private = list(
    # @field output_to_console (`logical(1)`)\cr
    #   Should the Logger output to console?
    #   This can always be overridden by Logger$log_info(..., output_to_console = FALSE).
    .output_to_console = NULL,

    # @field log_path (`character(1)`)\cr
    #   The location log files are written (if this is not NULL). Defaults to `getOption("SCDB.log_path")`.
    .log_path = NULL,

    # @field log_tbl (`tbl_dbi(1)`)\cr
    #   The database table used for logging. Class is connection-specific, but inherits from `tbl_dbi`.
    .log_tbl = NULL,

    # @field log_table_id (`Id(1)`)\cr
    #   The Id of the database table used for logging.
    log_table_id = NULL,

    # @field log_conn (`DBIConnection(1)`)\cr
    #   A database connection where log table should exist.
    log_conn = NULL,

    # @field start_time (`POSIXct(1)`)\cr
    #   The time at which data processing was started.
    .start_time = NULL,

    # @field log_filename `character(1)`\cr
    #   The filename (basename) of the file that the `Logger` instance will output to.
    .log_filename = NULL,

    # @field db_table (`Id(1)`)\cr
    #   A table specification specifying the table being updated.
    db_table = NULL,

    # @field timestamp (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
    #   A timestamp describing the data being processed (not the current time)
    timestamp = NULL,

    # @field finalized (`logical(1)`)\cr
    #   Has the log entry been finalized?
    finalized = FALSE,

    # Format the log message
    # @inheritParams Logger$log_info()
    log_format = function(..., tic = Sys.time(), log_type = NULL, timestamp_format = NULL) {
      checkmate::assert_character(timestamp_format)
      checkmate::assert_posixct(tic)

      return(paste(format(tic, timestamp_format), Sys.info()[["user"]], log_type, paste(...), sep = " - "))
    },

    # Create a row for log in question
    generate_db_entry = function() {

      if (is.null(self$log_tbl)) {
        return(NULL)
      }

      coll <- checkmate::makeAssertCollection()
      assert_timestamp_like(private$timestamp, add = coll)
      assert_id_like(private$db_table, add = coll)
      assert_timestamp_like(self$start_time, add = coll)
      checkmate::reportAssertions(coll)

      patch <- data.frame(
        log_file = self$log_filename,
        schema = purrr::pluck(private$db_table, "name", "schema"),
        table = purrr::pluck(private$db_table, "name", "table")
      )

      # Add catalog if it exists in the Id
      if ("catalog" %in% purrr::pluck(private$db_table, "name", names)) {
        patch <- dplyr::mutate(patch, catalog = purrr::pluck(private$db_table, "name", "catalog"), .before = "schema")
      }

      patch <- dplyr::copy_to(
        dest = private$log_conn,
        df = patch,
        name = unique_table_name(),
        temporary = TRUE
      )
      defer_db_cleanup(patch) # Clean up on exit

      dplyr::rows_append(
        x = self$log_tbl,
        y = dplyr::mutate(
          patch,
          date = !!db_timestamp(private$timestamp, private$log_conn),
          start_time = !!db_timestamp(self$start_time, private$log_conn)
        ),
        copy = TRUE,
        in_place = TRUE
      )
    }
  ),

  active = list(
    #' @field output_to_console (`logical(1)`)\cr
    #'   Should the Logger output to console? Read only.
    #'   This can always be overridden by Logger$log_info(..., output_to_console = FALSE).
    output_to_console = function(value) {
      if (missing(value)) {
        return(private$.output_to_console)
      } else {
        stop(glue::glue("`$output_to_console` is read only"), call. = FALSE)
      }
    },

    #' @field log_path (`character(1)`)\cr
    #'   The location log files are written (if this is not NULL). Defaults to `getOption("SCDB.log_path")`. Read only.
    log_path = function(value) {
      if (missing(value)) {
        return(private$.log_path)
      } else {
        stop(glue::glue("`$log_path` is read only"), call. = FALSE)
      }
    },

    #' @field log_tbl (`tbl_dbi(1)`)\cr
    #'   The database table used for logging. Class is connection-specific, but inherits from `tbl_dbi`. Read only.
    log_tbl = function(value) {
      if (missing(value)) {
        return(private$.log_tbl)
      } else {
        stop(glue::glue("`$log_tbl` is read only"), call. = FALSE)
      }
    },

    #' @field start_time (`POSIXct(1)`)\cr
    #'   The time at which data processing was started. Read only.
    start_time = function(value) {
      if (missing(value)) {
        return(private$.start_time)
      } else {
        stop(glue::glue("`$start_time` is read only"), call. = FALSE)
      }
    },

    #' @field log_filename (`character(1)`)\cr
    #'   The filename (basename) of the file that the `Logger` instance will output to.  Read only.
    log_filename = function() {

      # Early return, if log_filename has been generated
      if (!is.null(private$.log_filename)) {
        return(private$.log_filename)
      }

      # If we are not producing a file log, we provide a random string to key by
      if (is.null(self$log_path)) {
        private$.log_filename <- basename(tempfile(tmpdir = "", pattern = ""))
        return(private$.log_filename)
      }

      coll <- checkmate::makeAssertCollection()
      assert_dbtable_like(private$db_table, add = coll)
      assert_timestamp_like(private$timestamp, null.ok = FALSE, add = coll)
      checkmate::reportAssertions(coll)

      start_format <- format(self$start_time, "%Y%m%d.%H%M")
      timestamp <- private$timestamp

      if (is.character(timestamp)) timestamp <- as.Date(timestamp)
      ts_format <- format(timestamp, "%Y_%m_%d")
      filename <- sprintf(
        "%s.%s.%s.log",
        start_format,
        ts_format,
        private$db_table
      )

      if (file.exists(file.path(self$log_path, filename))) {
        stop(sprintf("Log file '%s' already exists!", filename))
      }

      private$.log_filename <- filename
      return(filename)
    },

    #' @field log_realpath (`character(1)`)\cr
    #'   The full path to the logger's log file. Read only.
    log_realpath = function() {
      if (is.null(self$log_path)) {
        return(nullfile())
      } else {
        return(file.path(self$log_path, self$log_filename))
      }
    }
  )
)


#' LoggerNull: The no-logging Logger
#'
#' @description
#'   The `LoggerNull` class overwrites the functions of the `Logger` so no logging is produced.
#'   Errors and warnings are still produced.
#' @return
#'   A new instance of the `LoggerNull` [R6][R6::R6Class] class.
#' @examples
#'   logger <- LoggerNull$new()
#'
#'   logger$log_info("This message will not print!")
#'   logger$log_to_db(message = "This message will no be written in database!")
#'   try(logger$log_warn("This is a warning!"))
#'   try(logger$log_error("This is an error!"))
#' @export
#' @importFrom R6 R6Class
LoggerNull <- R6::R6Class(                                                                                              # nolint: object_name_linter
  classname  = "LoggerNull",
  inherit = Logger,
  public = list(

    #' @description
    #'   Create a new `LoggerNull` object
    #' @param ... Captures arguments given, but does nothing
    initialize = function(...) {
      super$initialize(output_to_console = FALSE, warn = FALSE, log_path = NULL) # Disable all console logging

    },

    #' @description
    #'   Matches the signature of `Logger$log_to_db()`, but does nothing.
    #' @param ... Captures arguments given, but does nothing
    log_to_db = function(...) {
      return(invisible(NULL))
    },

    #' @description
    #'   Matches the signature of `Logger$finalize_db_entry()`, but does nothing.
    #' @param ... Captures arguments given, but does nothing
    finalize_db_entry = function(...) {
      return(invisible(NULL))
    }
  )
)
