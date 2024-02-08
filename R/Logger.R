#' Logger
#'
#' @description
#'   Create an object for logging database operations.
#'
#' @importFrom R6 R6Class
#' @param db_table (`character(1)`)\cr
#'  A string specifying the table being updated.
#' @param log_table_id (`id-like object`)\cr
#'   A table specification (coercible by `id()`) specifying the location of the log table.
#' @param log_path (`character(1)`)\cr
#'   The path where logs are stored.
#'   If `NULL`, no file logs are created.
#' @param ts A timestamp describing the data being processed (not the current time)
#' @param start_time (`POSIXct(1)`)\cr
#'   The time at which data processing was started (defaults to [Sys.time()]).
#' @return
#'   A new instance of the `Logger` [R6][R6::R6Class] class.
#' @examples
#'   logger <- Logger$new(db_table = "test.table",
#'                        timestamp = "2020-01-01 09:00:00")
#' @export
Logger <- R6::R6Class(                                                                                                  #nolint: object_name_linter
  classname  = "Logger",
  public = list(

    #' @field log_path (`character(1)`)\cr
    #'   A directory where log file is written (if this is not NULL). Defaults to `getOption("SCDB.log_path")`.
    log_path = NULL,

    #' @field log_tbl (`tbl_dbi`)\cr
    #'   The DB table used for logging. Class is connection-specific, but inherits from `tbl_dbi`.
    log_tbl = NULL,

    #' @field output_to_console (`logical(1)`)\cr
    #'   Should the Logger output to console?
    #'   This can always be overridden by Logger$log_info(..., output_to_console = FALSE).
    output_to_console = NULL,

    #' @description
    #'   Create a new Logger object
    #' @param log_conn (`DBIConnection`)\cr
    #'   A database connection where log table should exist.
    #' @param warn (`logical(1)`)\cr
    #'   Should a warning be produced if neither log_table_id or log_path could be determined?
    #' @param output_to_console (`logical(1)`)\cr
    #'   Should the Logger output to console?
    initialize = function(
      db_table = NULL,
      ts = NULL,
      start_time = Sys.time(),
      # DB
      log_table_id = getOption("SCDB.log_table_id"),
      log_conn = NULL,
      # File
      log_path = getOption("SCDB.log_path"),
      # Console
      output_to_console = TRUE,
      warn = TRUE
    ) {

      # Initialize logger
      coll <- checkmate::makeAssertCollection()
      assert_id_like(db_table, null.ok = TRUE, add = coll)
      assert_timestamp_like(ts, null.ok = TRUE, add = coll)
      checkmate::assert_posixct(start_time, add = coll)
      assert_id_like(log_table_id, null.ok = TRUE, add = coll)
      checkmate::assert_class(log_conn, "DBIConnection", null.ok = is.null(log_table_id), add = coll)
      checkmate::assert_character(log_path, null.ok = TRUE, add = coll)
      checkmate::reportAssertions(coll)

      self$output_to_console <- output_to_console

      if (!is.null(db_table)) {
        private$db_table <- id(db_table, log_conn)
      }

      private$ts <- ts
      private$.start_time <- start_time

      if (!is.null(log_table_id)) {
        log_table_id <- id(log_table_id, log_conn)
        self$log_tbl <- create_logs_if_missing(log_conn, log_table_id)
      }
      private$log_conn <- log_conn

      self$log_path <- log_path

      if (warn && (is.null(log_path) & is.null(log_table_id))) {
        warning("log_path and log_table_id are both NULL and therefore NO LOGGING WILL BE DONE.\n",
                "Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile")
      }


      # Create a line in log DB for Logger
      private$generate_db_entry()

    },

    #' @description
    #' Remove generated log_name from database if not writing to a file
    finalize = function() {
      self$finalize_db_entry()
    },

    #' @description
    #'   Write a line to log file.
    #' @param ... `r log_dots <- "One or more character strings to be concatenated"; log_dots`
    #' @param tic  start_time (`POSIXct(1)`)\cr
    #'   The timestamp used by the log entry (defaults to [Sys.time()]).
    #' @param output_to_console (`logical(1)`)\cr
    #'   Should the line be written to console?
    #' @param log_type `r log_type <- "A character string which describes the severity of the log message"; log_type`
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
      if (!is.null(self$log_path)) {
        write(msg, self$log_realpath, append = TRUE)
      }

      return(invisible(msg))

    },

    #' @description
    #'   Write a warning to log file and generate warning.
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_warn = function(..., log_type = "WARNING") {
      warning(self$log_info(..., log_type = log_type))
    },

    #' @description
    #'   Write an error to log file and stop execution.
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_error = function(..., log_type = "ERROR") {
      stop(self$log_info(..., log_type = log_type))
    },

    #' @description
    #'   Write or update log table.
    #' @param ...
    #'   Name-value pairs with which to update the log table.
    log_to_db = function(...) {

      # Only write if we have a valid connection
      if (!is.null(private$log_conn) && !is.null(self$log_tbl) && DBI::dbIsValid(private$log_conn) &&
            table_exists(private$log_conn, self$log_tbl)) {

        patch <- data.frame(log_file = self$log_filename) |>
          dplyr::copy_to(
            dest = private$log_conn,
            df = _,
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

      assert_timestamp_like(self$start_time)
      assert_timestamp_like(end_time)

      # Auto-fill log with end time and duration
      self$log_to_db(
        end_time = !!db_timestamp(end_time, private$log_conn),
        duration = !!format(round(difftime(as.POSIXct(end_time), as.POSIXct(self$start_time)), digits = 2))
      )


      # Remove the log_file from the log table if no actual file is being written
      if (is.null(self$log_path) && !is.null(private$log_conn) && !is.null(self$log_tbl) &&
            DBI::dbIsValid(private$log_conn) && table_exists(private$log_conn, self$log_tbl)) {

        expected_rows <- self$log_tbl |>
          dplyr::filter(log_file == !!self$log_filename) |>
          dplyr::count() |>
          dplyr::pull()

        query <- dbplyr::build_sql(
          "UPDATE ",
          dbplyr::as.sql(id(self$log_tbl, conn = private$log_conn), con = private$log_conn),
          " SET ",
          dbplyr::ident("log_file"),
          " = NULL WHERE ",
          dbplyr::ident("log_file"),
          " = '",
          sql(self$log_filename),
          "'",
          con = private$log_conn
        )

        affected_rows <- DBI::dbExecute(private$log_conn, query)
        if (affected_rows != expected_rows) {
          rlang::warn("Something went wrong while finalizing Logger",
                      log_filename = self$log_filename,
                      affected_rows = affected_rows,
                      expected_rows = expected_rows)
        }
      }
    }
  ),

  private = list(

    # @field start_time (`POSIXct(1)`)\cr
    #   The time at which data processing was started.
    .start_time = NULL,

    .log_filename = NULL,
    db_table = NULL,
    log_conn = NULL,
    ts = NULL,

    generate_db_entry = function() {
      # Create a row for log in question
      if (is.null(self$log_tbl)) {
        return(NULL)
      }

      coll <- checkmate::makeAssertCollection()
      assert_timestamp_like(private$ts, add = coll)
      assert_id_like(private$db_table, add = coll)
      assert_timestamp_like(self$start_time, add = coll)
      checkmate::reportAssertions(coll)

      patch <- data.frame(
        log_file = self$log_filename,
        schema = purrr::pluck(private$db_table, "name", "schema"),
        table = purrr::pluck(private$db_table, "name", "table")
      ) |>
        dplyr::copy_to(
          dest = private$log_conn,
          df = _,
          name = unique_table_name(),
          temporary = TRUE
        )
      defer_db_cleanup(patch) # Clean up on exit

      dplyr::rows_append(
        x = self$log_tbl,
        y = dplyr::mutate(
          patch,
          date = !!db_timestamp(private$ts, private$log_conn),
          start_time = !!db_timestamp(self$start_time, private$log_conn)
        ),
        copy = TRUE,
        in_place = TRUE
      )
    },


    log_format = function(..., tic = Sys.time(), log_type = NULL, timestamp_format = NULL) {
      checkmate::assert_character(timestamp_format)
      checkmate::assert_posixct(tic)

      return(paste(format(tic, timestamp_format), Sys.info()[["user"]], log_type, paste(...), sep = " - "))
    }
  ),

  active = list(
    #' @field start_time (`POSIXct(1)`)\cr
    #'   The time at which data processing was started. Read only.
    start_time = function(value) {
      if (missing(value)) {
        return(private$.start_time)
      } else {
        stop(glue::glue("`$start_time` is read only"), call. = FALSE)
      }
    },


    #' @field log_filename `character(1)`\cr
    #'   The filename (basename) of the file that the `Logger` instance will output to
    log_filename = function() {
      # If we are not producing a file log, we provide a random string to key by
      if (!is.null(private$.log_filename)) {
        return(private$.log_filename)
      }
      if (is.null(self$log_path)) {
        private$.log_filename <- basename(tempfile(tmpdir = "", pattern = ""))
        return(private$.log_filename)
      }

      coll <- checkmate::makeAssertCollection()
      assert_dbtable_like(private$db_table, add = coll)
      assert_timestamp_like(private$ts, null.ok = FALSE, add = coll)
      checkmate::reportAssertions(coll)

      start_format <- format(self$start_time, "%Y%m%d.%H%M")
      ts <- private$ts

      if (is.character(ts)) ts <- as.Date(ts)
      ts_format <- format(ts, "%Y_%m_%d")
      filename <- sprintf(
        "%s.%s.%s.log",
        start_format,
        ts_format,
        private$db_table
      )

      private$.log_filename <- filename

      if (file.exists(file.path(self$log_path, private$.log_filename))) {
        stop(sprintf("Log file '%s' already exists!", private$.log_filename))
      }

      return(filename)
    },

    #' @field log_realpath `character(1)`\cr
    #'   The full path to the logger's log file.
    log_realpath = function() {
      if (is.null(self$log_path)) {
        return(nullfile())
      } else {
        return(file.path(self$log_path, self$log_filename))
      }
    }
  )
)


#' No-logging Logger
#'
#' @description
#'   Create a [Logger] object that does no logging.
#'
#' @importFrom R6 R6Class
#' @examples
#'   logger <- NullLogger$new()
#' @return A new instance of the `Logger` [R6][R6::R6Class] class where no logging will be done.
#' @export
NullLogger <- R6::R6Class(                                                                                              # nolint: object_name_linter
  classname  = "NullLogger",
  inherit = Logger,
  public = list(

    #' @description
    #'   Create a new `NullLogger` object
    #' @param ... Captures arguments given, but does nothing
    initialize = function(...){
    },

    #' @description
    #'   Matches the signature of `Logger$log_info()`, but does nothing.
    #' @param ... Captures arguments given, but does nothing
    log_info = function(...) {
      return(invisible(NULL))
    },

    #' @description
    #'   Matches the signature of `Logger$log_warn()`, but does nothing.
    #' @param ... Captures arguments given, but does nothing
    log_warn = function(...) {
      return(invisible(NULL))
    },

    #' @description
    #'   Matches the signature of `Logger$log_error()`, but does nothing.
    #' @param ... Captures arguments given, but does nothing
    log_error = function(...) {
      return(invisible(NULL))
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
