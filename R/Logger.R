#' @title Logger
#' @description
#' Create an object for logging database operations
#'
#' @importFrom R6 R6Class
#' @param db_tablestring A string specifying the table being updated
#' @template log_table_id
#' @template log_path
#' @param ts A timestamp describing the data being processed (not the current time)
#' @param start_time The time at which data processing was started (defaults to [Sys.time()])
#' @examples
#' logger <- Logger$new(db_tablestring = "test.table",
#'                      ts = "2020-01-01 09:00:00")
#' @return A new instance of the `Logger` [R6][R6::R6Class] class.
#' @export
Logger <- R6::R6Class( #nolint: object_name_linter
  classname  = "Logger",
  public = list(

    #' @field log_path (`character(1)`)\cr
    #' A directory where log file is written (if this is not NULL). Defaults to `getOption("SCDB.log_path")`.
    log_path = NULL,

    #' @field log_tbl
    #' The DB table used for logging. Class is connection-specific, but inherits from `tbl_dbi`.
    log_tbl = NULL,

    #' @field start_time (`POSIXct(1)`)\cr
    #' The time at which data processing was started.
    start_time = NULL,

    #' @field output_to_console (`logical(1)`)\cr
    #' Should the Logger output to console?
    #' This can always be overridden by Logger$log_info(..., output_to_console = FALSE).
    output_to_console = NULL,

    #' @description
    #' Create a new Logger object
    #' @param log_conn A database connection inheriting from `DBIConnection`
    #' @param warn Show a warning if neither log_table_id or log_path could be determined
    #' @param output_to_console Should the Logger output to console (TRUE/FALSE)?
    initialize = function(db_tablestring = NULL,
                          log_table_id   = getOption("SCDB.log_table_id"),
                          log_conn = NULL,
                          log_path = getOption("SCDB.log_path"),
                          output_to_console = TRUE,
                          warn = TRUE,
                          ts = NULL,
                          start_time = Sys.time()) {

      # Initialize logger
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_character(db_tablestring, null.ok = TRUE, add = coll)
      assert_id_like(log_table_id, null.ok = TRUE, add = coll)
      checkmate::assert_class(log_conn, "DBIConnection", null.ok = TRUE, add = coll)
      checkmate::assert_character(log_path, null.ok = TRUE, add = coll)
      assert_timestamp_like(ts, null.ok = TRUE, add = coll)
      checkmate::assert_posixct(start_time, add = coll)
      checkmate::reportAssertions(coll)

      self$output_to_console <- output_to_console

      private$ts <- ts
      self$start_time <- start_time
      lockBinding("start_time", self)

      if (!is.null(log_table_id)) {
        log_table_id <- id(log_table_id, log_conn)
        self$log_tbl <- create_logs_if_missing(log_table_id, log_conn)
      }
      private$log_conn <- log_conn

      if (warn && (is.null(log_path) & is.null(log_table_id))) {
        warning("log_path and log_table_id are both NULL and therefore NO LOGGING WILL BE DONE.\n",
                "Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile")
      }

      self$log_path <- log_path
      private$db_tablestring <- db_tablestring

      # Create a line in log DB for Logger
      private$generate_log_entry()

    },

    #' @description
    #' Remove generated log_name from database if not writing to a file
    finalize = function() {
      if (is.null(self$log_path) &&
            !is.null(self$log_tbl) &&
            DBI::dbIsValid(private$log_conn) &&
            table_exists(private$log_conn, self$log_tbl)) {

        expected_rows <- self$log_tbl |>
          dplyr::filter(log_file == !!self$log_filename) |>
          dplyr::count() |>
          dplyr::pull()

        query <- dbplyr::build_sql(
          "UPDATE ",
          ident(remote_name(self$log_tbl)),
          " SET ",
          ident("log_file"),
          " = NULL WHERE ",
          ident("log_file"),
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
    },

    #' @description
    #' Write a line to log file
    #' @param ... `r log_dots <- "One or more character strings to be concatenated"; log_dots`
    #' @param tic The timestamp used by the log entry (default Sys.time())
    #' @param output_to_console Should the line be written to console?
    #' @param log_type `r log_type <- "A character string which describes the severity of the log message"; log_type`
    log_info = function(..., tic = Sys.time(), output_to_console = self$output_to_console, log_type = "INFO") {

      format_str <- private$log_format(..., tic = tic, log_type = log_type)

      sink(
        file = self$log_realpath,
        split = isTRUE(output_to_console),
        append = TRUE,
        type = "output"
      )
      cat(private$log_format(..., tic = tic, log_type = log_type), "\n", sep = "")
      sink()
    },

    #' @description Write a warning to log file and generate warning.
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_warn = function(..., log_type = "WARNING") {
      self$log_info(..., log_type = log_type)
      warning(private$log_format(..., log_type = log_type))
    },

    #' @description Write an error to log file and stop execution
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_error = function(..., log_type = "ERROR") {
      self$log_info(..., log_type = log_type)
      stop(private$log_format(..., log_type = log_type))
    },

    #' @description Write or update log table
    #' @param ... Name-value pairs with which to update the log table
    log_to_db = function(...) {
      if (is.null(self$log_tbl)) return()

      dplyr::rows_patch(
        x = self$log_tbl,
        y = data.frame(log_file = self$log_filename) |>
          dplyr::mutate(...),
        by = "log_file",
        copy = TRUE,
        in_place = TRUE,
        unmatched = "ignore"
      )
    }
  ),

  private = list(

    .log_filename = NULL,
    db_tablestring = NULL,
    log_conn = NULL,
    ts = NULL,

    generate_log_entry = function() {
      # Create a row for log in question
      if (is.null(self$log_tbl)) return()

      dplyr::rows_append(x = self$log_tbl,
                         y = data.frame(log_file = self$log_filename),
                         copy = TRUE,
                         in_place = TRUE)
    },

    log_format = function(..., tic = self$start_time, log_type = NULL) {
      ts_str <- stringr::str_replace(format(tic, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")

      return(paste(ts_str, Sys.info()[["user"]], log_type, paste(...), sep = " - "))
    }
  ),

  active = list(
    #' @field log_filename `character(1)`\cr
    #' The filename (basename) of the file that the `Logger` instance will output to
    log_filename = function() {
      # If we are not producing a file log, we provide a random string to key by
      if (!is.null(private$.log_filename)) return(private$.log_filename)
      if (is.null(self$log_path)) {
        private$.log_filename <- basename(tempfile(tmpdir = "", pattern = ""))
        return(private$.log_filename)
      }

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_character(private$db_tablestring, null.ok = FALSE, add = coll)
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
        private$db_tablestring
      )

      private$.log_filename <- filename

      if (file.exists(file.path(self$log_path, private$.log_filename))) {
        stop(sprintf("Log file '%s' already exists!", private$.log_filename))
      }

      return(filename)
    },

    #' @field log_realpath `character(1)`\cr
    #' The full path to the logger's log file.
    log_realpath = function() {
      if (is.null(self$log_path)) {
        return(nullfile())
      } else {
        return(file.path(self$log_path, self$log_filename))
      }
    }
  )
)
