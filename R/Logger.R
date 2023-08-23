#' @title Logger
#' @description
#' Create an object for logging database operations
#'
#' @param db_tablestring A string specifying the table being updated
#' @template log_table_id
#' @template log_path
#' @param ts A timestamp describing the data being processed (not the current time)
#' @param start_time The time at which data processing was started (defaults to [Sys.time()])
#'
#' @export
Logger <- R6::R6Class( #nolint: object_name_linter
  classname  = "Logger",
  public = list(

    #' @field log_path (`character(1)`)\cr
    #' A directory where log file is written (if this is not NULL). Defaults to `getOption("SCDB.log_path")`.
    log_path = NULL,

    #' @field log_filename (`character(1)`)\cr
    #' The name (basename) of the log file.
    log_filename = NULL,

    #' @field log_tbl
    #' The DB table used for logging. Class is connection-specific, but inherits from `tbl_dbi`.
    log_tbl = NULL,

    #' @field start_time (`POSIXct(1)`)\cr
    #' The time at which data processing was started.
    start_time = NULL,

    #' @description
    #' Create a new Logger object
    #' @param log_conn A database connection inheriting from `DBIConnection`
    #' @param warn Show a warning if neither log_table_id or log_path could be determined
    initialize = function(db_tablestring = NULL,
                          log_table_id   = getOption("SCDB.log_table_id"),
                          log_conn = NULL,
                          log_path = getOption("SCDB.log_path"),
                          warn = TRUE,
                          ts = NULL,
                          start_time = Sys.time()) {

      # Initialize logger
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_character(db_tablestring, add = coll)
      assert_id_like(log_table_id, null.ok = TRUE, add = coll)
      checkmate::assert_class(log_conn, "DBIConnection", null.ok = TRUE, add = coll)
      checkmate::assert_character(log_path, null.ok = TRUE, add = coll)
      assert_timestamp_like(ts, add = coll)
      checkmate::assert_posixct(start_time, add = coll)
      checkmate::reportAssertions(coll)

      private$ts <- ts
      self$start_time <- start_time
      lockBinding("start_time", self)

      if (!is.null(log_table_id)) {
        self$log_tbl <- create_logs_if_missing(log_table_id, log_conn)
      }
      private$log_conn <- log_conn

      if (warn && (is.null(log_path) & is.null(log_table_id))) {
        warning("log_path and log_table_id are both NULL and therefore NO LOGGING WILL BE DONE.\n",
                "Consider adding options SCDB.log_table_id and/or SCDB.log_path to your .Rprofile")
      }

      self$log_path <- log_path
      private$db_tablestring <- db_tablestring
      self$log_filename <- private$generate_filename()
      lockBinding("log_filename", self)

      # Create a line in log DB for Logger
      private$generate_log_entry()

      if (!is.null(self$log_path) && file.exists(file.path(self$log_path, self$log_filename))) {
        stop("Log file for given timestamp already exists!")
      }

    },

    #' @description
    #' Write a line to log file
    #' @param ... `r log_dots <- "One or more character strings to be concatenated"; log_dots`
    #' @param tic The timestamp used by the log entry (default Sys.time())
    #' @param log_type `r log_type <- "A character string which describes the severity of the log message"; log_type`
    log_info = function(..., tic = Sys.time(), log_type = "INFO") {

      # Writes log file (if set)
      if (!is.null(self$log_path)) {
        sink(file = file.path(self$log_path, self$log_filename), split = TRUE, append = TRUE, type = "output")
      }

      cat(private$log_format(..., tic = tic, log_type = log_type), "\n", sep = "")

      if (!is.null(self$log_path)) sink()
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
        y = dplyr::copy_to(private$log_conn, data.frame(log_file = self$log_filename), overwrite = TRUE) |>
          dplyr::mutate(...),
        by = "log_file",
        copy = TRUE,
        in_place = TRUE,
        unmatched = "ignore"
      )
    }
  ),
  private = list(

    db_tablestring = NULL,
    log_conn = NULL,
    ts = NULL,

    generate_filename = function() {
      # If we are not producing a file log, we provide a random string to key by
      if (is.null(self$log_path)) return(basename(tempfile(tmpdir = "", pattern = "")))

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

      return(filename)
    },


    generate_log_entry = function() {
      # Create a row for log in question
      if (!is.null(self$log_tbl)) {
        dplyr::rows_append(x = self$log_tbl,
                           y = data.frame(log_file = self$log_filename),
                           copy = TRUE,
                           in_place = TRUE)
      }

      return()
    },

    log_format = function(..., tic = self$start_time, log_type = NULL) {
      ts_str <- stringr::str_replace(format(tic, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")

      return(paste(ts_str, Sys.info()[["user"]], log_type, paste(...), sep = " - "))
    }
  )
)
