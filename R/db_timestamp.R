#' Determine the type of timestamps the database supports
#'
#' @name db_timestamp
#' @param timestamp (`POSIXct(1)` or `character(1)`)\cr
#'   The timestamp to be transformed to the database type.
#' @template conn
#' @return
#'   The given timestamp converted to a SQL-backend dependent timestamp.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   db_timestamp(Sys.time(), conn)
#'
#'   close_connection(conn)
#' @export
db_timestamp <- function(timestamp, conn = NULL) {
  if (is.null(conn)) db_timestamp.NULL(timestamp, conn)
  else UseMethod("db_timestamp", conn)
}

#' @export
db_timestamp.default <- function(timestamp, conn) {
  # Cast to POSIXct with local timezone
  timestamp <- lubridate::parse_date_time(
    timestamp,
    orders = c(
      "ymdHMSOS",
      "ymdHMS",
      "ymdHM",
      "ymd"
    ),
    tz = Sys.timezone()
  )

  return(dbplyr::translate_sql(!!timestamp, con = conn))
}

#' @export
db_timestamp.NULL <- function(timestamp, conn) {
  # Cast to POSIXct with local timezone
  timestamp <- lubridate::parse_date_time(
    timestamp,
    orders = c(
      "ymdHMSOS",
      "ymdHMS",
      "ymdHM",
      "ymd"
    ),
    tz = Sys.timezone()
  )

  return(as.POSIXct(timestamp, tz = Sys.timezone()))
}

#' @export
db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    return(dbplyr::translate_sql(NA_character_, con = conn))
  } else {
    return(dbplyr::translate_sql(!!as.character(timestamp), con = conn))
  }
}
