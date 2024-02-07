#' Determine the type of timestamps the DB supports
#' @name db_timestamp
#' @param timestamp The timestamp to be transformed to the DB type. Can be character.
#' @param conn A `DBIConnection` to the DB where the timestamp should be stored
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' db_timestamp(Sys.time(), conn)
#'
#' close_connection(conn)
#' @return The given timestamp converted to a SQL-backend dependent timestamp
#' @export
db_timestamp <- function(timestamp, conn = NULL) {
  if (is.null(conn)) db_timestamp.NULL(timestamp, conn)
  else UseMethod("db_timestamp", conn)
}

#' @rdname db_timestamp
#' @export
db_timestamp.default <- function(timestamp, conn) {
  if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
  return(as.POSIXct(timestamp))
}

#' @export
db_timestamp.NULL <- function(timestamp, conn) {
  if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
  return(timestamp)
}

#' @rdname db_timestamp
#' @export
db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    return(NA_character_)
  } else {
    if (inherits(timestamp, "POSIXt")) timestamp <- format(timestamp)
    return(strftime(timestamp))
  }
}
