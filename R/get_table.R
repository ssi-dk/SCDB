#' Retrieves a named table from a given schema on the connection
#'
#' @template conn
#' @templateVar miss TRUE
#' @template db_table
#' @param slice_ts (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   If set different from `NA` (default), the returned data looks as on the given date.
#'   If set as `NULL`, all data is returned.
#' @param include_slice_info (`logical(1)`)\cr
#'   Should the history columns "checksum", "from_ts", "until_ts" are also be returned?
#' @return
#'   A "lazy" data.frame (tbl_lazy) generated using dbplyr.
#'
#'   Note that a temporary table will be preferred over ordinary tables in the default schema (see [get_schema()]) with
#'   an identical name.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'
#'   get_table(conn)
#'   if (table_exists(conn, "mtcars")) {
#'     get_table(conn, "mtcars")
#'   }
#'
#'   close_connection(conn)
#' @importFrom rlang .data
#' @export
get_table <- function(conn, db_table = NULL, slice_ts = NA, include_slice_info = FALSE) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table, null.ok = TRUE)
  assert_timestamp_like(slice_ts, null.ok = TRUE)
  checkmate::assert_logical(include_slice_info)

  # Get tables in database schema
  if (is.null(db_table)) {
    message("Select one of the following tables:")
    return(get_tables(conn))
  }

  # Ensure id is fully qualified
  db_table_id <- id(db_table, conn = conn)

  # Look-up table in database
  tryCatch({
    q <- dplyr::tbl(conn, db_table_id)
  }, error = function(e) {
    stop(glue::glue("Table {as.character(db_table_id)} could not be found!"), call. = FALSE)
  })

  # Check whether data is historical
  if (is.historical(q) && !is.null(slice_ts)) {

    # Filter based on date
    if (is.na(slice_ts)) {
      q <- dplyr::filter(q, is.na(.data$until_ts)) # Newest data
    } else {
      q <- slice_time(q, slice_ts)
    }

    # Remove history columns
    if (!include_slice_info) {
      q <- dplyr::select(q, !tidyselect::any_of(c("from_ts", "until_ts", "checksum")))
    }
  }

  return(q)
}
