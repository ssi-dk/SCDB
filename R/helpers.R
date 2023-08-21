#' @import tidyverse dbplyr lubridate
NULL

#' nrow() but also works on remote tables
#'
#' @param .data lazy_query to parse
#' @return The number of records in the object
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' m <- dplyr::copy_to(conn, mtcars)
#' nrow(m) == nrow(mtcars) # TRUE
#'
#' close_connection(conn)
#' @export
nrow <- function(.data) {
  if (inherits(.data, "tbl_dbi")) {
    return(dplyr::pull(dplyr::count(dplyr::ungroup(.data))))
  } else {
    return(base::nrow(.data))
  }
}


#' Checks if table contains historical data
#'
#' @template .data
#' @return TRUE if .data contains the columns: "checksum", "from_ts", and "until_ts". FALSE otherwise
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = id("mtcars", conn))
#' create_table(mtcars, conn, db_table_id = id("mtcars_historical", conn))
#'
#' is.historical(get_table(conn, "mtcars")) # FALSE
#' is.historical(get_table(conn, "mtcars_historical")) # TRUE
#'
#' close_connection(conn)
#' @export
is.historical <- function(.data) { # nolint: object_name_linter

  # Check arguments
  assert_data_like(.data)

  return(any(c("checksum", "from_ts", "until_ts") %in% colnames(.data)))
}



#' not-in operator
#' @inheritParams base::match
#' @export
`%notin%` <- function(x, table) {
  return(!(x %in% table))
}


#' checkmate helper: Assert "generic" data.table/data.frame/tbl/tibble type
#' @param .data Object to test if is data.table, data.frame, tbl or tibble
#' @param ...   Parameters passed to checkmate::check_*
#' @param add   `AssertCollection` to add assertions to
assert_data_like <- function(.data, ..., add = NULL) {
  checkmate::assert( # nolint start: indentation_linter
    checkmate::check_class(.data, "tbl_dbi", ...),
    checkmate::check_data_frame(.data, ...),
    checkmate::check_data_table(.data, ...),
    checkmate::check_tibble(.data, ...),
    add = add) # nolint end
}


#' checkmate helper: Assert "generic" timestamp type
#' @param timestamp Object to test if is POSIX or character
#' @param ...       parameters passed to checkmate::check_*
#' @param add       `AssertCollection` to add assertions to
assert_timestamp_like <- function(timestamp, ..., add = NULL) {
  checkmate::assert( # nolint start: indentation_linter
    checkmate::check_posixct(timestamp, ...),
    checkmate::check_character(timestamp, ...),
    checkmate::check_date(timestamp, ...),
    add = add) # nolint end
}


#' checkmate helper: Assert for "generic" db_table type
#' @param db_table Object to test if is of class "tbl_dbi" or character on form "schema.table"
#' @param ...      Parameters passed to checkmate::check_*
#' @param add      `AssertCollection` to add assertions to
assert_dbtable_like <- function(db_table, ..., add = NULL) {
  checkmate::assert( # nolint start: indentation_linter
    checkmate::check_character(db_table, pattern = r"{^\w*.\w*$}", ...),
    checkmate::check_class(db_table, "Id", ...),
    checkmate::check_class(db_table, "tbl_dbi", ...),
    add = add) # nolint end
}


#' checkmate helper: Assert for "generic" id structure
#' @param id   Object to test if is of class "Id" or character on form "schema.table"
#' @param ...  Parameters passed to checkmate::check_*
#' @param add `AssertCollection` to add assertions to
assert_id_like <- function(id, ..., add = NULL) {
  checkmate::assert( # nolint start: indentation_linter
    checkmate::check_character(id, ...),
    checkmate::check_class(id, "Id", ...),
    add = add) # nolint end
}
