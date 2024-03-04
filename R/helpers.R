#' nrow() but also works on remote tables
#'
#' @template .data
#' @return
#'   The number of records in the object.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   m <- dplyr::copy_to(conn, mtcars)
#'   nrow(m) == nrow(mtcars) # TRUE
#'
#'   close_connection(conn)
#' @export
nrow <- function(.data) {
  if (inherits(.data, "tbl_dbi")) {
    return(as.numeric(dplyr::pull(dplyr::count(dplyr::ungroup(.data)))))
  } else {
    return(base::nrow(.data))
  }
}


#' Checks if table contains historical data
#'
#' @template .data
#' @return
#'   `TRUE` if `.data` contains the columns: "checksum", "from_ts", and "until_ts". `FALSE` otherwise.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'   create_table(mtcars, conn, db_table = id("mtcars_historical", conn))
#'
#'   is.historical(get_table(conn, "mtcars")) # FALSE
#'   is.historical(get_table(conn, "mtcars_historical")) # TRUE
#'
#'   close_connection(conn)
#' @export
is.historical <- function(.data) {                                                                                      # nolint: object_name_linter

  # Check arguments
  assert_data_like(.data)

  return(any(c("checksum", "from_ts", "until_ts") %in% colnames(.data)))
}


#' Delete table at function exit
#' @description
#'   This function marks a table for deletion once the current function exits.
#' @param db_table (`tbl_sql`)\cr
#'   A unmanipulated reference to a sql table.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   mt <- dplyr::copy_to(conn, mtcars)
#'   id_mt <- id(mt)
#'
#'   defer_db_cleanup(mt)
#'
#'   DBI::dbExistsTable(conn, id_mt) # TRUE
#'
#'   withr::deferred_run()
#'
#'   DBI::dbExistsTable(conn, id_mt) # FALSE
#'
#'   close_connection(conn)
#' @return NULL (called for side effects)
#' @export
defer_db_cleanup <- function(db_table) {

  # Determine table info
  conn <- dbplyr::remote_con(db_table)
  db_table_id <- id(db_table)

  # Remove table on exit
  withr::defer_parent(
    if (DBI::dbIsValid(conn) && DBI::dbExistsTable(conn, db_table_id)) {
      DBI::dbRemoveTable(conn, db_table_id)
    }
  )
}


#' Create a name for a temporary table
#' @description
#'   This function is heavily inspired by the unexported dbplyr function unique_table_name
#' @param scope (`character(1)`)\cr
#'   A naming scope to generate the table name within.
#' @examples
#'   print(unique_table_name()) # SCDB_001
#'   print(unique_table_name()) # SCDB_002
#'
#'   print(unique_table_name("test")) # test_001
#'   print(unique_table_name("test")) # test_002
#'
#' @return A character string for a table name based on the given scope parameter
#' @export
unique_table_name <- function(scope = "SCDB") {
  option <- paste(scope, "table_name", sep = "_")
  index <- getOption(option, default = 0) + 1
  options(tibble::lst(!!option := index))
  return(glue::glue("{scope}_{sprintf('%03i', index)}"))
}


#' checkmate helper: Assert "generic" data.table/data.frame/tbl/tibble type
#'
#' @template .data
#' @param ...
#'   Parameters passed to checkmate::check_*.
#' @template add
#' @inherit checkmate::assert return
#' @noRd
assert_data_like <- function(.data, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_class(.data, "tbl_dbi", ...),
    checkmate::check_data_frame(.data, ...),
    checkmate::check_data_table(.data, ...),
    checkmate::check_tibble(.data, ...),
    add = add
  )
}


#' checkmate helper: Assert "generic" timestamp type
#' @param timestamp (`any(1)`)\cr
#'   Object to test if is `POSIXct`, `Date` or `character`.
#' @param ...
#'   Parameters passed to checkmate::check_*.
#' @template add
#' @inherit checkmate::assert return
#' @noRd
assert_timestamp_like <- function(timestamp, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_posixct(timestamp, ...),
    checkmate::check_character(timestamp, ...),
    checkmate::check_date(timestamp, ...),
    add = add
  )
}


#' checkmate helper: Assert for "generic" db_table type
#' @param db_table (`any(1)`)\cr
#'   Object to test if is of class `tbl_dbi` or `character` on form "[catalog].schema.table".
#' @param ...
#'   Parameters passed to checkmate::check_*.
#' @template add
#' @inherit checkmate::assert return
#' @noRd
assert_dbtable_like <- function(db_table, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(db_table, pattern = r"{^\w*.\w*$}", ...),
    checkmate::check_class(db_table, "Id", ...),
    checkmate::check_class(db_table, "tbl_dbi", ...),
    add = add
  )
}


#' checkmate helper: Assert for "generic" id structure
#' @param id (`any(1)`)\cr
#'   Object to test if is coercible by `id()`.
#' @param ...
#'   Parameters passed to checkmate::check_*.
#' @template add
#' @inherit checkmate::assert return
#' @noRd
assert_id_like <- function(id, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(id, ...),
    checkmate::check_class(id, "Id", ...),
    checkmate::check_class(id, "tbl_dbi", ...),
    add = add
  )
}
