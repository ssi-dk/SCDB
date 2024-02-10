#' Get the current catalog of a database-related objects
#'
#' @name get_catalog
#' @inheritParams get_schema
#' @return
#' For `get_catalog.Microsoft SQL Server`, the current database context of the connection or "tempdb" if
#' `temporary = TRUE`.
#'
#' For `get_catalog.*`, `NULL` is returned.
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'
#' get_catalog(conn)
#' get_catalog(get_table(conn, id("mtcars", conn = conn)))
#'
#' close_connection(conn)
#' @export
get_catalog <- function(obj, ...) {
  UseMethod("get_catalog")
}

#' @export
get_catalog.tbl_dbi <- function(obj,  ...) {
  return(purrr::pluck(id(obj), "name", "catalog"))
}

#' @export
get_catalog.Id <- function(obj, ...) {
  return(purrr::pluck(obj@name, "catalog"))
}

#' @rdname get_catalog
#' @export
`get_catalog.Microsoft SQL Server` <- function(obj, temporary = FALSE, ...) {
  checkmate::assert_logical(temporary)

  if (temporary) {
    return("tempdb")
  } else {
    query <- paste("SELECT DB_NAME() AS current_database;")
    return(DBI::dbGetQuery(obj, query)$current_database)
  }
}

#' @export
get_catalog.default <- function(obj, ...) {
  return(NULL)
}
