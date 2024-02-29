#' @rdname get_schema
#' @return
#'   The catalog is extracted from `obj` depending on the type of input:
#'
#'   * For `get_catalog.Microsoft SQL Server`, the current database context of the connection or "tempdb" if
#'     `temporary = TRUE`.
#'
#'   * For `get_schema.tbl_dbi` the catalog is determined via `id()`.
#'
#'   * For `get_catalog.\\*`, `NULL` is returned.
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

#' @rdname get_schema
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
get_catalog.duckdb_connection <- function(obj, temporary = FALSE,  ...) {
  if (temporary) {
    return("temp")
  } else {
    query <- paste("SELECT current_catalog() AS current_database;")
    return(DBI::dbGetQuery(obj, query)$current_database)
  }
}

#' @export
get_catalog.default <- function(obj, ...) {
  return(NULL)
}
