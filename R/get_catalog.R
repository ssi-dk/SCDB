#' @rdname get_schema
#' @return
#' For `get_catalog.Microsoft SQL Server`, the current database context of the connection or "tempdb" if
#' `temporary = TRUE`.
#'
#' For `get_catalog.*`, `NULL` is returned.
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
    return(DBI::dbGetQuery(obj, query)$default_schema)
  }
}

#' @export
get_catalog.default <- function(obj, ...) {
  return(NULL)
}
