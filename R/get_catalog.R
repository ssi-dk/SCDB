#' @rdname get_schema
#' @param ... Further arguments passed to methods.
#' @return
#' For `get_catalog.Microsoft SQL Server`, the current database context of the connection or "tempdb" if
#' `temorary = TRUE`.
#'
#' For `get_catalog.*`, `NULL` is returned.
#' @export
get_catalog <- function(.x, ...) {
  UseMethod("get_catalog")
}

#' @export
get_catalog.tbl_dbi <- function(.x, ...) {
  catalog <- dbplyr::remote_table(.x) |>
    unclass() |>
    purrr::discard(is.na) |>
    purrr::pluck("catalog")

  return(catalog)
}

#' @export
get_catalog.Id <- function(.x, ...) {
  return(purrr::pluck(.x@name, "catalog"))
}

#' @rdname get_schema
#' @param temporary (`logical(1)`) \cr
#'   Is the table in the temporary database?
#' @export
`get_catalog.Microsoft SQL Server` <- function(.x, temporary = FALSE, ...) {
  checkmate::assert_logical(temporary)

  if (temporary) {
    return("tempdb")
  } else {
    query <- paste("SELECT DB_NAME() AS current_database;")
    return(DBI::dbGetQuery(.x, query)$default_schema)
  }
}

#' @export
get_catalog.default <- function(.x, ...) {
  return(NULL)
}
