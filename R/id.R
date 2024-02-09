#' Convenience function for DBI::Id
#'
#' @template db_table_id
#' @template conn
#' @param allow_table_only
#'  logical. If `TRUE`, allows for returning an `DBI::Id` with `table` = `myschema.table` if schema `myschema`
#'  is not found in `conn`.
#'  If `FALSE`, the function will raise an error if the implied schema cannot be found in `conn`
#' @details The given `db_table_id` is parsed to a DBI::Id depending on the type of input:
#'  * `character`: db_table_id is parsed to a DBI::Id object using an assumption of "schema.table" syntax
#'     with corresponding schema (if found in `conn`) and table values.
#'     If no schema is implied, the default schema of `conn` will be used.
#'
#'  * `DBI::Id`: if schema is not specified in `Id`, the schema is set to the default schema for `conn` (if given).
#'
#'  * `tbl_sql`: the remote name is used to resolve the table identification.
#'
#' @return A DBI::Id object parsed from db_table_id (see details)
#' @examples
#' id("schema.table")
#' @seealso [DBI::Id] which this function wraps.
#' @export
id <- function(db_table_id, ...) {
  UseMethod("id")
}


#' @export
#' @rdname id
id.Id <- function(db_table_id, conn = NULL, ...) {

  # Store the table_name for computations down the line
  table_name <- purrr::pluck(db_table_id, "name", "table")

  # Determine if the table would be in a temporary database / catalog
  if (inherits(conn, "Microsoft SQL Server")) {
    catalog <- get_catalog(conn, temporary = startsWith(table_name, "#"))
  } else {
    catalog <- get_catalog(conn)
  }

  fully_qualified_id <- DBI::Id(
    catalog = purrr::pluck(db_table_id, "name", "catalog", .default = catalog),
    schema = purrr::pluck(db_table_id, "name", "schema", .default = SCDB::get_schema(conn)),
    table = table_name
  )

  return(fully_qualified_id)
}


#' @export
#' @rdname id
id.character <- function(db_table_id, conn = NULL, allow_table_only = TRUE, ...) {

  checkmate::assert(is.null(conn), DBI::dbIsValid(conn), combine = "or")

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split(db_table_id, "\\.")[[1]]
    db_name <- db_name[rev(seq_along(db_name))] # Reverse order (table, schema?, catalog?)

    db_table <- purrr::pluck(db_name, 1)
    db_schema <- purrr::pluck(db_name, 2)
    db_catalog <- purrr::pluck(db_name, 3)

    # If no matching implied schema is found, return the unmodified db_table_id in the default schema
    if (allow_table_only && !is.null(conn) && !schema_exists(conn, db_schema)) {
      return(DBI::Id(catalog = db_catalog, schema = get_schema(conn), table = db_table_id))
    }
  } else {
    db_table <- db_table_id
    db_schema <- get_schema(conn)
    db_catalog <- get_catalog(conn)
  }

  return(DBI::Id(catalog = db_catalog, schema = db_schema, table = db_table))
}


#' @export
id.tbl_dbi <- function(db_table_id, ...) {

  if (is.null(dbplyr::remote_table(db_table_id))) {
    stop(
      "Table identification can only be determined if the lazy query is unmodified ",
      "(i.e. no dplyr manipulation steps can be made)!"
    )
  }

  # Store currently known information about table
  table_conn <- dbplyr::remote_con(db_table_id)
  table_ident <- dbplyr::remote_table(db_table_id) |>
    unclass() |>
    purrr::discard(is.na)

  table <- purrr::pluck(table_ident, "table")
  schema <- purrr::pluck(table_ident, "schema")
  catalog <- purrr::pluck(table_ident, "catalog")

  # If only table is known, attempt to attempt to resolve the table from existing tables.
  # For SQLite, there should only be one table in main/temp matching the table.
  # In some cases, tables may have been added to the DB that makes the id ambiguous.
  if (is.null(schema)) {

    # If not, attempt to resolve the table from existing tables.
    # For SQLite, there should only be one table in main/temp matching the table
    # In some cases, tables may have been added to the DB that makes the id ambiguous.
    schema <- get_tables(table_conn, show_temporary = TRUE) |>
      dplyr::filter(.data$table == !!table) |>
      dplyr::pull("schema")

    if (length(schema) > 1)  {
      stop(
        "Table identification has been corrupted! ",
        "This table does not contain information about its schema and ",
        "multiple tables with this name were found across schemas."
      )
    }
  }

  # Is the table temporary?
  if (is.null(catalog) &&
        (inherits(table_conn, "Microsoft SQL Server") && startsWith(table, "#")) ||
        identical(schema, get_schema(table_conn, temporary = TRUE))) {
    catalog <- get_catalog(table_conn, temporary = TRUE)
  } else {
    catalog <- get_catalog(table_conn, temporary = FALSE)
  }

  # Return the inferred Id
  return(DBI::Id(catalog = catalog, schema = schema, table = table))
}


#' @export
as.character.Id <- function(x, ...) {

  info <- x@name |>
    purrr::discard(is.na)

  id_representation <- list(
    catalog = purrr::pluck(info, "catalog"),
    schema = purrr::pluck(info, "schema"),
    table = purrr::pluck(info, "table")
  ) |>
    purrr::discard(is.null) |>
    do.call(purrr::partial(paste, sep = "."), args = _)

  return(id_representation)
}
