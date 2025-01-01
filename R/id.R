#' Convenience function for DBI::Id
#'
#' @template db_table
#' @template conn
#' @param ... Further arguments passed to methods.
#' @details
#'   The given `db_table` is parsed to a DBI::Id depending on the type of input:
#'
#'   * `character`: db_table is parsed to a DBI::Id object using an assumption of "schema.table" syntax
#'     with corresponding schema (if found in `conn`) and table values.
#'     If no schema is implied, the default schema of `conn` will be used.
#'
#'   * `DBI::Id`: if schema is not specified in `Id`, the schema is set to the default schema for `conn` (if given).
#'
#'   * `tbl_sql`: the remote name is used to resolve the table identification.
#'
#'   * `data.frame`: A Id is built from the data.frame (columns `catalog`, `schema`, and `table`).
#'     Can be used in conjunction with `get_tables(conn, pattern)`.
#'
#' @return
#'   A `DBI::Id` object parsed from db_table (see details).
#' @examples
#'   id("schema.table")
#' @seealso [DBI::Id] which this function wraps.
#' @export
id <- function(db_table, ...) {
  UseMethod("id")
}


#' @export
#' @rdname id
id.Id <- function(db_table, conn = NULL, ...) {

  # Store the table_name for computations down the line
  table_name <- purrr::pluck(db_table, "name", "table")

  # Determine if the table would be in a temporary database / catalog
  if (inherits(conn, "Microsoft SQL Server")) {
    catalog <- get_catalog(conn, temporary = startsWith(table_name, "#"))
  } else {
    catalog <- get_catalog(conn)
  }

  fully_qualified_id <- DBI::Id(
    catalog = purrr::pluck(db_table, "name", "catalog", .default = catalog),
    schema = purrr::pluck(db_table, "name", "schema", .default = get_schema(conn)),
    table = table_name
  )

  return(fully_qualified_id)
}


#' @param allow_table_only (`logical(1)`)\cr
#'   If `TRUE`, allows for returning an `DBI::Id` with `table` = "myschema.table" if schema "myschema"
#'   is not found in `conn`.
#'   If `FALSE`, the function will raise an error if the implied schema cannot be found in `conn`.
#' @export
#' @rdname id
id.character <- function(db_table, conn = NULL, allow_table_only = TRUE, ...) {

  checkmate::assert(is.null(conn), DBI::dbIsValid(conn), combine = "or")

  if (stringr::str_detect(db_table, stringr::fixed("."))) {
    db_name <- stringr::str_split(db_table, stringr::fixed("."))[[1]]
    db_name <- db_name[rev(seq_along(db_name))] # Reverse order (table, schema?, catalog?)

    table <- purrr::pluck(db_name, 1)
    schema <- purrr::pluck(db_name, 2)
    catalog <- purrr::pluck(db_name, 3, .default = get_catalog(conn))

    # If no matching implied schema is found, return the unmodified db_table in the default schema
    if (allow_table_only && !is.null(conn) && !schema_exists(conn, schema)) {
      return(DBI::Id(catalog = catalog, schema = get_schema(conn), table = db_table))
    }
  } else {
    table <- db_table
    schema <- get_schema(conn)
    catalog <- get_catalog(conn)
  }

  return(DBI::Id(catalog = catalog, schema = schema, table = table))
}


#' @export
id.tbl_dbi <- function(db_table, ...) {

  if (is.null(dbplyr::remote_table(db_table))) {
    stop(
      "Table identification can only be determined if the lazy query is unmodified ",
      "(i.e. no dplyr manipulation steps can be made)!"
    )
  }

  # Store currently known information about table
  table_conn <- dbplyr::remote_con(db_table)

  # Check table still exists
  if (!table_exists(table_conn, db_table)) {
    stop("Table does not exist (anymore) and id cannot be determined!")
  }

  table_ident <- dbplyr::remote_table(db_table)

  if (inherits(table_ident, "dbplyr_table_path")) { # dbplyr >= 2.5.0
    components <- dbplyr::table_path_components(table_ident, table_conn)[[1]]

    components <- components[rev(seq_along(components))] # Reverse order (table, schema?, catalog?)

    if (length(components) > 3) {
      stop("Unknown table specification")
    }

    table <- purrr::pluck(components, 1)
    schema <- purrr::pluck(components, 2)
    catalog <- purrr::pluck(components, 3)

  } else {

    table_ident <- table_ident %>%
      unclass() %>%
      purrr::discard(is.na)

    catalog <- purrr::pluck(table_ident, "catalog")
    schema <- purrr::pluck(table_ident, "schema")
    table <- purrr::pluck(table_ident, "table")

  }

  # Match against known tables
  # In some cases, tables may have been added to the database that makes the id ambiguous.
  matches <- get_tables(dbplyr::remote_con(db_table), show_temporary = TRUE) %>%
    dplyr::filter(.data$table == !!table)

  if (!is.null(schema)) matches <- dplyr::filter(matches, .data$schema == !!schema)
  if (!is.null(catalog)) matches <- dplyr::filter(matches, .data$catalog == !!catalog)

  if (nrow(matches) > 1)  {
    stop(
      glue::glue(
        "Table identification has been corrupted! ",
        "The table ({paste(c(catalog, schema, table), collapse = '.')}) does not contain enough information about its ",
        "schema/catalog and multiple tables were matched."
      )
    )
  }

  return(id(matches))
}


#' @export
#' @rdname id
id.data.frame <- function(db_table, ...) {

  checkmate::assert_data_frame(db_table, nrows = 1)
  checkmate::assert_names(
    names(db_table),
    must.include = c("schema", "table"),
    subset.of = c("catalog", "schema", "table")
  )

  # Convert the given data.frame to DBI::Id
  return(
    DBI::Id(
      "catalog" = purrr::pluck(db_table, "catalog"),
      "schema" = purrr::pluck(db_table, "schema"),
      "table" = purrr::pluck(db_table, "table")
    )
  )
}


#' Convert Id object to character
#'
#' @description
#'   This method extends the `as.character` function for objects of class `Id`.
#'   It converts an `Id` object to a character string on the form "catalog.schema.table".
#' @param x (`Id(1)`)\cr
#'   Id object to convert to character.
#' @param explicit (`logical(1)`)\cr
#'   Should Id elements be quoted explicitly?
#' @seealso
#'   \code{\link[base]{as.character}} for the base method.
#' @export
#' @noRd
as.character.Id <- function(x, explicit = FALSE, ...) {

  info <- x@name %>%
    purrr::discard(is.na)

  id_elements <- list(
    catalog = purrr::pluck(info, "catalog"),
    schema = purrr::pluck(info, "schema"),
    table = purrr::pluck(info, "table")
  ) %>%
    purrr::discard(is.null)

  if (explicit) id_elements <- purrr::map(id_elements, ~ paste0('"', ., '"'))

  id_representation <- do.call(purrr::partial(paste, sep = "."), args = id_elements)

  return(id_representation)
}
