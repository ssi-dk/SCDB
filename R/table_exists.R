#' Test if a table exists in database
#'
#' @name table_exists
#' @description
#'   This functions attempts to determine the existence of a given table.
#'   If a character input is given, matching is done heuristically assuming a "schema.table" notation.
#'   If no schema is implied in this case, the default schema is assumed.
#' @template conn
#' @template db_table
#' @return
#'   `TRUE` if db_table can be parsed to a table found in `conn`.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'   dplyr::copy_to(conn, iris, name = "iris")
#'
#'   table_exists(conn, "mtcars")    # TRUE
#'   table_exists(conn, "iris")      # FALSE
#'   table_exists(conn, "temp.iris") # TRUE
#'
#'   close_connection(conn)
#' @importFrom rlang .data
#' @export
table_exists <- function(conn, db_table) {
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  checkmate::assert(DBI::dbIsValid(conn), add = coll)
  assert_id_like(db_table, add = coll)
  checkmate::reportAssertions(coll)

  # Check arguments
  if (inherits(db_table, "tbl_dbi")) {
    exists <- tryCatch({
      dplyr::collect(utils::head(db_table, 0))
      return(TRUE)
    },
    error = function(e) {
      return(FALSE)
    })

    return(exists)
  }

  UseMethod("table_exists", conn)
}

#' @rdname table_exists
#' @importFrom rlang .data
#' @export
table_exists.DBIConnection <- function(conn, db_table) {
  tables <- get_tables(conn, show_temporary = TRUE)

  if (inherits(db_table, "Id")) {
    db_table_id <- id(db_table, conn) # Ensure Id is fully qualified (has schema)

    exact_match <- tables |>
      dplyr::filter(
        .data$table == db_table_id@name["table"],
        .data$schema == db_table_id@name["schema"]
      )

    if ("catalog" %in% names(db_table_id@name)) {
      exact_match <- exact_match |>
        dplyr::filter(.data$catalog == db_table_id@name["catalog"])
    }


    if (nrow(exact_match) == 1) {
      return(TRUE)
    } else {
      return(FALSE)
    }

  } else if (inherits(db_table, "character")) {

    # Then heuristically match with tables in conn
    matches <- dplyr::union_all(
      tables,
      dplyr::mutate(dplyr::filter(tables, .data$schema == get_schema(conn)), schema = NA_character_)
    ) |>
      tidyr::unite("table_str", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
      dplyr::filter(.data$table_str == !!db_table) |>
      dplyr::select(!"table_str")

    if (nrow(matches) <= 1) {
      return(nrow(matches) == 1)
    } else {
      rlang::abort(
        message = paste0("More than one table matching '", db_table, "' was found!"),
        matches = matches
      )
    }
  } else {
    rlang::abort("Only character or DBI::Id inputs to table_exists is allowed!")
  }
}
