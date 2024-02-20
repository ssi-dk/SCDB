#' Test if a table exists in database
#'
#' @description
#'   This functions attempts to determine the existence of a given table.
#'   If a character input is given, matching is done heuristically assuming a "schema.table" notation.
#'   If no schema is implied in this case, the default schema is assumed.
#' @template conn
#' @template db_table_id
#' @return TRUE if db_table_id can be parsed to a table found in conn
#' @importFrom rlang .data
#' @name table_exists
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#' dplyr::copy_to(conn, iris, name = "iris")
#'
#' table_exists(conn, "mtcars")    # TRUE
#' table_exists(conn, "iris")      # FALSE
#' table_exists(conn, "temp.iris") # TRUE
#'
#' close_connection(conn)
#' @export
table_exists <- function(conn, db_table_id) {
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  checkmate::assert(DBI::dbIsValid(conn), add = coll)
  assert_id_like(db_table_id, add = coll)
  checkmate::reportAssertions(coll)

  # Check arguments
  if (inherits(db_table_id, "tbl_dbi")) {
    exists <- tryCatch({
      dplyr::collect(utils::head(db_table_id, 0))
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
table_exists.DBIConnection <- function(conn, db_table_id) {
  tables <- get_tables(conn, show_temporary = TRUE)

  if (inherits(db_table_id, "Id")) {
    db_table_id <- id(db_table_id, conn) # Ensure Id is fully qualified (has schema)

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

  } else if (inherits(db_table_id, "character")) {

    # Check if schema is implied -- use default if not implied
    if (!stringr::str_detect(db_table_id, r"{\w*\.\w*}")) {
      db_table_id <- paste(get_schema(conn), db_table_id, sep = ".")
    }

    # Then heuristically match with tables in conn
    matches <- dplyr::union_all(
      tables,
      dplyr::mutate(dplyr::filter(tables, .data$schema == get_schema(conn)), schema = NA)
    ) |>
      tidyr::unite("table_str", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
      dplyr::filter(.data$table_str == !!db_table_id) |>
      dplyr::select(!"table_str")

    if (nrow(matches) <= 1) {
      return(nrow(matches) == 1)
    } else {
      rlang::abort(
        message = paste0("More than one table matching '", db_table_id, "' was found!"),
        matches = matches
      )
    }
  } else {
    rlang::abort("Only character or DBI::Id inputs to table_exists is allowed!")
  }
}
