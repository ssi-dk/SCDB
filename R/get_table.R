#' Gets a named table from a given schema
#'
#' @template conn
#' @templateVar miss TRUE
#' @template db_table_id
#' @param slice_ts
#'   If set different from NA (default), the returned data looks as on the given date.
#'   If set as NULL, all data is returned
#' @param include_slice_info
#'   Default FALSE.
#'   If set TRUE, the history columns "checksum", "from_ts", "until_ts" are returned also
#' @return
#'   A "lazy" dataframe (tbl_lazy) generated using dbplyr.
#'
#'   Note that a temporary table will be preferred over ordinary tables in the default schema (see [get_schema()]) with
#'   an identical name.
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars", temporary = FALSE)
#'
#' get_table(conn)
#' if (table_exists(conn, "mtcars")) {
#'   get_table(conn, "mtcars")
#' }
#'
#' close_connection(conn)
#' @importFrom rlang .data
#' @export
get_table <- function(conn, db_table_id = NULL, slice_ts = NA, include_slice_info = FALSE) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table_id, null.ok = TRUE)
  assert_timestamp_like(slice_ts, null.ok = TRUE)
  checkmate::assert_logical(include_slice_info)

  # Get tables in db schema
  if (is.null(db_table_id)) {
    message("Select one of the following tables:")
    return(get_tables(conn))
  }

  # Ensure id is fully qualified
  db_table_id <- id(db_table_id, conn = conn)

  # Ensure existence of table
  if (!table_exists(conn, db_table_id)) {
    rlang::abort(glue::glue("Table {as.character(db_table_id)} could not be found!"))
  }

  # Look-up table in DB
  q <- dplyr::tbl(conn, db_table_id, check_from = FALSE)

  # Check whether data is historical
  if (is.historical(q) && !is.null(slice_ts)) {

    # Filter based on date
    if (is.na(slice_ts)) {
      q <- dplyr::filter(q, is.na(.data$until_ts)) # Newest data
    } else {
      q <- slice_time(q, slice_ts)
    }

    # Remove history columns
    if (!include_slice_info) {
      q <- dplyr::select(q, !tidyselect::any_of(c("from_ts", "until_ts", "checksum")))
    }
  }

  return(q)
}


#' Gets the available tables
#'
#' @template conn
#' @param pattern A regex pattern with which to subset the returned tables
#' @param show_temporary (`logical`)\cr
#'   Should temporary tables be listed?
#'
#' @return A data.frame containing table names in the DB
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "my_test_table_1", temporary = FALSE)
#' dplyr::copy_to(conn, mtcars, name = "my_test_table_2")
#'
#' get_tables(conn, pattern = "my_[th]est")
#' get_tables(conn, pattern = "my_[th]est", show_temporary = FALSE)
#'
#' close_connection(conn)
#' @importFrom rlang .data
#' @export
get_tables <- function(conn, pattern = NULL, show_temporary = TRUE) {

  checkmate::assert_character(pattern, null.ok = TRUE)
  checkmate::assert_logical(show_temporary)

  UseMethod("get_tables")
}

#' @importFrom rlang .data
#' @export
get_tables.SQLiteConnection <- function(conn, pattern = NULL, show_temporary = TRUE) {
  query <- paste("SELECT schema, name 'table' FROM pragma_table_list",
                 "WHERE NOT name IN ('sqlite_schema', 'sqlite_temp_schema')",
                 "AND NOT name LIKE 'sqlite_stat%'")

  tables <- DBI::dbGetQuery(conn, query)

  if (!show_temporary) {
    tables <- tables |>
      dplyr::filter(.data$schema != "temp")
  }

  if (!is.null(pattern)) {
    tables <- tables |>
      dplyr::mutate(db_table_str = ifelse(
        is.na(.data$schema), .data$table,
        paste(.data$schema, .data$table, sep = ".")
      )) |>
      dplyr::filter(grepl(pattern, .data$db_table_str)) |>
      dplyr::select(!"db_table_str")
  }

  if (!conn@dbname %in% c("", ":memory:") && nrow(tables) == 0) {
    warning("No tables found. Check user privileges / database configuration")
  }

  return(tables)
}

#' @export
#' @importFrom rlang .data
get_tables.PqConnection <- function(conn, pattern = NULL, show_temporary = TRUE) {
  query <- paste("SELECT",
                 "schemaname AS schema,",
                 "tablename AS table,",
                 "is_temporary",
                 "FROM (",
                 "SELECT *, 0 AS is_temporary FROM pg_tables",
                 "WHERE NOT (schemaname LIKE 'pg_%' OR schemaname = 'information_schema')",
                 "UNION ALL",
                 "SELECT *, 1 AS is_temporary FROM pg_tables",
                 "WHERE schemaname LIKE 'pg_temp_%'",
                 ")")

  tables <- DBI::dbGetQuery(conn, query)

  if (!show_temporary) {
    tables <- tables |>
      dplyr::filter(!.data$is_temporary)
  }

  tables <- tables |>
    dplyr::select(!"is_temporary")


  if (!is.null(pattern)) {
    tables <- tables |>
      dplyr::mutate(db_table_str = ifelse(
        is.na(.data$schema), .data$table,
        paste(.data$schema, .data$table, sep = ".")
      )) |>
      dplyr::filter(grepl(pattern, .data$db_table_str)) |>
      dplyr::select(!"db_table_str")
  }

  if (nrow(tables) == 0) warning("No tables found. Check user privileges / database configuration")

  return(tables)
}

#' @importFrom rlang .data
#' @export
`get_tables.Microsoft SQL Server` <- function(conn, pattern = NULL, show_temporary = TRUE) {
  query <- paste("SELECT",
                 "s.name AS [schema],",
                 "t.name AS [table],",
                 "t.is_temporary",
                 "FROM (",
                 "SELECT *, 0 AS is_temporary FROM sys.tables WHERE NOT name  LIKE '#%'",
                 "UNION ALL",
                 "SELECT *, 1 AS is_temporary FROM tempdb.sys.tables WHERE name LIKE '#%'",
                 ") AS t",
                 "INNER JOIN sys.schemas AS s",
                 "ON t.schema_id = s.schema_id")

  tables <- DBI::dbGetQuery(conn, query)

  if (!show_temporary) {
    tables <- tables |>
      dplyr::filter(.data$is_temporary == 0)
  }

  # Filter out trailing underscores added by engine
  tables <- tables |>
    dplyr::mutate(table = stringr::str_remove(.data$table, "_+[0-9a-fA-F]+$"))

  tables <- tables |>
    dplyr::select(!"is_temporary")

  if (!is.null(pattern)) {
    tables <- tables |>
      tidyr::unite("db_table_str", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
      dplyr::filter(grepl(pattern, .data$db_table_str)) |>
      dplyr::select(!"db_table_str")
  }

  if (nrow(tables) == 0) warning("No tables found. Check user privileges / database configuration")

  return(tables)
}

#' @export
get_tables.OdbcConnection <- function(conn, pattern = NULL, show_temporary = TRUE) {
  query <- paste("SELECT",
                 "s.name AS [schema],",
                 "t.name AS [table]",
                 "FROM sys.tables t",
                 "INNER JOIN sys.schemas s",
                 "ON t.schema_id = s.schema_id")

  tables <- DBI::dbGetQuery(conn, query) |>
    dplyr::mutate(schema = dplyr::na_if(.data$schema, "dbo"))

  return(tables)
}

#' @export
get_tables.DBIConnection <- function(conn, pattern = NULL, show_temporary = TRUE) {
  if (isFALSE(show_temporary)) { # nocov start
    rlang::warn("show_temporary must be 'FALSE' for unsupported backends!") # nocov end
  }

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert_character(pattern, null.ok = TRUE)

  # Retrieve all objects in conn
  objs <- DBI::dbListObjects(conn) |>
    dplyr::select(table)

  # purrr::map fails if .x is empty, avoid by returning early
  if (nrow(objs) == 0) {
    warning("No tables found. Check user privileges / database configuration")

    return(data.frame(schema = character(), table = character()))
  }

  tables <- objs$table |> # For each top-level object (except tables)...
    purrr::map(\(.x) {
      if (names(.x@name) == "table") {
        return(data.frame(schema = NA_character_, table = .x@name["table"]))
      }

      # ...retrieve all tables
      DBI::dbListObjects(conn, .x) |>
        dplyr::pull(table) |>
        purrr::map(\(.y) data.frame(schema = .x@name, table = .y@name["table"])) |>
        purrr::reduce(rbind.data.frame)
    }) |>
    purrr::reduce(rbind.data.frame)

  # Skip dbplyr temporary tables
  tables <- dplyr::filter(tables, !startsWith(.data$table, "dbplyr_"))

  # Subset if pattern is given
  if (!is.null(pattern)) {
    tables <- subset(tables, grepl(pattern, table))
  }

  # Remove empty schemas
  tables <- dplyr::mutate(tables, schema = dplyr::if_else(.data$schema == "", NA, .data$schema))

  row.names(tables) <- NULL  # Reset row names
  return(tables)
}

#' Slices a data object based on time / date
#'
#' @template .data
#' @param slice_ts The time / date to slice by
#' @param from_ts  The name of the column in .data specifying valid from time (note: must be unquoted)
#' @param until_ts The name of the column in .data specifying valid until time (note: must be unquoted)
#' @template .data_return
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' m <- mtcars |>
#'   dplyr::mutate(from_ts = dplyr::if_else(dplyr::row_number() > 10,
#'                                          as.Date("2020-01-01"),
#'                                          as.Date("2021-01-01")),
#'                 until_ts = as.Date(NA))
#'
#' dplyr::copy_to(conn, m, name = "mtcars", temporary = FALSE)
#'
#' q <- dplyr::tbl(conn, id("mtcars", conn))
#'
#' nrow(slice_time(q, "2020-01-01")) # 10
#' nrow(slice_time(q, "2021-01-01")) # nrow(mtcars)
#'
#' close_connection(conn)
#' @export
slice_time <- function(.data, slice_ts, from_ts = from_ts, until_ts = until_ts) {

  # Check arguments
  assert_data_like(.data)
  assert_timestamp_like(slice_ts)

  from_ts  <- dplyr::enquo(from_ts)
  until_ts <- dplyr::enquo(until_ts)
  .data <- .data |>
    dplyr::filter(is.na({{until_ts}}) | slice_ts < {{until_ts}},
                  {{from_ts}} <= slice_ts)
  return(.data)
}

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
  checkmate::assert(DBI::dbIsValid(conn))
  assert_id_like(db_table_id)

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
      dplyr::filter(.data$table == db_table_id@name["table"], .data$schema == db_table_id@name["schema"])

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
