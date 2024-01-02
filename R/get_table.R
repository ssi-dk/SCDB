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
#' dplyr::copy_to(conn, mtcars, name = "mtcars")
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

  if (is.character(db_table_id)) db_table_id <- id(db_table_id, conn = conn)

  # Ensure existence of table
  if (!table_exists(conn, db_table_id)) {
    db_table_name_str <- paste(c(purrr::pluck(db_table_id, "name", "schema"),
                                 purrr::pluck(db_table_id, "name", "table")),
                               collapse = ".")
    stop(glue::glue("Table {db_table_name_str} is not found!"))
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
#' @param show_temp
#' A string specifying how to handle temporary tables.
#'
#' - `"never"`, the default, will never return any temporary tables.
#' - `"always"`, lists temporary tables as well as ordinary tables.
#'   Note that this may return duplicate tables.
#' - `"fallback"` lists temporary tables, but only if no ordinary table with the same name exists
#'
#' @return A data.frame containing table names in the DB
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, datasets::mtcars, name = DBI::Id(table = "my_test_table"))
#'
#' get_tables(conn, pattern = "my_[th]est")
#' get_tables(conn, pattern = "my_[th]est", show_temp = "always")
#'
#' close_connection(conn)
#' @importFrom rlang .data
#' @export
get_tables <- function(conn, pattern = NULL, show_temp = "never") {

  checkmate::check_character(pattern, null.ok = TRUE)
  checkmate::check_choice(show_temp, c("always", "fallback", "never"))

  UseMethod("get_tables")
}

#' @importFrom rlang .data
#' @export
get_tables.SQLiteConnection <- function(conn, pattern = NULL, show_temp = "never") {
  query <- paste("SELECT schema, name 'table' FROM pragma_table_list",
                 "WHERE NOT name IN ('sqlite_schema', 'sqlite_temp_schema')",
                 "AND NOT name LIKE 'sqlite_stat%'")

  tables <- DBI::dbGetQuery(conn, query)

  if (show_temp == "never") {
    tables <- tables |>
      dplyr::filter(.data$schema != "temp")
  }
  if (show_temp == "fallback") {
    tables <- tables |>
      dplyr::filter(
        .data$schema != "temp" | !.data$table %in% .data$table[.data$schema == "main"]
      )
  }
  if (show_temp != "always") {
    tables <- tables |>
      dplyr::mutate(schema = ifelse(.data$schema == "main", NA_character_, .data$schema))
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
get_tables.PqConnection <- function(conn, pattern = NULL, show_temp = "never") {
  query <- paste('SELECT schemaname "schema", tablename "table" FROM pg_tables',
                 "WHERE NOT (schemaname LIKE 'pg_%'",
                 "OR schemaname = 'information_schema'",
                 "OR tablename LIKE 'dbplyr_%')")

  default_schema <- get_schema(conn)
  tables <- DBI::dbGetQuery(conn, query) |>
    dplyr::mutate(is_temporary = stringr::str_detect(.data$schema, "^pg_temp_[[:digit:]]+$"))

  if (show_temp == "never") {
    tables <- tables |>
      dplyr::filter(!.data$is_temporary)
  }
  if (show_temp == "fallback") {
    tables <- tables |>
      dplyr::filter(
        !.data$is_temporary |
          !.data$table %in% .data$table[.data$schema == default_schema]
      )
  }
  if (show_temp != "always") {
    tables <- tables |>
      dplyr::mutate(schema = ifelse(.data$schema == default_schema | .data$is_temporary, NA_character_, .data$schema))
  }

  tables <- tables |>
    dplyr::mutate(schema = ifelse(.data$schema == default_schema, NA_character_, .data$schema)) |>
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
`get_tables.Microsoft SQL Server` <- function(conn, pattern = NULL, show_temp = "never") {
  if (show_temp != "never") {
    rlang::warn("Temporary tables are currently not supported for Microsoft SQL Server")
  }

  tables <- NextMethod("get_tables") |>
    dplyr::mutate(schema = dplyr::na_if(.data$schema, "dbo"))


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
get_tables.OdbcConnection <- function(conn, pattern = NULL, show_temp = "never") {
  query <- paste("SELECT",
                 "s.name AS [schema],",
                 "t.name AS [table]",
                 "FROM sys.tables t",
                 "INNER JOIN sys.schemas s",
                 "ON t.schema_id = s.schema_id")

  tables <- DBI::dbGetQuery(conn, query) |>
    tidyr::unite("db_table_str", "schema", "table", sep = ".", na.rm = TRUE)

  return(tables)
}

#' @export
get_tables.DBIConnection <- function(conn, pattern = NULL, show_temp = "never") {
  if (show_temp != "never") {
    rlang::warn("show_temp must be 'never' for unsupported backends!")
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

  # Skip PostgreSQL metadata tables
  if (inherits(conn, "PqConnection")) {
    tables <- dplyr::filter(tables,
                            dplyr::case_when(is.na(schema) ~ TRUE,
                                             .data$schema == "information_schema" ~ FALSE,
                                             grepl("^pg_.*", .data$schema) ~ FALSE,
                                             TRUE ~ TRUE))
  }

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
#' dplyr::copy_to(conn, m, name = "mtcars")
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
#' @template conn
#' @template db_table_id
#' @return TRUE if db_table_id can be parsed to a table found in conn
#' @importFrom rlang .data
#' @name table_exists
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars")
#'
#' table_exists(conn, "mtcars") # TRUE
#' table_exists(conn, "iris") # FALSE
#'
#' close_connection(conn)
#' @export
table_exists <- function(conn, db_table_id) {

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

  db_table_id <- id(db_table_id, conn = conn)

  # Add default schema if none could be found
  if (!is.null(conn) && !"schema" %in% names(db_table_id@name)) {
    db_table_id@name["schema"] <- get_schema(conn)
  }

  UseMethod("table_exists", conn)
}

#' @rdname table_exists
#' @importFrom rlang .data
#' @export
table_exists.SQLiteConnection <- function(conn, db_table_id) {
  tables <- get_tables(conn, show_temp = "fallback")

  if (inherits(db_table_id, "Id")) {
    exact_match <- dplyr::filter(tables, .data$table == db_table_id@name["table"])

    if ("schema" %in% names(db_table_id@name)) {
      exact_match <- dplyr::filter(exact_match, .data$schema == db_table_id@name["schema"])
    }

    if (nrow(exact_match) == 1) {
      return(TRUE)
    }

    db_table_id <- paste(db_table_id@name, collapse = ".")
  }

  matches <- tables |>
    dplyr::mutate(schema = ifelse(.data$schema == "main", NA_character_, .data$schema)) |>
    tidyr::unite("table_str", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
    dplyr::filter(.data$table_str == !!db_table_id) |>
    dplyr::select(!"table_str")

  if (nrow(matches) <= 1) {
    return(nrow(matches) == 1)
  }

  rlang::abort(
    message = paste0("More than one table matching '", db_table_id, "' was found!"),
    matches = matches
  )
}

#' @rdname table_exists
#' @importFrom rlang .data
#' @export
table_exists.DBIConnection <- function(conn, db_table_id) {
  assert_id_like(db_table_id)

  db_table_id <- id(db_table_id, conn = conn)

  if (inherits(db_table_id, "Id")) {
    db_name <- attr(db_table_id, "name")
    db_schema <- purrr::pluck(db_name, "schema", .default = NA_character_)
    db_table  <- purrr::pluck(db_name, "table")
    db_table_id <- paste0(purrr::discard(c(db_schema, db_table), is.na), collapse = ".")
  }

  # Determine matches in the existing tables
  n_matches <- get_tables(conn) |>
    dplyr::mutate(schema = ifelse(.data$schema == get_schema(conn), NA_character_, .data$schema)) |>
    tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE) |>
    dplyr::filter(db_table_id == !!db_table_id) |>
    nrow()

  if (n_matches > 1) stop("More than one table matching '", db_table_id, "' was found!")
  return(n_matches == 1)
}
