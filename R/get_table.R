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
#'   A "lazy" dataframe (tbl_lazy) generated using dbplyr
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' dplyr::copy_to(conn, mtcars, name = "mtcars")
#'
#' get_table(conn)
#' get_table(conn, "mtcars")
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
  q <- dplyr::tbl(conn, db_table_id)

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
#' @return A data.frame containing table names in the DB
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' get_tables(conn)
#'
#' close_connection(conn)
#' @importFrom rlang .data
#' @export
get_tables <- function(conn, pattern = NULL) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert_character(pattern, null.ok = TRUE)

  # Retrieve all objects in conn
  objs <- DBI::dbListObjects(conn) |>
    dplyr::select(table)

  # purrr::map fails if .x is empty, avoid by returning early
  if (nrow(objs) == 0) return(data.frame(schema = character(), table = character()))

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
  assert_id_like(db_table_id)

  if (inherits(db_table_id, "Id")) {
    db_name <- attr(db_table_id, "name")
    db_schema <- purrr::pluck(db_name, "schema", .default = NA_character_)
    db_table  <- purrr::pluck(db_name, "table")
    db_table_id <- paste0(purrr::discard(c(db_schema, db_table), is.na), collapse = ".")
  }

  # Determine matches in the existing tables
  n_matches <- get_tables(conn) |>
    tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE) |>
    dplyr::filter(db_table_id == !!db_table_id) |>
    nrow()

  if (n_matches >= 2) stop("Edge case detected. Cannot determine if table exists!")
  return(n_matches == 1)
}
