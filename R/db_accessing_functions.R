#' Opens connection to the database
#'
#' Connects to the specified dbname of host:port using user and password from given arguments.
#' Certain drivers may use credentials stored in a file, such as ~/.pgpass (PostgreSQL)
#'
#' @param drv
#'   An object that inherits from DBIDriver or an existing DBIConnection (default: RPostgres::Postgres())
#' @param host
#'   Character string giving the ip of the host to connect to
#' @param port
#'   Host port to connect to (numeric)
#' @param dbname
#'   Name of the database located at the host
#' @param user
#'   Username to login with
#' @param password
#'   Password to login with
#' @param timezone
#'   Sets the timezone of DBI::dbConnect()
#' @param timezone_out
#'   Sets the timezone_out of DBI::dbConnect()
#' @param ...
#'  Additional parameters sent to DBI::dbConnect()
#' @return
#'   An object that inherits from DBIConnection driver specified in drv
#' @examples
#' conn <- get_connection()
#'
#' close_connection(conn)
#'
#' @seealso [RPostgres::Postgres]
#' @export
get_connection <- function(drv = RPostgres::Postgres(),
                           host = NULL,
                           port = NULL,
                           dbname = NULL,
                           user = NULL,
                           password = NULL,
                           timezone = NULL,
                           timezone_out = NULL,
                           ...) {

  # Check arguments
  checkmate::assert_character(host, pattern = r"{^\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}$}", null.ok = TRUE)
  checkmate::assert_numeric(port, null.ok = TRUE)
  checkmate::assert_character(dbname,   null.ok = TRUE)
  checkmate::assert_character(user,     null.ok = TRUE)
  checkmate::assert_character(password, null.ok = TRUE)
  checkmate::assert_character(timezone, null.ok = TRUE)
  checkmate::assert_character(timezone_out, null.ok = TRUE)

  # Set PostgreSQL-specific options
  if (inherits(drv, "PqDriver")) {
    if (is.null(timezone)) timezone <- Sys.timezone()
    if (is.null(timezone_out)) timezone_out <- timezone
  }

  # Default SQLite connections to temporary on-disk database
  if (inherits(drv, "SQLiteDriver") && is.null(dbname)) dbname <- ""

  # Check if connection can be established given these settings
  can_connect <- DBI::dbCanConnect(drv = drv, ...)
  if (!can_connect) stop("Could not connect to database with the given parameters: ", attr(can_connect, "reason"))

  conn <- DBI::dbConnect(drv = drv,
                         dbname = dbname,
                         host = host,
                         port = port,
                         user = user,
                         password = password,
                         timezone = timezone,
                         timezone_out = timezone_out,
                         ...,
                         bigint = "integer", # R has poor integer64 integration, which is the default return
                         check_interrupts = TRUE)

  # Check connection
  if (nrow(get_tables(conn)) == 0) {
    warn_str <- "No tables found. Check user permissions / database configuration"
    args <- c(as.list(environment()), list(...))
    set_args <- args[names(args) %in% c("dbname", "host", "port", "user", "password")]

    if (length(set_args) > 0) {
      warn_str <- paste0(warn_str, ":\n  ")
      warn_str <- paste0(warn_str, paste(names(set_args), set_args, sep = ": ", collapse = "\n  "))
    }
    warning(warn_str)
  }

  return(conn)
}

#' Gets the available tables
#'
#' @template conn
#' @param pattern A regex pattern with which to subset the returned tables
#' @return A data.frame containing table names in the DB
#' @examples
#' conn <- get_connection()
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

  tables <- purrr::map(
    # For each top-level object (except tables)...
    objs$table, \(.x) {
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
    tables <- dplyr::filter(tables, dplyr::case_when(
      is.na(schema) ~ TRUE,
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


#' Get the current schema of a DB connection
#'
#' @param .x A DBIConnection or lazy_query object
#' @return The current schema name, but defaults to "prod" instead of "public"
#' @examples
#' conn <- get_connection()
#'
#' get_schema(conn)
#' get_schema(get_table(conn, 'prod.basis_samples'))
#'
#' close_connection(conn)
#' @export
get_schema <- function(.x) {

  if (inherits(.x, "PqConnection")) {
    # Get schema from connection object
    schema <- DBI::dbGetQuery(.x, "SELECT CURRENT_SCHEMA()")$current_schema

  } else if (inherits(.x, "SQLiteConnection")) {
    return(NULL)
  } else if (inherits(.x, "tbl_dbi")) {
    # Get schema from a DBI object (e.g. lazy query)
    schema <- stringr::str_extract_all(dbplyr::remote_query(.x), '(?<=FROM \")[^"]*')[[1]]
    if (length(unique(schema)) > 1) {
      # Not sure if this is even possible due to dbplyr limitations
      warning("Multiple different schemas detected. You might need to handle these (more) manually:\n",
              paste(unique(schema), collapse = ", "))
    } else {
      schema <- unique(schema)
    }
  } else {
    stop("Could not detect object type")
  }

  if (schema == "public") schema <- "prod"

  return(schema)
}


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
#' conn <- get_connection()
#'
#' get_table(conn)
#' get_table(conn, "prod.covid_19_wgs")
#' get_table(conn, "prod.covid_19_wgs", slice_ts = "2022-01-01", include_slice_info = TRUE)
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
    print("Select one the following tables:")
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


# TODO: id as S3 vs in-function
#' Convenience function for DBI::Id
#'
#' @template db_table_id
#' @template conn
#' @seealso [DBI::Id] which this function wraps.
#' @export
id <- function(db_table_id, conn = NULL) {

  # Check if already Id
  if (inherits(db_table_id, "Id")) return(db_table_id)

  # Check arguments
  checkmate::assert_character(db_table_id)

  # SQLite does not have schemas
  if (inherits(conn, "SQLiteConnection")) {
    return(DBI::Id(table = db_table_id))
  }

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split_1(db_table_id, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]
  } else {
    db_schema <- NULL
    db_table <- db_table_id
  }

  DBI::Id(schema = db_schema, table = db_table)
}

#' Check if table exists in db
#'
#' @template conn
#' @template db_table_id
#' @examples
#' conn <- get_connection()
#'
#' table_exists(conn, "prod.basis_samples") # TRUE
#' table_exists(conn, "data.basis_samples") # FALSE
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


#' Close connection to the DB
#'
#' @template conn
#' @export
close_connection <- function(conn) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")

  if (DBI::dbIsValid(conn)) DBI::dbDisconnect(conn)
}
