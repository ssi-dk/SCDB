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
#'   Host port to connect to. Must be a number or a numeric string.
#' @param dbname
#'   Name of the database located at the host
#' @param user
#'   Username to login with
#' @param password
#'   Password to login with
#' @param timezone
#'   Sets the timezone of DBI::dbConnect(). Must be in [OlsonNames()].
#' @param timezone_out
#'   Sets the timezone_out of DBI::dbConnect(). Must be in [OlsonNames()].
#' @param ...
#'  Additional parameters sent to DBI::dbConnect()
#' @inheritParams RPostgres::dbConnect_PqDriver
#' @return
#'   An object that inherits from DBIConnection driver specified in drv
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#' conn <- get_connection(drv = RSQLite::SQLite(), dbname = ":memory:")
#'
#' DBI::dbIsValid(conn) # TRUE
#'
#' close_connection(conn)
#'
#' DBI::dbIsValid(conn) # FALSE
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
                           ...,
                           bigint = "integer",
                           check_interrupts = TRUE) {

  # Check arguments
  checkmate::assert_character(host, null.ok = TRUE)
  if (is.character(port)) {
    checkmate::assert_character(port, pattern = "^[:digit:]*$")
    port <- as.numeric(port) # nocov
  }
  checkmate::assert_numeric(port, null.ok = TRUE)
  checkmate::assert_character(dbname,   null.ok = TRUE)
  checkmate::assert_character(user,     null.ok = TRUE)
  checkmate::assert_character(password, null.ok = TRUE)
  checkmate::assert_choice(timezone, OlsonNames(), null.ok = TRUE)
  checkmate::assert_choice(timezone_out, OlsonNames(), null.ok = TRUE)

  # Set PostgreSQL-specific options
  if (inherits(drv, "PqDriver")) {
    if (is.null(timezone)) timezone <- Sys.timezone()
    if (is.null(timezone_out)) timezone_out <- timezone
  }

  # Default SQLite connections to temporary on-disk database
  if (inherits(drv, "SQLiteDriver") && is.null(dbname)) dbname <- ""

  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()

  args <- args[match(unique(names(args)), names(args))]

  # Check if connection can be established given these settings
  status <- do.call(DBI::dbCanConnect, args = args)
  if (!status) rlang::abort(attr(status, "reason"))

  conn <- do.call(DBI::dbConnect, args = args)

  .supported <- c(
    "PqConnection",
    "SQLiteConnection",
    "Microsoft SQL Server"
  )

  if (!class(conn) %in% .supported) {
    warning("Connections of class '",
            class(conn),
            "' is currently not fully supported and SCDB may not perform as expected.")
  }

  return(conn)
}


#' Close connection to the DB
#'
#' @template conn
#' @inherit DBI::dbDisconnect return
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' close_connection(conn)
#' @export
close_connection <- function(conn) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")

  DBI::dbDisconnect(conn)
}


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
id <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {
  UseMethod("id")
}


#' @export
id.Id <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {
  return(DBI::Id(schema = purrr::pluck(db_table_id, "name", "schema", .default = SCDB::get_schema(conn)),
                 table = purrr::pluck(db_table_id, "name", "table")))
}


#' @export
id.character <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {

  checkmate::assert(is.null(conn), DBI::dbIsValid(conn), combine = "or")

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split_1(db_table_id, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]

    # If no matching implied schema is found, return the unmodified db_table_id in the default schema
    if (allow_table_only && !is.null(conn) && !schema_exists(conn, db_schema)) {
      return(DBI::Id(schema = get_schema(conn), table = db_table_id))
    }
  } else {
    db_schema <- get_schema(conn)
    db_table <- db_table_id
  }

  return(DBI::Id(schema = db_schema, table = db_table))
}


#' @export
id.tbl_dbi <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {

  # If table identification is fully qualified extract Id from remote_Table
  if (!is.na(purrr::pluck(dbplyr::remote_table(db_table_id), unclass, "schema"))) {

    table_ident <- dbplyr::remote_table(db_table_id) |>
      unclass() |>
      purrr::discard(is.na)

    table_conn <- dbplyr::remote_con(db_table_id)

    return(
      DBI::Id(
        catalog = purrr::pluck(table_ident, "catalog"),
        schema = purrr::pluck(table_ident, "schema", .default = get_schema(table_conn)),
        table = purrr::pluck(table_ident, "table")
      )
    )

  } else {

    # If not attempt to resolve the table from existing tables.
    # For SQLite, there should only be one table in main/temp matching the table
    # In some cases, tables may have been added to the DB that makes the id ambiguous.
    schema <- get_tables(dbplyr::remote_con(db_table_id), show_temporary = TRUE) |>
      dplyr::filter(.data$table == dbplyr::remote_name(db_table_id)) |>
      dplyr::pull("schema")

    if (length(schema) > 1)  {
      stop(
        "Table identification has been corrupted! ",
        "This table does not contain information about its schema and ",
        "multiple tables with this name were found across schemas."
      )
    }

    return(DBI::Id(schema = schema, table = dbplyr::remote_name(db_table_id)))
  }
}
