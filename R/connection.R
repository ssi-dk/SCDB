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
  if (inherits(db_table_id, "Id")) {
    return(DBI::Id(schema = purrr::pluck(db_table_id, "name", "schema", .default = SCDB::get_schema(conn)),
                   table = purrr::pluck(db_table_id, "name", "table")))
  }

  if (!allow_table_only && !checkmate::test_class(conn, "DBIConnection") && !DBI::dbIsValid(conn)) {
    stop("When allow_table_only is FALSE, a valid `conn` must be supplied!")
  }

  UseMethod("id")
}

#' @export
id.character <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split_1(db_table_id, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]

    # If no matching implied schema is found, return the unmodified db_table_id
    if (allow_table_only && !schema_exists(conn, db_schema)) {
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
  table_ident <- dbplyr::remote_table(db_table_id)

  id <- with(table_ident, {
    list(catalog = catalog,
         schema = schema,
         table = table) |>
      (\(.x) subset(.x, !is.na(.x)))() |>
      do.call(DBI::Id, args = _)
  })

  if (!is.null(conn) && !identical(conn, dbplyr::remote_con(db_table_id))) {
    rlang::warn("Table connection is different than conn")
  }

  return(id)
}
