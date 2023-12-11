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
                           ...) {

  # Check arguments
  checkmate::assert_character(host, pattern = r"{^\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}$}", null.ok = TRUE)
  checkmate::assert_numeric(port, null.ok = TRUE)
  checkmate::assert_character(dbname,   null.ok = TRUE)
  checkmate::assert_character(user,     null.ok = TRUE)
  checkmate::assert_character(password, null.ok = TRUE)
  checkmate::assert_character(timezone, null.ok = TRUE)
  checkmate::assert_character(timezone_out, null.ok = TRUE)

  .supported_drivers <- c(
    "PqDriver",
    "SQLiteDriver"
  )

  if (!class(drv) %in% .supported_drivers) {
    warning("Driver of class '", class(drv), "' is currently not fully supported and SCDB may not perform as expected.")
  }

  # Set PostgreSQL-specific options
  if (inherits(drv, "PqDriver")) {
    if (is.null(timezone)) timezone <- Sys.timezone()
    if (is.null(timezone_out)) timezone_out <- timezone
  }

  # Default SQLite connections to temporary on-disk database
  if (inherits(drv, "SQLiteDriver") && is.null(dbname)) dbname <- ""

  # Check if connection can be established given these settings
  tryCatch({
    args <- append(list(...), as.list(environment()))
    status <- do.call(DBI::dbCanConnect, args = args)

    if (!status) rlang::abort(attr(status, "reason"))
  })

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
#' @details The given db_table_id is parsed using an assumption of "schema.table" syntax into
#'  a DBI::Id object with corresponding schema (if the conn supports it) and table values.
#' @return A DBI::Id object parsed from db_table_id
#' @examples
#' id("schema.table")
#' @seealso [DBI::Id] which this function wraps.
#' @export
id <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {
  if (inherits(db_table_id, "Id")) {
    return(db_table_id)
  }

  if (!allow_table_only) checkmate::check_class(conn, "DBIConnection")

  UseMethod("id")
}

#' @export
id.character <- function(db_table_id, conn = NULL, allow_table_only = TRUE) {

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split_1(db_table_id, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]

    if (allow_table_only && !is.null(conn) && !schema_exists(conn, db_schema)) {
      return(DBI::Id(table = db_table_id))
    }
  } else {
    db_schema <- NULL
    db_table <- db_table_id
  }

  return(DBI::Id(schema = db_schema, table = db_table))
}

#' @importFrom rlang %||%
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
