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
#' \dontrun{
#' close_connection(conn)
#' }
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

  return(conn)
}


#' Close connection to the DB
#'
#' @template conn
#' @export
close_connection <- function(conn) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")

  DBI::dbDisconnect(conn)
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
