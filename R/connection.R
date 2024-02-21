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

  if (!checkmate::test_choice(class(conn), .supported)) {
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
