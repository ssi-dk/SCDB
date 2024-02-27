#' Opens connection to the database
#'
#' @description
#'   This is a convenience wrapper for DBI::dbConnect() for different database backends.
#'
#'   Connects to the specified dbname of host:port using user and password from given arguments (if applicable).
#'   Certain drivers may use credentials stored in a file, such as ~/.pgpass (PostgreSQL).
#' @param drv (`DBIDriver(1)` or `DBIConnection(1)`)\cr
#'   The driver for the connection.
#' @param dbname (`character(1)`)\cr
#'   Name of the database located at the host.
#' @param bigint (`character(1)`)\cr
#'   The datatype to convert integers to.
#'   Support depends on the database backend.
#' @param timezone (`character(1)`)\cr
#'   Sets the timezone of DBI::dbConnect(). Must be in [OlsonNames()].
#' @param timezone_out (`character(1)`)\cr
#'   Sets the timezone_out of DBI::dbConnect(). Must be in [OlsonNames()].
#' @param ...
#'  Additional parameters sent to DBI::dbConnect().
#' @return
#'   An object that inherits from `DBIConnection` driver specified in `drv`.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection(drv = RSQLite::SQLite(), dbname = ":memory:")
#'
#'   DBI::dbIsValid(conn) # TRUE
#'
#'   close_connection(conn)
#'
#'   DBI::dbIsValid(conn) # FALSE
#' @seealso [RPostgres::Postgres]
#' @export
get_connection <- function(drv = RPostgres::Postgres(), ...) {
  UseMethod("get_connection")
}

#' @rdname get_connection
#' @seealso [RSQLite::SQLite]
#' @export
get_connection.SQLiteDriver <- function(
    drv,
    dbname = ":memory:",
    ...,
    bigint = c("integer", "bigint64", "numeric", "character")) {

  # Resolve the bigint argument (if not set, first of default vector is used)
  bigint <- match.arg(bigint)

  # Store the given arguments
  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()
  args <- args[match(unique(names(args)), names(args))]

  # Check arguments
  checkmate::assert_character(dbname, null.ok = TRUE)

  # Check if connection can be established given these settings
  status <- do.call(DBI::dbCanConnect, args = args)
  if (!status) stop(attr(status, "reason"))

  return(do.call(DBI::dbConnect, args = args))
}

#' @rdname get_connection
#' @param host (`character(1)`)\cr
#'   The ip of the host to connect to.
#' @param port (`numeric(1)` or `character(1)`)\cr
#'   Host port to connect to.
#' @param password (`character(1)`)\cr
#'   Password to login with.
#' @param user (`character(1)`)\cr
#'   Username to login with.
#' @param check_interrupts (`logical(1)`)\cr
#'   Should user interrupts be checked during the query execution?
#' @seealso [RPostgres::Postgres]
#' @export
get_connection.PqDriver <- function(
    drv,
    dbname = NULL,
    host = NULL,
    port = NULL,
    password = NULL,
    user = NULL,
    ...,
    bigint = c("integer", "bigint64", "numeric", "character"),
    check_interrupts = TRUE,
    timezone = Sys.timezone(),
    timezone_out = Sys.timezone()) {

  # Resolve the bigint argument (if not set, first of default vector is used)
  bigint <- match.arg(bigint)

  # Store the given arguments
  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()
  args <- args[match(unique(names(args)), names(args))]

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_character(dbname, null.ok = TRUE, add = coll)
  checkmate::assert_character(host, null.ok = TRUE, add = coll)
  if (is.character(port)) {
    checkmate::assert_character(port, pattern = "^[:digit:]*$", add = coll)
    port <- as.numeric(port)
  }
  checkmate::assert_numeric(port, null.ok = TRUE, add = coll)
  checkmate::assert_character(password, null.ok = TRUE, add = coll)
  checkmate::assert_character(user, null.ok = TRUE, add = coll)
  checkmate::assert_logical(check_interrupts, add = coll)
  checkmate::assert_choice(timezone, OlsonNames(), null.ok = TRUE, add = coll)
  checkmate::assert_choice(timezone_out, OlsonNames(), null.ok = TRUE, add = coll)
  checkmate::reportAssertions(coll)

  # Check if connection can be established given these settings
  status <- do.call(DBI::dbCanConnect, args = args)
  if (!status) stop(attr(status, "reason"))

  return(do.call(DBI::dbConnect, args = args))
}

#' @rdname get_connection
#' @param dsn (`character(1)`)\cr
#'   The data source name to connect to.
#' @seealso [odbc::odbc]
#' @export
get_connection.OdbcDriver <- function(
    drv,
    dsn = NULL,
    ...,
    bigint = c("integer", "bigint64", "numeric", "character"),
    timezone = Sys.timezone(),
    timezone_out = Sys.timezone()) {

  # Resolve the bigint argument (if not set, first of default vector is used)
  bigint <- match.arg(bigint)

  # Store the given arguments
  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()
  args <- args[match(unique(names(args)), names(args))]

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_character(dsn, null.ok = TRUE, add = coll)
  checkmate::assert_choice(timezone, OlsonNames(), null.ok = TRUE, add = coll)
  checkmate::assert_choice(timezone_out, OlsonNames(), null.ok = TRUE, add = coll)
  checkmate::reportAssertions(coll)

  # Check if connection can be established given these settings
  status <- do.call(DBI::dbCanConnect, args = args)
  if (!status) stop(attr(status, "reason"))

  return(do.call(DBI::dbConnect, args = args))
}

#' @rdname get_connection
#' @param dbdir (`character(1)`)\cr
#'   The directory where the database is located.
#' @seealso [duckdb::duckdb]
#' @export
get_connection.duckdb_driver <- function(
    drv,
    dbdir = ":memory:",
    ...,
    bigint = c("numeric", "character"),
    timezone_out = Sys.timezone()) {

  # Resolve the bigint argument (if not set, first of default vector is used)
  bigint <- match.arg(bigint)

  # Store the given arguments
  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()
  args <- args[match(unique(names(args)), names(args))]

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert(
    checkmate::check_path_for_output(dbdir),
    checkmate::check_character(dbdir, pattern = ":memory:", fixed = TRUE),
    add = coll
  )
  checkmate::assert_choice(timezone_out, OlsonNames(), null.ok = TRUE)
  checkmate::reportAssertions(coll)

  # Connect, don't check if connection can be established
  # (we are getting errors when testing for connectability first)
  return(do.call(DBI::dbConnect, args = args))
}

#' @rdname get_connection
#' @export
get_connection.default <- function(drv, ...) {

  # Store the given arguments
  args <- list(...) |>
    append(as.list(rlang::current_env())) |>
    unlist()
  args <- args[match(unique(names(args)), names(args))]

  # Check if connection can be established given these settings
  status <- do.call(DBI::dbCanConnect, args = args)
  if (!status) stop(attr(status, "reason"))

  conn <- do.call(DBI::dbConnect, args = args)

  warning(
    "Connections of class '", class(conn),
    "' is currently not formally supported and SCDB may not perform as expected."
  )

  return(conn)
}


#' Close connection to the DB
#'
#' @template conn
#' @inherit DBI::dbDisconnect return
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection(drv = RSQLite::SQLite())
#'
#'   close_connection(conn)
#' @export
close_connection <- function(conn) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")

  DBI::dbDisconnect(conn, shutdown = TRUE)
}
