#' Sets, queries and removes locks for db tables
#'
#' @name db_locks
#' @description
#'   This set of function adds a simple locking system to db tables.
#'   * `add_table_lock()` adds a record in the schema.locks table with the current time and R-session process id.
#'   * `remove_table_lock()` removes records in the schema.locks table with the target table and the
#'      R-session process id.
#'   * `is_lock_owner()` returns `TRUE` if the current process id (pid) matches the pid associated with the lock on
#'      db_table in schema.locks. If no lock is found, `NULL` is returned.
#' @template conn
#' @param db_table (`character(1)`)\cr
#'   A specification of "schema.table" to modify lock for.
#' @param schema (`character(1)`)\cr
#'   The schema where the "locks" table should be created.
#' @return
#'   Most have return value (called for side effects).
#'   `is_lock_owner()` returns the TRUE if the process can modify the table.
#' @examples
#'   conn <- DBI::dbConnect(RSQLite::SQLite())
#'
#'   is_lock_owner(conn, "test_table") # NULL
#'
#'   add_table_lock(conn, "test_table")
#'   is_lock_owner(conn, "test_table") # TRUE
#'
#'   remove_table_lock(conn, "test_table")
#'   is_lock_owner(conn, "test_table") # NULL
#'
#'   DBI::dbDisconnect(conn)
#' @export
add_table_lock <- function(conn, db_table, schema = NULL) {
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table)
  checkmate::assert_character(schema, null.ok = TRUE)

  # Determine lock table id
  lock_table_id <- id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!table_exists(conn, lock_table_id)) {
    suppressMessages(
      dplyr::copy_to(conn,
                     data.frame("schema" = character(0),
                                "table" = character(0),
                                "lock_start" = numeric(0),
                                "pid" = numeric(0)),
                     lock_table_id, temporary = FALSE)
    )
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id, check_from = FALSE)

  # We then try to insert a lock, if none exists, our process ID (pid) will be assigned to the table-
  # If one already exists, our insert will fail.
  tryCatch(
    {
      db_table_id <- id(db_table, conn)

      lock <- dplyr::copy_to(
        conn,
        data.frame(
          "schema" = purrr::pluck(db_table_id, "name", "schema"),
          "table" = purrr::pluck(db_table_id, "name", "table"),
          "pid" = Sys.getpid(),
          "lock_start" = as.numeric(Sys.time())
        ),
        name = unique_table_name()
      )

      dplyr::rows_insert(lock_table, lock, by = c("schema", "table"), conflict = "ignore", in_place = TRUE)

    },
    error = function(e) {
      print(e$message)
    }
  )

  return(NULL)
}


#' @rdname db_locks
#' @export
remove_table_lock <- function(conn, db_table, schema = NULL) {
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table)
  checkmate::assert_character(schema, null.ok = TRUE)

  # Determine lock table id
  lock_table_id <- id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!table_exists(conn, lock_table_id)) {
    return(NULL)
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id, check_from = FALSE)

  # Delete locks matching  our process ID (pid) and the given db_table
  tryCatch(
    {
      db_table_id <- id(db_table, conn)

      lock <- dplyr::copy_to(
        conn,
        data.frame(
          "schema" = purrr::pluck(db_table_id, "name", "schema"),
          "table" = purrr::pluck(db_table_id, "name", "table"),
          "pid" = Sys.getpid()
        ),
        name = unique_table_name()
      )

      dplyr::rows_delete(lock_table, lock, by = c("schema", "table", "pid"), unmatched = "ignore", in_place = TRUE)

    },
    error = function(e) {
      print(e$message)
    }
  )

  return(NULL)
}


#' @rdname db_locks
#' @export
is_lock_owner <- function(conn, db_table, schema = NULL) {
  checkmate::assert_class(conn, "DBIConnection")
  assert_id_like(db_table)
  checkmate::assert_character(schema, null.ok = TRUE)

  # Determine lock table id
  lock_table_id <- id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!table_exists(conn, lock_table_id)) {
    return(NULL)
  }

  # Get a reference to the table
  db_table_id <- id(db_table, conn)
  lock_owner <- dplyr::tbl(conn, lock_table_id, check_from = FALSE) |>
    dplyr::filter(.data$schema == purrr::pluck(db_table_id, "name", "schema"),
                  .data$table  == purrr::pluck(db_table_id, "name", "table")) |>
    dplyr::pull("pid")


  # Return early if we own the lock
  if (identical(as.integer(lock_owner), Sys.getpid())) {
    return(TRUE)
  }

  # If we don't, check if the owner is still active
  if (!identical(lock_owner, numeric(0))) {

    ## Detect stale lock
    # Attempt to get the un-exported pid_exists() from parallelly
    pid_exists <- tryCatch(
      utils::getFromNamespace("pid_exists", "parallelly"),
      error = function(e) FALSE
    )

    # If pid_exists is not available we cannot determine invalid locks and we throw an error to prevent infinite looping
    checkmate::assert_function(pid_exists)

    stopifnot("Lock owner is no longer a valid PID (process likely crashed before completing)!"=pid_exists(lock_owner))

  }

  return(FALSE)
}
