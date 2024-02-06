#' Sets, queries and removes locks for db tables
#'
#' @name db_locks
#' @description
#' This set of function adds a simple locking system to db tables.
#' * `add_table_lock` adds a record in the target_schema.lock table with the current time and R-session process id.
#' * `remove_table_lock` removes records in the target_schema.lock table with the target table and the
#'    R-session process id.
#' * `is_lock_owner` returns TRUE if the current process id (pid) matches the pid associated with the lock on db_table
#'    in target_schema.lock. If no lock is found, NULL is returned.
#' * `remove_expired_locks` removes locks that are timed out.
#' @param conn `r rd_conn()`
#' @param db_table (`character(1)`)\cr
#'   A specification of 'schema.table'.
#' @param schema `r rd_schema()`
#' @return Most return `r rd_side_effects`. `is_lock_owner` returns the TRUE if the process can modify the table.
#' @examples
#' conn <- DBI::dbConnect(RSQLite::SQLite())
#'
#' is_lock_owner(conn, "test_table") # NULL
#'
#' add_table_lock(conn, "test_table")
#' is_lock_owner(conn, "test_table") # TRUE
#'
#' remove_table_lock(conn, "test_table")
#' is_lock_owner(conn, "test_table") # NULL
#'
#' DBI::dbDisconnect(conn)
#' @noRd
add_table_lock <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(schema, "locks", sep = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    suppressMessages(
      dplyr::copy_to(conn,
                     data.frame("db_table" = character(0),
                                "lock_start" = numeric(0),
                                "pid" = numeric(0)),
                     lock_table_id, temporary = FALSE, unique_indexes = "db_table")
    )
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id, check_from = FALSE)

  # We first delete old locks.
  remove_expired_locks(conn, schema)

  # We then try to insert a lock, if none exists, our process ID (pid) will be assigned to the table
  # if one already exists, our insert will fail.
  tryCatch(
    {
      lock <- dplyr::copy_to(
        conn,
        data.frame("db_table" = db_table, "pid" = Sys.getpid(), "lock_start" = as.numeric(Sys.time())),
        name = paste0("ds_lock_", Sys.getpid()),
        overwrite = TRUE
      )

      dplyr::rows_insert(lock_table, lock, by = "db_table", conflict = "ignore", in_place = TRUE)

    },
    error = function(e) {
      print(e$message)
    }
  )

  return(NULL)
}


#' @rdname db_locks
#' @noRd
remove_table_lock <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(schema, "locks", sep = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return(NULL)
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id, check_from = FALSE)

  # Delete locks matching  our process ID (pid) and the given db_table
  tryCatch(
    {
      lock <- dplyr::copy_to(
        conn,
        data.frame("db_table" = db_table, "pid" = Sys.getpid()),
        name = paste0("ds_lock_", Sys.getpid()),
        overwrite = TRUE
      )

      dplyr::rows_delete(lock_table, lock, by = c("db_table", "pid"), unmatched = "ignore", in_place = TRUE)

    },
    error = function(e) {
      print(e$message)
    }
  )

  return(NULL)
}


#' @rdname db_locks
#' @noRd
is_lock_owner <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(schema, "locks", sep = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return(NULL)
  }

  # Get a reference to the table
  lock_owner <- dplyr::tbl(conn, lock_table_id, check_from = FALSE) |>
    dplyr::filter(.data$db_table == !!db_table) |>
    dplyr::pull("pid") |>
    as.integer()

  return(lock_owner == Sys.getpid())
}


#' @rdname db_locks
#' @importFrom rlang .data
#' @noRd
remove_expired_locks <- function(conn, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(schema, "locks", sep = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return(NULL)
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id, check_from = FALSE)

  # Detect and delete old locks
  old_locks <- lock_table |>
    dplyr::filter(.data$lock_start < !!as.numeric(Sys.time()) - !!diseasyoption("lock_wait_max")) |>
    dplyr::select("db_table")
  dplyr::rows_delete(lock_table, old_locks, by = "db_table", unmatched = "ignore", in_place = TRUE)

  return(NULL)
}
