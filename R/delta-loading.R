#' Import and export a data-chunk with history from historical data
#' @name delta_loading
#' @description
#'   `delta_export()` exports data from tables created with `update_snapshot()`
#'   in chunks to allow for faster migration of data between sources.
#'
#'   `delta_load()` import deltas created by `delta_export()` to rebuild a
#'   historical table.
#'
#'   See `vignette("delta-loading")` for further introduction to the function.
#' @details
#'   This pair of functions is designed to facilitate easy migration or
#'   incremental backups of a historical table (created by `update_snapshot()`).
#'
#'   To construct the basis of incremental backups, `delta_export()` can be
#'   called with only `timestamp_from` at the desired frequency (weekly etc.)
#'
#'   To migrate a historical table in chunks, `delta_export()` can be
#'   called with `timestamp_until` to constrain the size of the delta.
#'
#'   In either case, the table can then be re-constructed by "replaying" the
#'   deltas with `delta_load()`.
#'   The order the deltas are replayed does not matter, but all have to be
#'   replayed to achieve the same state as the source table.
#' @template conn
#' @template db_table
#' @param timestamp_from (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The timestamp describing the start of the export (including).
#' @param timestamp_until (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The timestamp describing the end of the export (including).
#'
#'   If `NULL` (default), all history after `timestamp_from` is exported.
#' @return
#'   The lazy-query containing the data (and history) in the source to be used
#'   in conjunction with `delta_load()`.
#'
#'   This table is a temporary table that may need cleaning up.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- get_connection()
#'
#'   data <- dplyr::copy_to(conn, mtcars)
#'
#'   # Copy the first 3 records
#'   update_snapshot(
#'     head(data, 3),
#'     conn = conn,
#'     db_table = "test.mtcars",
#'     timestamp = "2020-01-01"
#'   )
#'
#'   # Create a delta with the current state
#'   delta <- delta_export(
#'     conn,
#'     db_table = "test.mtcars",
#'     timestamp_from = "2020-01-01"
#'   )
#'
#'   # Update with the first 5 records
#'   update_snapshot(
#'     head(data, 5),
#'     conn = conn,
#'     db_table = "test.mtcars",
#'     timestamp = "2021-01-01"
#'   )
#'
#'   dplyr::tbl(conn, "test.mtcars")
#'
#'   # Create a backup using the delta
#'   delta_load(
#'     conn = conn,
#'     db_table = "test.mtcars_backup",
#'     delta = delta
#'   )
#'
#'   dplyr::tbl(conn, "test.mtcars_backup")
#'
#'   close_connection(conn)
#' @seealso update_snapshot
#' @importFrom rlang .data
#' @export
delta_export <- function(
  conn,
  db_table,
  timestamp_from,
  timestamp_until = NULL
) {

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  assert_dbtable_like(db_table, len = 1, add = coll)
  assert_timestamp_like(timestamp_from, len = 1, add = coll)
  assert_timestamp_like(timestamp_until, null.ok = TRUE, len = 1, add = coll)
  checkmate::reportAssertions(coll)

  # Slice the table on the timestamps
  # If no `timestamp_until` is supplied, all history after `timestamp_from` is
  # exported.
  # If a `timestamp_until` is supplied, the exported history should only allow
  # the user to restore the data up until that point.

  # We take all records created or closed within the interval

  # Any future `until_ts` (i.e. greater than `timestamp_until`) is removed.

  # Consider this example data to illustrate the thought process:
  # (col, checksum,   from_ts, until_ts)
  # [NA,  <checksum>, 1,       2 ]
  # [A,   <checksum>, 2,       3 ]
  # [NA,  <checksum>, 3,       NA]

  # Lets apply `delta_export()` to each timestamp in the data which gets us
  # three deltas
  # delta_1:
  # [NA,  <checksum>, 1, NA] # added at ts = 1
  # delta_2:
  # [NA,  <checksum>, 1, 2 ] # closed at ts = 2
  # [A,   <checksum>, 2, NA] # added at ts = 2
  # delta_3:
  # [A,   <checksum>, 2, 3 ] # closed at ts = 3
  # [NA,  <checksum>, 3, NA] # added at ts = 3

  # If we apply in order
  # (dplyr::rows_patch() and rows_append() matched by checksum and from_ts),
  # we get the following
  # Applied: delta_1
  # [NA,  <checksum>, 1, NA]
  # Applied: delta_1, delta_2
  # [NA,  <checksum>, 1, 2 ]
  # [A,   <checksum>, 2, NA]
  # Applied: delta_1, delta_2, delta_3
  # [NA,  <checksum>, 1, 2 ]
  # [A,   <checksum>, 2, 3 ]
  # [NA,  <checksum>, 3, NA]

  # If we apply out of order (starting from delta_1)
  # (dplyr::rows_patch() and rows_append() matched by checksum and from_ts),
  # Applied: delta_1
  # [NA,  <checksum>, 1, NA]
  # Applied: delta_1, delta_3
  # [NA,  <checksum>, 1, NA] # Notice that we are currently in a broken state
  # [A,   <checksum>, 2, 3 ]
  # [NA,  <checksum>, 3, NA]

  # Applied: delta_1, delta_3, delta_2
  # [NA,  <checksum>, 1, 2 ] # Now we recover a working state
  # [A,   <checksum>, 2, 3 ]
  # [NA,  <checksum>, 3, NA]


  # Starting from the full table,
  out <- get_table(conn, db_table, slice_ts = NULL) %>%
    dplyr::filter(
      # ..any data created within the interval gets exported
      ((!!timestamp_from <= .data$from_ts) & (!!is.null(timestamp_until) | .data$from_ts <= !!timestamp_until)) |
        # and any data that expires within the interval gets exported
        ((!!timestamp_from <= .data$until_ts) & (!!is.null(timestamp_until) | .data$until_ts <= !!timestamp_until))
    )

  # Censor future until_ts values
  if (!is.null(timestamp_until)) {
    out <- out %>%
      dplyr::mutate(
        "until_ts" = dplyr::if_else(
          condition = !is.na(.data$until_ts) & !!timestamp_until < .data$until_ts,
          true = NA,
          false = .data$until_ts
        )
      )
  }

  # Store the computation
  out <- dplyr::compute(out, name = unique_table_name("SCDB_delta"))

  # Store metadata on the delta
  attr(out, "timestamp_from")  <- timestamp_from

  return(out)
}


#' @rdname delta_loading
#' @param delta .data (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\cr
#'   A "delta" exported from `delta_export()` to load.
#' @template logger
#' @export
delta_load <- function(
  conn,
  db_table,
  delta,
  logger = NULL
) {
  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  assert_dbtable_like(db_table, len = 1, add = coll)
  checkmate::assert_multi_class(logger, "Logger", null.ok = TRUE, add = coll)
  checkmate::reportAssertions(coll)

  # Check if the target and delta share connection
  if (identical(conn, dbplyr::remote_con(delta))) {

    delta_src <- delta # On the same connection, we use to input as is

  } else {

    # Copy delta to target (if needed)
    delta_src <- dplyr::copy_to(
      conn,
      delta,
      name = unique_table_name("SCDB_delta")
    )
    defer_db_cleanup(delta_src)

    # Recompute checksums on new connection
    delta_src <- digest_to_checksum(
      delta_src,
      col = "checksum",
      exclude = c("checksum", "from_ts", "until_ts"),
      warn = FALSE
    )
  }

  # Construct id for target table
  db_table_id <- id(db_table, conn)

  # Lock the table
  if (lock_table(conn, db_table_id)) {

    # Apply the delta to the target table
    if (!table_exists(conn, db_table_id)) {

      # Create table and apply delta
      delta_src %>%
        utils::head(0) %>%
        dplyr::collect() %>%
        dplyr::select(!c("checksum", "from_ts", "until_ts")) %>%
        create_table(conn = conn, db_table = db_table_id)

    }

    # Identify existing records
    existing <- dplyr::tbl(conn, db_table_id) %>%
      dplyr::select(dplyr::all_of((c("checksum", "from_ts", "until_ts")))) %>%
      dplyr::compute(name = unique_table_name("SCDB_delta_existing"))
    defer_db_cleanup(existing)

    # Patch existing records
    deactivations <- dplyr::anti_join(
      x = dplyr::filter(delta_src, !is.na(.data$until_ts)), # deactivations must have until_ts
      y = existing,
      by = c("checksum", "from_ts", "until_ts")
    )

    dplyr::rows_patch(
      x = dplyr::tbl(conn, db_table_id),
      y = deactivations,
      by = c("checksum", "from_ts"),
      in_place = TRUE,
      unmatched = "ignore"
    )

    # Add new records
    insertions <- dplyr::anti_join(
      delta_src,
      existing,
      by = c("checksum", "from_ts")
    )

    dplyr::rows_append(
      x = dplyr::tbl(conn, db_table_id),
      y = insertions,
      in_place = TRUE
    )

    # Release the lock
    unlock_table(conn, db_table_id)

    if (!is.null(logger)) {

      # Compute insertions and deactivations
      insertions <- insertions %>%
        dplyr::count("ts" = .data$from_ts, name = "n_insertions")

      deactivations <- deactivations %>%
        dplyr::filter(!is.na(.data$until_ts)) %>%
        dplyr::count("ts" = .data$until_ts, name = "n_deactivations")

      updates <- dplyr::full_join(
        insertions,
        deactivations,
        by = "ts"
      ) %>%
        dplyr::collect()

      # Update the logs
      updates %>%
        purrr::pwalk(
          \(ts, n_insertions, n_deactivations) {
            logger$set_timestamp(ts)
            logger$log_to_db(
              n_insertions = !!ifelse(is.na(n_insertions), 0, n_insertions),
              n_deactivations = !!ifelse(is.na(n_deactivations), 0, n_deactivations),
              message = !!glue::glue(
                "Update via delta load (timestamp_from = {attr(delta, \"timestamp_from\")})"
              )
            )
            logger$finalize_db_entry()
          }
        )
    }

  } else {

    stop(glue::glue("Failed to achieve lock on table ({db_table_id}) -- delta not applied!"))

  }
}
