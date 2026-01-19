
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

  # Censor future until_ts values when in "batch" mode
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

  # Store meta data on the delta
  attr(out, "timestamp_from")  <- timestamp_from

  return(out)
}


#' @rdname delta_loading
#' @param delta .data (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\cr
#'   A "delta" exported from `delta_export()` to load.
#'
#'   A list of deltas can also be supplied to be applied.
#' @export
delta_load <- function(
  conn,
  db_table,
  delta
) {

  # If a list of deltas is given, load each element
  if (inherits(delta, "list")) {
    purrr::walk(delta, ~ delta_load(conn, db_table, delta = .))
  }

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  assert_dbtable_like(db_table, len = 1, add = coll)
  checkmate::reportAssertions(coll)

  # Mark the current time
  tic <- Sys.time()

  # Check if the target and delta share connection
  if (!identical(conn, dbplyr::remote_con(delta))) {

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

  } else {

    # On the same connection, we use to input as is
    delta_src <- delta

  }

  # Construct id for target table
  db_table_id <- id(db_table, conn)

  # Lock the table
  lock_table(conn, db_table_id)

  # Apply the delta to the target table
  if (!table_exists(conn, db_table_id)) {

    # Create table and apply delta
    delta_src %>%
      utils::head(0) %>%
      dplyr::collect() %>%
      dplyr::select(!c("checksum", "from_ts", "until_ts")) %>%
      create_table(conn = conn, db_table = db_table_id)

  }

  #print(dplyr::tbl(conn, db_table_id))

  # Identify existing records
  existing <- dplyr::tbl(conn, db_table_id) %>%
    dplyr::select(dplyr::all_of((c("checksum", "from_ts")))) %>%
    dplyr::compute(name = unique_table_name("SCDB_delta_existing"))
  defer_db_cleanup(existing)

  # Patch existing records
  dplyr::rows_patch(
    x = dplyr::tbl(conn, db_table_id),
    y = dplyr::inner_join(
      x = existing,
      y = delta_src,
      by = c("checksum", "from_ts")
    ),
    by = c("checksum", "from_ts"),
    in_place = TRUE,
    unmatched = "ignore"
  )

  # Add new records
  dplyr::rows_append(
    x = dplyr::tbl(conn, db_table_id),
    y = dplyr::anti_join(delta_src, existing, by = c("checksum", "from_ts")),
    in_place = TRUE
  )

  # Release the lock
  unlock_table(conn, db_table_id)

  # Update the logs
  delta_src |>
    dplyr::count(.data$from_ts, .data$until_ts)
  #log_tbl <- create_logs_if_missing(conn, log_table_id)
}
