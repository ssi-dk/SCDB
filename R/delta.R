
#' Import and export a data-chunk with history from historical data
#' @name delta_loading
#' @description
#'   `delta_export` exports data from tables created with `update_snapshot()` in
#'   chunks to allow for faster migration of data between sources.
#'
#'   See `vignette("delta-loading")` for further introduction to the function.
#' @template conn
#' @template db_table
#' @param timestamp_from (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The timestamp describing the start of the export (including).
#' @param timestamp_until (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The timestamp describing the end of the export (including).
#' @param collapse_continuous_records (`logical(1)`)\cr
#'   Check for records where from/until time stamps are equal and delete?
#' @return
#'   The lazy-query containing the data (and history) in the source to be used
#'   in conjunction with `delta_load()`.
#' @seealso update_snapshot
#' @importFrom rlang .data
#' @export
delta_export <- function(
  conn,
  db_table,
  timestamp_from,
  timestamp_until,
  collapse_continuous_records = FALSE
) {

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", len = 1, add = coll)
  assert_dbtable_like(db_table, len = 1, add = coll)
  assert_timestamp_like(timestamp_from, len = 1, add = coll)
  assert_timestamp_like(timestamp_until, any.missing = TRUE, len = 1, add = coll)
  checkmate::assert_logical(collapse_continuous_records, len = 1, add = coll)
  checkmate::reportAssertions(coll)

  # Slice the table on the timestamps
  # (We export any change that occurs within the timespan)
  out <- get_table(conn, db_table, slice_ts = NULL) |>
    dplyr::filter(
       # Inserted at or after timestamp_from or deactivated at or after timestamp_from
      !!timestamp_from  <= .data$from_ts | (!!timestamp_from  <= .data$until_ts & is.na(.data$until_ts))
    )

  if (!is.na(timestamp_until)) {
    out <- out |>
      dplyr::filter(
        # Inserted at or before timestamp_from or deactivated at or before timestamp_from
        !!timestamp_until >= .data$from_ts | (!!timestamp_until >= .data$until_ts & is.na(.data$until_ts))
      )
  }

  # Collapse continuous records
  if (collapse_continuous_records) {
    out <- dplyr::filter(out, is.na(.data$until_ts) | .data$from_ts != .data$until_ts)
  }

  return(out)
}


#' @rdname delta_loading
#' @param delta .data (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\cr
#'   "Delta" exported from `delta_export()` to load.
#' @export
delta_export <- function(
  conn,
  db_table,
  delta
) {

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(conn, "DBIConnection", len = 1, add = coll)
  assert_dbtable_like(db_table, len = 1, add = coll)
  assert_data_like(delta, add = coll)
  checkmate::reportAssertions(coll)

  # Get a reference to the table to update
  target_table <- dplyr::tbl(conn, id(db_table, conn))

  # Check if the target and delta share connection
  if (!identical(dbplyr::remote_con(target_table), dbplyr::remote_con(delta))) {

    # Copy delta to target (if needed)
    delta_src <- dplyr::copy_to(
      dbplyr::remote_con(target_table),
      delta,
      name = unique_table_name("SCDB_delta")
    )
    defer_db_cleanup(delta_src)

    # Recompute checksums on new connection
    delta_src <- digest_to_checksum(
      delta_src,
      col = "checksum",
      exclude = c("checksum", "from_ts", "until_ts")
    )
  }



}
