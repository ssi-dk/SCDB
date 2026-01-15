
#' Export a data-chunk with history from historical data
#'
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
#' @seealso update_snapshot, delta_load
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