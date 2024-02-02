#' Update a historical table
#' @template .data_dbi
#' @template conn
#' @param db_table An object that inherits from `tbl_dbi`, a [DBI::Id()] object or a character string readable by [id].
#' @param timestamp
#'   A timestamp (POSIXct) with which to update from_ts/until_ts columns
#' @template filters
#' @param message
#'   A message to add to the log-file (useful for supplying metadata to the log)
#' @param tic
#'   A timestamp when computation began. If not supplied, it will be created at call-time.
#'   (Used to more accurately convey how long runtime of the update process has been)
#' @param logger
#'   A [Logger] instance. If none is given, one is initialized with default arguments.
#' @param enforce_chronological_order
#'   A logical that controls whether or not to check if timestamp of update is prior to timestamps in the DB
#' @return No return value, called for side effects
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' data <- dplyr::copy_to(conn, mtcars)
#'
#' update_snapshot(data,
#'                 conn = conn,
#'                 db_table = "test.mtcars",
#'                 timestamp = Sys.time())
#'
#' close_connection(conn)
#' @seealso filter_keys
#' @importFrom rlang .data
#' @export
update_snapshot <- function(.data, conn, db_table, timestamp, filters = NULL, message = NULL, tic = Sys.time(), # nolint: cyclocomp_linter, line_length_linter
                            logger = NULL,
                            enforce_chronological_order = TRUE) {

  # Check arguments
  checkmate::assert_class(.data, "tbl_dbi")
  checkmate::assert_class(conn, "DBIConnection")
  assert_dbtable_like(db_table)
  assert_timestamp_like(timestamp)
  checkmate::assert_class(filters, "tbl_dbi", null.ok = TRUE)
  checkmate::assert_character(message, null.ok = TRUE)
  assert_timestamp_like(tic)
  checkmate::assert_class(logger, "Logger", null.ok = TRUE)
  checkmate::assert_logical(enforce_chronological_order)

  # Retrieve Id from any valid db_table inputs to correctly create a missing table
  db_table_id <- id(db_table, conn)
  db_table_name <- db_table_id |>
    methods::slot("name") |>
    stats::na.omit() |>
    paste(collapse = ".")

  if (table_exists(conn, db_table_id)) {
    db_table <- dplyr::tbl(conn, db_table_id, check_from = FALSE)
  } else {
    db_table <- create_table(dplyr::collect(utils::head(.data, 0)), conn, db_table_id, temporary = FALSE)
  }

  # Initialize logger
  if (is.null(logger)) {
    logger <- Logger$new(
      db_tablestring = db_table_name,
      log_conn = conn,
      ts = timestamp,
      start_time = tic
    )
  }

  logger$log_to_db(start_time = !!db_timestamp(tic, conn))
  logger$log_info("Started", tic = tic) # Use input time in log

  # Add message to log (if given)
  if (!is.null(message)) {
    logger$log_to_db(message = message)
    logger$log_info("Message:", message, tic = tic)
  }


  # Opening checks
  if (!is.historical(db_table)) {
    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error("Table does not seem like a historical table", tic = tic) # Use input time in log
  }

  if (!setequal(colnames(.data),
                colnames(dplyr::select(db_table, !c("checksum", "from_ts", "until_ts"))))) {
    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error("Columns do not match!\n",
                     "Table columns:\n",
                     paste(colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts")))),
                           collapse = ", "),
                     "\nInput columns:\n",
                     paste(colnames(.data), collapse = ", "), tic = tic) # Use input time in log
  }

  logger$log_to_db(schema = purrr::pluck(db_table_id@name, "schema"), table = purrr::pluck(db_table_id@name, "table"))
  logger$log_info("Parsing data for table", db_table_name, "started", tic = tic) # Use input time in log
  logger$log_to_db(date = !!db_timestamp(timestamp, conn))
  logger$log_info("Given timestamp for table is", timestamp, tic = tic) # Use input time in log

  # Check for current update status
  db_latest <- db_table |>
    dplyr::summarize(max(.data$from_ts, na.rm = TRUE)) |>
    dplyr::pull() |>
    as.character() |>
    max("1900-01-01 00:00:00", na.rm = TRUE)

  # Convert timestamp to character to prevent inconsistent R behavior with date/timestamps
  timestamp <- strftime(timestamp)
  db_latest <- strftime(db_latest)

  if (enforce_chronological_order && timestamp < db_latest) {
    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error("Given timestamp", timestamp, "is earlier than latest",
                     "timestamp in table:", db_latest, tic = tic) # Use input time in log
  }

  # Compute .data immediately to reduce runtime and compute checksum
  .data <- .data |>
    dplyr::ungroup() |>
    dplyr::select(
      colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts"))))
    ) |>
    digest_to_checksum(col = "checksum") |>
    filter_keys(filters) |>
    dplyr::compute()

  if (!identical(dbplyr::remote_con(.data), conn)) {
    if (!table_exists(conn, "update_snapshot_patch")) DBI::dbRemoveTable(conn, "update_snapshot_patch")
    .data <- dplyr::copy_to(conn, .data, name = "SCDB_update_snapshot_patch", temporary = TRUE)
  }

  # Apply filter to current records
  if (!is.null(filters) && !identical(dbplyr::remote_con(filters), conn)) {
    if (!table_exists(conn, "update_snapshot_patch")) DBI::dbRemoveTable(conn, "update_snapshot_patch")
    filters <- dplyr::copy_to(conn, filters, name = "SCDB_update_snapshot_filters", temporary = TRUE)
  }
  db_table <- filter_keys(db_table, filters)

  # Determine the next timestamp in the data (can be NA if none is found)
  next_timestamp <- min(db_table |>
                          dplyr::filter(.data$from_ts  > timestamp) |>
                          dplyr::summarize(next_timestamp = min(.data$from_ts, na.rm = TRUE)) |>
                          dplyr::pull("next_timestamp"),
                        db_table |>
                          dplyr::filter(.data$until_ts > timestamp) |>
                          dplyr::summarize(next_timestamp = min(.data$until_ts, na.rm = TRUE)) |>
                          dplyr::pull("next_timestamp")) |>
    strftime()

  # Consider only records valid at timestamp (and apply the filter if present)
  db_table <- slice_time(db_table, timestamp)

  # Count open rows at timestamp
  nrow_open <- nrow(db_table)


  # Select only data with no until_ts and with different values in any fields
  logger$log_info("Deactivating records")
  if (nrow_open > 0) {
    to_remove <- dplyr::setdiff(dplyr::select(db_table, "checksum"),
                                dplyr::select(.data, "checksum")) |>
      dplyr::compute() # Something has changed in dbplyr (2.2.1) that makes this compute needed.
    # Code that takes 20 secs with can be more than 30 minutes to compute without...

    nrow_to_remove <- nrow(to_remove)

    # Determine from_ts and checksum for the records we need to deactivate
    to_remove <- to_remove |>
      dplyr::left_join(dplyr::select(db_table, "from_ts", "checksum"), by = "checksum") |>
      dplyr::mutate(until_ts = !!db_timestamp(timestamp, conn))

  } else {
    nrow_to_remove <- 0
  }
  logger$log_info("After to_remove")



  to_add <- dplyr::setdiff(.data, dplyr::select(db_table, colnames(.data))) |>
    dplyr::mutate(from_ts  = !!db_timestamp(timestamp, conn),
                  until_ts = !!db_timestamp(next_timestamp, conn))

  nrow_to_add <- nrow(to_add)
  logger$log_info("After to_add")



  if (nrow_to_remove > 0) {
    dplyr::rows_update(x = dplyr::tbl(conn, db_table_id, check_from = FALSE),
                       y = to_remove,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       unmatched = "ignore")
  }

  logger$log_to_db(n_deactivations = nrow_to_remove) # Logs contains the aggregate number of added records on the day
  logger$log_info("Deactivate records count:", nrow_to_remove)
  logger$log_info("Adding new records")

  if (nrow_to_add > 0) {
    dplyr::rows_append(x = dplyr::tbl(conn, db_table_id, check_from = FALSE),
                       y = to_add,
                       in_place = TRUE)
  }

  logger$log_to_db(n_insertions = nrow_to_add)
  logger$log_info("Insert records count:", nrow_to_add)


  # If several updates come in a single day, some records may have from_ts = until_ts.
  # We remove these records here
  redundant_rows <- dplyr::tbl(conn, db_table_id, check_from = FALSE) |>
    dplyr::filter(.data$from_ts == .data$until_ts) |>
    dplyr::select("checksum", "from_ts")
  nrow_redundant <- nrow(redundant_rows)

  if (nrow_redundant > 0) {
    dplyr::rows_delete(dplyr::tbl(conn, db_table_id, check_from = FALSE),
                       redundant_rows,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE, unmatched = "ignore")
    logger$log_info("Doubly updated records removed:", nrow_redundant)
  }

  # If chronological order is not enforced, some records may be split across several records
  # checksum is the same, and from_ts / until_ts are continuous
  # We collapse these records here
  if (!enforce_chronological_order) {
    redundant_rows <- dplyr::tbl(conn, db_table_id, check_from = FALSE) |>
      filter_keys(filters)

    redundant_rows <- dplyr::inner_join(
      redundant_rows,
      redundant_rows |> dplyr::select("checksum", "from_ts", "until_ts"),
      suffix = c("", ".p"),
      sql_on = '"RHS"."checksum" = "LHS"."checksum" AND "LHS"."until_ts" = "RHS"."from_ts"'
    ) |>
      dplyr::select(!"checksum.p")

    redundant_rows_to_delete <- redundant_rows |>
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts.p) |>
      dplyr::compute()

    redundant_rows_to_update <- redundant_rows |>
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts, until_ts = .data$until_ts.p) |>
      dplyr::compute()

    if (nrow(redundant_rows_to_delete) > 0) {
      dplyr::rows_delete(x = dplyr::tbl(conn, db_table_id, check_from = FALSE),
                         y = redundant_rows_to_delete,
                         by = c("checksum", "from_ts"),
                         in_place = TRUE,
                         unmatched = "ignore")
    }

    if (nrow(redundant_rows_to_update) > 0) {
      dplyr::rows_update(x = dplyr::tbl(conn, db_table_id, check_from = FALSE),
                         y = redundant_rows_to_update,
                         by = c("checksum", "from_ts"),
                         in_place = TRUE,
                         unmatched = "ignore")
      logger$log_info("Continous records collapsed:", nrow(redundant_rows_to_update))
    }

  }

  toc <- Sys.time()
  logger$log_to_db(end_time = !!db_timestamp(toc, conn),
                   duration = !!format(round(difftime(toc, tic), digits = 2)), success = TRUE)
  logger$log_info("Finished processing for table", db_table_name, tic = toc)

  return()
}
