#' Update a historical table
#'
#' @description
#'   `update_snapshots` makes it easy to create and update a historical data table on a remote (SQL) server.
#'   The function takes the data (`.data`) as it looks on a given point in time (`timestamp`) and then updates
#'   (or creates) an remote table identified by `db_table`.
#'   This update only stores the changes between the new data (`.data`) and the data currently stored on the remote.
#'   This way, the data can be reconstructed as it looked at any point in time while taking as little space as possible.
#'
#'   See `vignette("basic-principles")` for further introduction to the function.
#'
#' @template .data
#' @template conn
#' @template db_table
#' @param timestamp (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The timestamp describing the data being processed (not the current time).
#' @template filters
#' @param message (`character(1)`)\cr
#'   A message to add to the log-file (useful for supplying metadata to the log).
#' @param tic (`POSIXct(1)`)\cr
#'   A timestamp when computation began. If not supplied, it will be created at call-time
#'   (used to more accurately convey the runtime of the update process).
#' @param logger (`Logger(1)`)\cr
#'   A configured logging object. If none is given, one is initialized with default arguments.
#' @param enforce_chronological_order (`logical(1)`)\cr
#'   Are updates allowed if they are chronologically earlier than latest update?
#' @return
#'   No return value, called for side effects.
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
#'     timestamp = Sys.time()
#'   )
#'
#'   # Update with the first 5 records
#'   update_snapshot(
#'     head(data, 5),
#'     conn = conn,
#'     db_table = "test.mtcars",
#'     timestamp = Sys.time()
#'   )
#'
#'   dplyr::tbl(conn, "test.mtcars")
#'
#'   close_connection(conn)
#' @seealso filter_keys
#' @importFrom rlang .data
#' @export
update_snapshot <- function(.data, conn, db_table, timestamp, filters = NULL, message = NULL, tic = Sys.time(),
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
  checkmate::assert_multi_class(logger, "Logger", null.ok = TRUE)
  checkmate::assert_logical(enforce_chronological_order)


  ### Create target table if not exists
  # Retrieve Id from any valid db_table inputs to correctly create a missing table
  db_table_id <- id(db_table, conn)

  if (table_exists(conn, db_table_id)) {
    # Obtain a lock on the table
    if (!lock_table(conn, db_table_id, schema = get_schema(db_table_id))) {
      stop("A lock could not be obtained on the table")
    }

    db_table <- dplyr::tbl(conn, db_table_id)
  } else {
    db_table <- create_table(dplyr::collect(utils::head(.data, 0)), conn, db_table_id, temporary = FALSE)
  }

  ### Initialize logger
  if (is.null(logger)) {
    logger <- Logger$new(
      db_table = db_table_id,
      log_conn = conn,
      timestamp = timestamp,
      start_time = tic
    )
  }

  logger$log_info("Started", tic = tic) # Use input time in log

  # Add message to log (if given)
  if (!is.null(message)) {
    logger$log_to_db(message = message)
    logger$log_info("Message:", message, tic = tic)
  }



  ### Check incoming data
  if (!is.historical(db_table)) {

    # Release table lock
    unlock_table(conn, db_table_id, get_schema(db_table_id))

    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error("Table does not seem like a historical table", tic = tic) # Use input time in log
  }

  if (!setequal(colnames(.data),
                colnames(dplyr::select(db_table, !c("checksum", "from_ts", "until_ts"))))) {

    # Release table lock
    unlock_table(conn, db_table_id, get_schema(db_table_id))

    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error(
      "Columns do not match!\n",
      "Table columns:\n",
      toString(colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts"))))),
      "\nInput columns:\n",
      toString(colnames(.data)),
      tic = tic # Use input time in log
    )
  }

  logger$log_info("Parsing data for table", as.character(db_table_id), "started", tic = tic) # Use input time in log
  logger$log_info("Given timestamp for table is", timestamp, tic = tic) # Use input time in log




  ### Check for current update status
  db_latest <- db_table |>
    dplyr::summarize(max(.data$from_ts, na.rm = TRUE)) |>
    dplyr::pull() |>
    as.character() |>
    dplyr::coalesce("1900-01-01 00:00:00")

  # Convert timestamp to character to prevent inconsistent R behavior with date/timestamps
  timestamp <- strftime(timestamp)
  db_latest <- strftime(db_latest)

  if (enforce_chronological_order && timestamp < db_latest) {

    # Release the table lock
    unlock_table(conn, db_table_id, get_schema(db_table_id))

    logger$log_to_db(success = FALSE, end_time = !!db_timestamp(tic, conn))
    logger$log_error("Given timestamp", timestamp, "is earlier than latest",
                     "timestamp in table:", db_latest, tic = tic) # Use input time in log
  }



  ### Filter and compute checksums for incoming data
  .data <- .data |>
    dplyr::ungroup() |>
    filter_keys(filters) |>
    dplyr::select(colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts")))))

  # Copy to the target connection if needed
  if (!identical(dbplyr::remote_con(.data), conn)) {
    .data <- dplyr::copy_to(conn, .data, name = unique_table_name())
    defer_db_cleanup(.data)
  }

  # Once we ensure .data is on the same connection as the target, we compute the checksums
  .data <- dplyr::compute(digest_to_checksum(.data, col = "checksum"))
  defer_db_cleanup(.data)

  ### Determine the next timestamp in the data (can be NA if none is found)
  next_timestamp <- min(db_table |>
                          dplyr::filter(.data$from_ts  > timestamp) |>
                          dplyr::summarize(next_timestamp = min(.data$from_ts, na.rm = TRUE)) |>
                          dplyr::pull("next_timestamp"),
                        db_table |>
                          dplyr::filter(.data$until_ts > timestamp) |>
                          dplyr::summarize(next_timestamp = min(.data$until_ts, na.rm = TRUE)) |>
                          dplyr::pull("next_timestamp")) |>
    strftime()



  ### Consider only records valid at timestamp (and apply the filter if present)
  db_table <- slice_time(db_table, timestamp)

  # Apply filter to current records
  if (!is.null(filters) && !identical(dbplyr::remote_con(filters), conn)) {
    filters <- dplyr::copy_to(conn, filters, name = unique_table_name())
    defer_db_cleanup(filters)
  }
  db_table <- filter_keys(db_table, filters)



  # Determine records to remove and to add to the DB
  # Records will be removed if their checksum no longer exists on the new date
  # Records will be added if their checksum does not exists in the current data


  # Generate SQL at lower level than tidyverse to get the affected rows without computing.
  slice_ts <- db_timestamp(timestamp, conn)

  currently_valid_checksums <- db_table |>
    dplyr::select("checksum")


  ## Deactivation
  checksums_to_deactivate <- dplyr::setdiff(currently_valid_checksums, dplyr::select(.data, "checksum"))

  sql_deactivate <- dbplyr::sql_query_update_from(
    con = conn,
    table = dbplyr::as.sql(db_table_id, con = conn),
    from = dbplyr::sql_render(checksums_to_deactivate),
    by = "checksum",
    update_values = c("until_ts" = slice_ts)
  )

  # Commit changes to DB
  rs_deactivate <- DBI::dbSendQuery(conn, sql_deactivate)
  n_deactivations <- DBI::dbGetRowsAffected(rs_deactivate)
  DBI::dbClearResult(rs_deactivate)
  logger$log_to_db(n_deactivations = !!n_deactivations)


  ## Insertion

  records_to_insert <- dbplyr::build_sql(
    con = conn,
    "SELECT *, ", db_timestamp(timestamp, conn), " AS ", dbplyr::ident("from_ts"), ", ",
    db_timestamp(next_timestamp, conn), " AS ", dbplyr::ident("until_ts"),
    " FROM ", dbplyr::remote_table(.data),
    " WHERE ", dbplyr::remote_table(.data), ".", dbplyr::ident("checksum"),
    " NOT IN (", dbplyr::sql_render(currently_valid_checksums), ")"
  )

  sql_insert <- dbplyr::sql_query_insert(
    con = conn,
    table = dbplyr::as.sql(db_table_id, con = conn),
    from = records_to_insert,
    insert_cols = c(colnames(.data), "from_ts", "until_ts"),
    by = c("checksum", "from_ts"),
    conflict = "ignore"
  )

  # Commit changes to DB
  rs_insert <- DBI::dbSendQuery(conn, sql_insert)
  n_insertions <- DBI::dbGetRowsAffected(rs_insert)
  DBI::dbClearResult(rs_insert)
  logger$log_to_db(n_insertions = !!n_insertions)


  # If chronological order is not enforced, some records may be split across several records
  # checksum is the same, and from_ts / until_ts are continuous
  # We collapse these records here
  if (!enforce_chronological_order) {

    # First we identify the records with this stitching
    consecutive_rows <- dplyr::inner_join(
      dplyr::tbl(conn, db_table_id),
      dplyr::tbl(conn, db_table_id) |> dplyr::select("checksum", "from_ts", "until_ts"),
      suffix = c("", ".p"),
      sql_on = paste(
        '"RHS"."checksum" = "LHS"."checksum" AND ',
        '("LHS"."until_ts" = "RHS"."from_ts" OR "LHS"."from_ts" = "RHS"."until_ts")'
      )
    )

    # If the record has the earlier from_ts, we use the until_ts of the other.
    # If the record has the later from_ts, we set until_ts equal to from_ts to trigger
    # clean up later in update_snapshot.
    consecutive_rows_fix <- consecutive_rows |>
      dplyr::mutate("until_ts" = ifelse(.data$from_ts < .data$from_ts.p, .data$until_ts.p, .data$from_ts)) |>
      dplyr::select(!tidyselect::ends_with(".p"))

    if (inherits(conn, "duckdb_connection")) {
      # For duckdb the lower level translation fails
      # dbplyr 2.5.0, duckdb 0.10.2
      consecutive_rows_fix <- dplyr::compute(consecutive_rows_fix)
      defer_db_cleanup(consecutive_rows_fix)


      dplyr::rows_update(
        x = dplyr::tbl(conn, db_table_id),
        y = consecutive_rows_fix,
        by = c("checksum", "from_ts"),
        unmatched = "ignore",
        in_place = TRUE
      )

    } else {
      sql_fix_consecutive <- dbplyr::sql_query_upsert(
        con = conn,
        table = dbplyr::as.sql(db_table_id, con = conn),
        from = dbplyr::sql_render(consecutive_rows_fix),
        by =  c("checksum", "from_ts"),
        update_cols = "until_ts"
      )

      # Commit changes to DB
      rs_fix_consecutive <- DBI::dbSendQuery(conn, sql_fix_consecutive)
      DBI::dbClearResult(rs_fix_consecutive)
    }
  }



  # If several updates come in a single day, some records may have from_ts = until_ts.
  # Alternatively, the above handling of consecutive records will make records have from_ts = until_ts
  # We remove these records here
  redundant_rows <- dplyr::tbl(conn, db_table_id) |>
    dplyr::filter(.data$from_ts == .data$until_ts) |>
    dplyr::select("checksum", "from_ts")

  sql_fix_redundant <- dbplyr::sql_query_delete(
    con = conn,
    table = dbplyr::as.sql(db_table_id, con = conn),
    from = dbplyr::sql_render(redundant_rows),
    by = c("checksum", "from_ts")
  )

  # Commit changes to DB
  rs_fix_redundant <- DBI::dbSendQuery(conn, sql_fix_redundant)
  DBI::dbClearResult(rs_fix_redundant)


  # Clean up
  toc <- Sys.time()
  logger$finalize_db_entry()
  logger$log_info("Finished processing for table", as.character(db_table_id), tic = toc)

  # Release table lock
  unlock_table(conn, db_table_id, get_schema(db_table_id))

  return(NULL)
}
