for (conn in get_test_conns()) {

  # Ensure test tables are ready
  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
  expect_false(table_exists(conn, "test.SCDB_tmp1"))

  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
  expect_false(table_exists(conn, "test.SCDB_tmp2"))

  # We create a data set for the tests in SCDB_tmp1 (!)
  t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
  t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
  t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(NA_real_,        2,        3))

  # Copy t0, t1, and t2 to conn
  t0 <- dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
  t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
  t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)

  logger_null <- LoggerNull$new()


  test_that("delta loading works for incremental backups", {

    # Build state on test connection

    # Update 1
    update_snapshot(
      .data = t0,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:00:00",
      logger = logger_null
    )

    delta_1 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:00:00"
    )
    defer_db_cleanup(delta_1)
    expect_identical(nrow(delta_1), 2L)

    # Replay delta on another table
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_1)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )


    # Update 2
    update_snapshot(
      .data = t1,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:10:00",
      logger = logger_null
    )

    delta_2 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:10:00"
    )
    defer_db_cleanup(delta_2)
    expect_identical(nrow(delta_2), 3L)

    # Replay delta on another table
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_2)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )


    # Update 3
    update_snapshot(
      .data = t2,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:20:00",
      logger = logger_null
    )

    delta_3 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:20:00"
    )
    defer_db_cleanup(delta_3)
    expect_identical(nrow(delta_3), 6L)

    # Replay delta on another table
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_3)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )

    # Clear and run all deltas
    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp2"))

    # Replay deltas
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_1)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_2)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_3)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )

    # Replay out of order
    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp2"))

    # Replay deltas
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_1)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_3)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_2)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )
  })


  test_that("delta loading works to migrate data", {

    # Re-build the table on another connection
    # (one with date-time (duckdb) and one without (sqlite))
    source_conns <- list(
      "SQLite" = get_connection(RSQLite::SQLite(), dbname = ":memory:"),
      "DuckDB" = get_connection(duckdb::duckdb(), dbdir = ":memory:")
    )

    for (source_conn in source_conns) {

      # Update 1
      update_snapshot(
        .data = t0,
        conn = source_conn,
        db_table = "source",
        timestamp = "2022-01-01 08:00:00",
        logger = logger_null
      )

      # Update 2
      update_snapshot(
        .data = t1,
        conn = source_conn,
        db_table = "source",
        timestamp = "2022-01-01 08:10:00",
        logger = logger_null
      )

      # Update 3
      update_snapshot(
        .data = t2,
        conn = source_conn,
        db_table = "source",
        timestamp = "2022-01-01 08:20:00",
        logger = logger_null
      )


      ## Test 1 ##############################################################

      # Clear test tables on connection
      if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
      expect_false(table_exists(conn, "test.SCDB_tmp1"))

      if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
      expect_false(table_exists(conn, "test.SCDB_tmp2"))

      # Export first timestamp
      delta_1 <- delta_export(
        conn = source_conn,
        db_table = "source",
        timestamp_from  = "2022-01-01 08:00:00",
        timestamp_until = "2022-01-01 08:10:00"
      )
      defer_db_cleanup(delta_1)

      # .. and load from a fresh state
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_1
        )
      )

      # Generate equivalent state on the connection
      update_snapshot(
        .data = t0,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:00:00",
        logger = logger_null
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 2 ##############################################################

      # Export second timestamp
      delta_2 <- delta_export(
        conn = source_conn,
        db_table = "source",
        timestamp_from  = "2022-01-01 08:10:00",
        timestamp_until = "2022-01-01 08:20:00"
      )
      defer_db_cleanup(delta_2)

      # .. add to existing state
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_2
        )
      )

      # Generate equivalent state on the other connection
      update_snapshot(
        .data = t1,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:10:00",
        logger = logger_null
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 2.5 ############################################################

      # Re-apply the second delta
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_2
        )
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 3 ##############################################################

      # Export third timestamp
      delta_3 <- delta_export(
        conn = source_conn,
        db_table = "source",
        timestamp_from  = "2022-01-01 08:20:00",
        timestamp_until = "2022-01-01 08:30:00"
      )
      defer_db_cleanup(delta_3)

      # .. add to existing state
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_3
        )
      )

      # Generate equivalent state on the other connection
      update_snapshot(
        .data = t2,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:20:00",
        logger = logger_null
      )

      # Check transfer success (now until_ts should also match)
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 4 ##############################################################

      # Clear test tables on connection
      if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
      expect_false(table_exists(conn, "test.SCDB_tmp1"))

      # Run a single batch update
      delta_batch <- delta_export(
        conn = source_conn,
        db_table = "source",
        timestamp_from  = "2022-01-01 08:00:00",
        timestamp_until = "2022-01-01 08:30:00"
      )
      defer_db_cleanup(delta_batch)

      # Add delta to the target connection
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_batch
        )
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 4 ##############################################################

      # Clear test tables on connection
      if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
      expect_false(table_exists(conn, "test.SCDB_tmp1"))

      # Run a single batch update (no timestamp_until)
      delta_batch_open_ended <- delta_export(
        conn = source_conn,
        db_table = "source",
        timestamp_from  = "2022-01-01 08:10:00" # From second update
      )
      defer_db_cleanup(delta_batch_open_ended)

      # Generate partial state on the other connection
      update_snapshot(
        .data = t0,
        conn = conn,
        db_table = "test.SCDB_tmp1",
        timestamp = "2022-01-01 08:00:00",
        logger = logger_null
      )

      # Add delta to the partial state on target connection
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_batch_open_ended
        )
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      ## Test 5 ##############################################################

      # Clear test tables on connection
      if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
      expect_false(table_exists(conn, "test.SCDB_tmp1"))

      # Out of order application
      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_1
        )
      )

      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_3
        )
      )

      suppressMessages(
        delta_load(
          conn,
          db_table = "test.SCDB_tmp1",
          delta = delta_2
        )
      )

      # Check transfer success
      expect_identical(
        get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts),
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
          dplyr::collect() %>%
          dplyr::arrange(col1, col2, from_ts)
      )

      close_connection(source_conn)
    }
  })


  test_that("delta loading writes logs as expected", {

    # Clear and run all deltas
    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))

    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp2"))

    if (DBI::dbExistsTable(conn, id("test.SCDB_logs", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_logs", conn))
    expect_false(table_exists(conn, "test.SCDB_logs"))

    # Configure a logger for the normal update steps
    logger_1 <- Logger$new(
      db_table = "test.SCDB_tmp1",
      log_table_id = "test.SCDB_logs",
      log_conn = conn,
      output_to_console = FALSE
    )

    # Build a test state
    # Update 1
    update_snapshot(
      .data = t0,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:00:00",
      logger = logger_1
    )

    delta_1 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:00:00"
    )
    defer_db_cleanup(delta_1)

    # Update 2
    update_snapshot(
      .data = t1,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:10:00",
      logger = logger_1
    )

    delta_2 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:10:00"
    )
    defer_db_cleanup(delta_2)

    # Update 3
    update_snapshot(
      .data = t2,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:20:00",
      logger = logger_1
    )

    delta_3 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:20:00"
    )
    defer_db_cleanup(delta_3)


    # Configure a logger for the delta load
    logger_2 <- Logger$new(
      db_table = "test.SCDB_tmp2",
      log_table_id = "test.SCDB_logs",
      log_conn = conn
    )

    # Replay deltas
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_1, logger = logger_2)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_2, logger = logger_2)
    delta_load(conn, db_table = "test.SCDB_tmp2", delta = delta_3, logger = logger_2)

    # Check transfer success
    expect_identical(
      get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts),
      get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2, from_ts)
    )

    # Check logs match insertions and deactivations
    expect_identical(
      dplyr::tbl(conn, id("test.SCDB_logs", conn)) %>%
        tidyr::unite("db_table_name", dplyr::any_of(c("catalog", "schema", "table")), sep = ".") %>%
        dplyr::filter(.data$db_table_name == !!as.character(id("test.SCDB_tmp2", conn))) %>% # delta_load() logs
        dplyr::select("date", "n_insertions", "n_deactivations") %>%
        dplyr::collect() %>%
        dplyr::arrange(.data$date),
      dplyr::tbl(conn, id("test.SCDB_logs", conn)) %>%
        tidyr::unite("db_table_name", dplyr::any_of(c("catalog", "schema", "table")), sep = ".") %>%
        dplyr::filter(.data$db_table_name == !!as.character(id("test.SCDB_tmp1", conn))) %>% # update_snapshot() logs
        dplyr::select("date", "n_insertions", "n_deactivations") %>%
        dplyr::collect() %>%
        dplyr::arrange(.data$date)
    )
  })

  close_connection(conn)
}
