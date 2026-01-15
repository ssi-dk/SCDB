devtools::load_all()
conn <- DBI::dbConnect(RSQLite::SQLite())

for (conn in get_test_conns()) {

  # Update snapshot can optionally collapse continuous records so we test both cases
  for (collapse_continuous_records in c(TRUE, FALSE)) {

    # Ensure test tables are ready
    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
    if (DBI::dbExistsTable(conn, id("test.SCDB_tmp2", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp2", conn))
    expect_false(table_exists(conn, "test.SCDB_tmp1"))
    expect_false(table_exists(conn, "test.SCDB_tmp2"))

    # We create a data set for the tests in SCDB_tmp1 (!)
    t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
    t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
    t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

    # Copy t0, t1, and t2 to conn
    t0 <- dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
    t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
    t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)

    logger <- LoggerNull$new()

    update_snapshot(
      .data = t0,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:00:00",
      logger = logger,
      collapse_continuous_records = collapse_continuous_records
    )

    update_snapshot(
      .data = t1,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:00:00", # Same as first timestamp to generate continuous records
      logger = logger,
      collapse_continuous_records = collapse_continuous_records
    )

    update_snapshot(
      .data = t2,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:10:00",
      logger = logger,
      collapse_continuous_records = collapse_continuous_records
    )


    test_that("delta loading works", {

      # Export first timestamp
      delta <- delta_export(
        conn = conn,
        db_table = "test.SCDB_tmp1",
        timestamp_from  = "2022-01-01 08:00:00",
        timestamp_until = "2022-01-01 08:00:00",
        collapse_continuous_records = collapse_continuous_records
      )

      # Re-build the table on another connection
      target_conn <- DBI::dbConnect(RSQLite::SQLite(":memory:"))

      # .. from a fresh state
      delta_load(
        target_conn,
        db_table = "test_table",
        delta = delta
      )

      # Generate equivalent state on test connection in SCDB_tmp2 (!)
      update_snapshot(
        .data = t0,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:00:00",
        logger = logger,
        collapse_continuous_records = collapse_continuous_records
      )

      update_snapshot(
        .data = t1,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:00:00",
        logger = logger,
        collapse_continuous_records = collapse_continuous_records
      )

      # Check transfer success
      expect_identical(
        get_table(target_conn, "test_table", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum"), # Checksums are not expected to be identical
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum")
      )


      # Export second timestamp
      delta <- delta_export(
        conn = conn,
        db_table = "test.SCDB_tmp1",
        timestamp_from  = "2022-01-01 08:10:00",
        timestamp_until = "2022-01-01 08:10:00",
        collapse_continuous_records = collapse_continuous_records
      )

      # Add delta to the target connection
      delta_load(
        target_conn,
        db_table = "test_table",
        delta = delta,
      )

      # Generate equivalent state on test connection in SCDB_tmp2 (!)
      update_snapshot(
        .data = t2,
        conn = conn,
        db_table = "test.SCDB_tmp2",
        timestamp = "2022-01-01 08:10:00",
        logger = logger,
        collapse_continuous_records = collapse_continuous_records
      )

      # Check transfer success
      expect_identical(
        get_table(target_conn, "test_table", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum"), # Checksums are not expected to be identical
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum")
      )


      # Export all timestamps again (with all changes)
      # and check that re-application of delta does not break things
      delta <- delta_export(
        conn = conn,
        db_table = "test.SCDB_tmp1",
        timestamp_from  = "2022-01-01 08:00:00",
        timestamp_until = "2022-01-01 08:10:00",
        collapse_continuous_records = collapse_continuous_records
      )

      # Add delta to the target connection
      delta_load(
        target_conn,
        db_table = "test_table",
        delta = delta
      )

      # Check transfer success
      expect_identical(
        get_table(target_conn, "test_table", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum"), # Checksums are not expected to be identical
        get_table(conn, "test.SCDB_tmp2", slice_ts = NULL) |>
          dplyr::collect() |>
          dplyr::arrange(col1, col2) |>
          dplyr::select(!"checksum")
      )

      # Generate an open-ended delta and check it matches the specific delta
      delta_open_ended <- delta_export(
        conn = conn,
        db_table = "test.SCDB_tmp1",
        timestamp_from  = "2022-01-01 08:00:00",
        timestamp_until = NA,
        collapse_continuous_records = collapse_continuous_records
      )

      expect_identical(
        dplyr::collect(delta_open_ended),
        dplyr::collect(delta)
      )

    })
  }

  close_connection(conn)
}
