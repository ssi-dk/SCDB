# devtools::load_all()
# conn <- DBI::dbConnect(RSQLite::SQLite())

for (conn in get_test_conns()) {

  # Ensure test tables are ready
  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
  expect_false(table_exists(conn, "test.SCDB_tmp1"))

  # We create a data set for the tests in SCDB_tmp1 (!)
  t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
  t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
  t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

  # Copy t0, t1, and t2 to conn
  t0 <- dplyr::copy_to(conn, t0, name = id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
  t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
  t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)

  logger <- LoggerNull$new()


  test_that("delta loading works", {

    # Build state on test connection
    update_snapshot(
      .data = t0,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:00:00",
      logger = logger
    )

    update_snapshot(
      .data = t1,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:10:00",
      logger = logger
    )

    update_snapshot(
      .data = t2,
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp = "2022-01-01 08:20:00",
      logger = logger
    )

    ## Round 1 ##############################################################

    # Export first timestamp
    delta_1 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:00:00",
      timestamp_until = "2022-01-01 08:00:00"
    ) %>%
      dplyr::compute(unique_table_name("SCDB_delta_test"))
    defer_db_cleanup(delta_1)

    # Re-build the table on another connection
    target_conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":memory:")

    # .. from a fresh state
    delta_load(
      target_conn,
      db_table = "test_incremental",
      delta = delta_1
    )

    # Generate equivalent state on the other connection
    update_snapshot(
      .data = t0,
      conn = target_conn,
      db_table = "test_reference",
      timestamp = "2022-01-01 08:00:00",
      logger = logger
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_incremental", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 2 ##############################################################

    # Export second timestamp
    delta_2 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:10:00",
      timestamp_until = "2022-01-01 08:10:00"
    ) %>%
      dplyr::compute(unique_table_name("SCDB_delta_test"))
    defer_db_cleanup(delta_2)

    # .. add to existing state
    delta_load(
      target_conn,
      db_table = "test_incremental",
      delta = delta_2
    )

    # Generate equivalent state on the other connection
    update_snapshot(
      .data = t1,
      conn = target_conn,
      db_table = "test_reference",
      timestamp = "2022-01-01 08:10:00",
      logger = logger
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_incremental", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 2.5 ############################################################

    # Re-apply the second delta
    delta_load(
      target_conn,
      db_table = "test_incremental",
      delta = delta_2
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_incremental", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 3 ##############################################################

    # Export third timestamp
    delta_3 <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:20:00",
      timestamp_until = "2022-01-01 08:20:00"
    ) %>%
      dplyr::compute(unique_table_name("SCDB_delta_test"))
    defer_db_cleanup(delta_3)

    # .. add to existing state
    delta_load(
      target_conn,
      db_table = "test_incremental",
      delta = delta_3
    )

    # Generate equivalent state on the other connection
    update_snapshot(
      .data = t2,
      conn = target_conn,
      db_table = "test_reference",
      timestamp = "2022-01-01 08:20:00",
      logger = logger
    )

    # Check transfer success (now until_ts should also match)
    expect_identical(
      get_table(target_conn, "test_incremental", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 4 ##############################################################

    # Run a single batch update
    delta_batch <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:00:00",
      timestamp_until = "2022-01-01 08:20:00"
    ) %>%
      dplyr::compute(unique_table_name("SCDB_delta_test"))
    defer_db_cleanup(delta_batch)

    # Add delta to the target connection
    delta_load(
      target_conn,
      db_table = "test_batch",
      delta = delta_batch
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_batch", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 4 ##############################################################

    # Run a single batch update (no timestamp_until)
    delta_batch_open_ended <- delta_export(
      conn = conn,
      db_table = "test.SCDB_tmp1",
      timestamp_from  = "2022-01-01 08:10:00" # From second update
    ) %>%
      dplyr::compute(unique_table_name("SCDB_delta_test"))
    defer_db_cleanup(delta_batch_open_ended)

    # Generate partial state on the other connection
    update_snapshot(
      .data = t0,
      conn = target_conn,
      db_table = "test_batch_open_ended",
      timestamp = "2022-01-01 08:00:00",
      logger = logger
    )

    # Add delta to the partial state on target connection
    delta_load(
      target_conn,
      db_table = "test_batch_open_ended",
      delta = delta_batch_open_ended
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_batch_open_ended", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    ## Round 5 ##############################################################

    # Out of order application
    delta_load(
      target_conn,
      db_table = "test_permutation",
      delta = delta_2
    )

    delta_load(
      target_conn,
      db_table = "test_permutation",
      delta = delta_3
    )

    delta_load(
      target_conn,
      db_table = "test_permutation",
      delta = delta_1
    )

    # Check transfer success
    expect_identical(
      get_table(target_conn, "test_permutation", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2),
      get_table(target_conn, "test_reference", slice_ts = NULL) %>%
        dplyr::collect() %>%
        dplyr::arrange(col1, col2)
    )

    close_connection(target_conn)
  })

  close_connection(conn)
}
