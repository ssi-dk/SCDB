test_that("get_tables() works", {
  for (conn in get_test_conns()) {

    tables <- get_tables(conn)
    expect_s3_class(tables, "data.frame")

    db_table_names <- tables |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)


    # Check for the existence of "test.mtcars" and "__mtcars" (added during test setup)
    # For SQLite connections, we don't always have the "test" schema, so we check for its existence
    # and use default schema if it does not exist.
    table_1 <- paste(c(switch(!schema_exists(conn, "test"), get_schema(conn)), "test.mtcars"), collapse = ".")
    table_2 <- paste(c(get_schema(conn), "__mtcars"), collapse = ".")

    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Our test tables should be present
    checkmate::expect_subset(c(table_1, table_2), db_table_names)


    # Now test with pattern
    db_table_names <- get_tables(conn, pattern = "__mt") |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)

    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Our test table that matches the pattern should be present
    expect_false(table_1 %in% db_table_names)
    expect_true(table_2 %in% db_table_names)


    # Now test with temporary tables
    tmp <- dplyr::copy_to(conn, mtcars, unique_table_name(), temporary = TRUE)
    tmp_id <- id(tmp)
    tmp_name <- paste(tmp_id@name["schema"], tmp_id@name["table"], sep = ".")

    db_table_names <- get_tables(conn, show_temporary = TRUE) |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)

    checkmate::expect_subset(c(table_1, table_2, tmp_name), db_table_names)


    # Now test with temporary tables
    tmp <- dplyr::copy_to(conn, mtcars, "__mtcars_2", temporary = TRUE)
    tmp_id <- id(tmp)
    tmp_name <- paste(tmp_id@name["schema"], tmp_id@name["table"], sep = ".")

    db_table_names <- get_tables(conn, show_temporary = TRUE) |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)


    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Our test tables should be present
    checkmate::expect_subset(c(table_1, table_2, tmp_name), db_table_names)

    connection_clean_up(conn)
  }
})
