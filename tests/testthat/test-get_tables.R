test_that("get_tables() works without pattern", {
  for (conn in get_test_conns()) {

    # Check for the existence of "test.mtcars" and "__mtcars" (added during test setup)
    # For SQLite connections, we don't always have the "test" schema, so we check for its existence
    # and use default schema if it does not exist.
    table_1 <- paste(c(switch(!schema_exists(conn, "test"), get_schema(conn)), "test.mtcars"), collapse = ".")
    table_2 <- paste(c(get_schema(conn), "__mtcars"), collapse = ".")

    # Check for the existence of views on backends that support it (added here)
    if (inherits(conn, "PqConnection")) {

      DBI::dbExecute(conn, "CREATE VIEW __mtcars_view AS SELECT * FROM __mtcars LIMIT 10")
      view_1 <- paste(c(get_schema(conn), "__mtcars_view"), collapse = ".")

    } else if (inherits(conn, "Microsoft SQL Server")) {

      DBI::dbExecute(conn, "CREATE VIEW __mtcars_view AS SELECT TOP 10 * FROM __mtcars")
      view_1 <- paste(c(get_schema(conn), "__mtcars_view"), collapse = ".")

    } else {
      view_1 <- NULL
    }


    # Pull the tables and compare with expectation
    tables <- get_tables(conn)
    expect_s3_class(tables, "data.frame")

    db_table_names <- tables |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)

    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Our test tables should be present
    checkmate::expect_subset(c(table_1, table_2, view_1), db_table_names)


    # Drop the view
    if (checkmate::test_multi_class(conn, c("PqConnection", "Microsoft SQL Server"))) {
      DBI::dbExecute(conn, glue::glue("DROP VIEW {view_1}"))
    }

    connection_clean_up(conn)
  }
})


test_that("get_tables() works with pattern", {
  for (conn in get_test_conns()) {

    # Call with pattern
    db_table_names <- get_tables(conn, pattern = "__mt") |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)


    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Check for the existence of "test.mtcars" and "__mtcars" (added during test setup)
    # For SQLite connections, we don't always have the "test" schema, so we check for its existence
    # and use default schema if it does not exist.
    table_1 <- paste(c(switch(!schema_exists(conn, "test"), get_schema(conn)), "test.mtcars"), collapse = ".")
    table_2 <- paste(c(get_schema(conn), "__mtcars"), collapse = ".")

    # Our test table that matches the pattern should be present
    expect_false(table_1 %in% db_table_names)
    expect_true(table_2 %in% db_table_names)

    connection_clean_up(conn)
  }
})


test_that("get_tables() works with temporary tables", {
  for (conn in get_test_conns()) {

    # Create temporary table
    tmp <- dplyr::copy_to(conn, mtcars, "__mtcars_2", temporary = TRUE)
    tmp_id <- id(tmp)
    tmp_name <- paste(tmp_id@name["schema"], tmp_id@name["table"], sep = ".")

    db_table_names <- get_tables(conn, show_temporary = TRUE) |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)


    # We should not get tables twice
    expect_setequal(db_table_names, unique(db_table_names))

    # Check for the existence of "test.mtcars" and "__mtcars" (added during test setup)
    # For SQLite connections, we don't always have the "test" schema, so we check for its existence
    # and use default schema if it does not exist.
    table_1 <- paste(c(switch(!schema_exists(conn, "test"), get_schema(conn)), "test.mtcars"), collapse = ".")
    table_2 <- paste(c(get_schema(conn), "__mtcars"), collapse = ".")

    # Our test tables should be present
    checkmate::expect_subset(c(table_1, table_2, tmp_name), db_table_names)

    connection_clean_up(conn)
  }
})


test_that("get_tables() works without temporary tables", {
  for (conn in get_test_conns()) {

    # Create temporary table
    tmp <- dplyr::copy_to(conn, mtcars, "__mtcars_2", temporary = TRUE)
    tmp_id <- id(tmp)
    tmp_name <- paste(tmp_id@name["schema"], tmp_id@name["table"], sep = ".")

    db_table_names <- get_tables(conn, show_temporary = FALSE) |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)

    # Check for the existence of "test.mtcars" and "__mtcars" (added during test setup)
    # For SQLite connections, we don't always have the "test" schema, so we check for its existence
    # and use default schema if it does not exist.
    table_1 <- paste(c(switch(!schema_exists(conn, "test"), get_schema(conn)), "test.mtcars"), collapse = ".")
    table_2 <- paste(c(get_schema(conn), "__mtcars"), collapse = ".")

    # Our permanent test tables should be present
    checkmate::expect_subset(c(table_1, table_2), db_table_names)

    # But not our temporary tables
    checkmate::expect_disjunct(tmp_name, db_table_names)

    connection_clean_up(conn)
  }
})


test_that("get_tables() matches the pattern of SCDB::id", {
  for (conn in get_test_conns()) {

    # Test for both a permanent table and a temporary table
    permanent_table <- id("test.mtcars", conn = conn)

    tmp <- dplyr::copy_to(conn, mtcars, "__mtcars_2", temporary = TRUE)
    defer_db_cleanup(tmp)
    temporary_table <- id(tmp)

    # Check tables can be found by get_tables with pattern
    expect_identical(nrow(get_tables(conn, pattern = paste0("^", as.character(permanent_table)))), 1L)
    expect_identical(id(get_tables(conn, pattern = paste0("^", as.character(permanent_table)))), permanent_table)

    expect_identical(nrow(get_tables(conn, pattern = paste0("^", as.character(temporary_table)))), 1L)
    expect_identical(id(get_tables(conn, pattern = paste0("^", as.character(temporary_table)))), temporary_table)

    connection_clean_up(conn)
  }
})
