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

    DBI::dbDisconnect(conn)
  }
})


test_that("get_table returns list of tables if no table is requested", {
  for (conn in get_test_conns()) {

    expect_message(
      get_table(conn),
      regexp = "Select one of the following tables:"
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("get_table() works when tables exist", {
  for (conn in get_test_conns()) {


    mtcars_t <- tibble::tibble(mtcars |> dplyr::mutate(name = rownames(mtcars)))

    # Lets try different ways to read __mtcars
    expect_mapequal(get_table(conn, "__mtcars")  |> dplyr::collect(), mtcars_t)
    expect_equal(get_table(conn, id("__mtcars")) |> dplyr::collect(), mtcars_t)
    t <- "__mtcars"
    expect_equal(get_table(conn, t) |> dplyr::collect(), mtcars_t)
    t <- id("__mtcars")
    expect_equal(get_table(conn, t) |> dplyr::collect(), mtcars_t)

    # And test.mtcars
    expect_equal(get_table(conn, "test.mtcars") |> dplyr::collect(), mtcars_t)
    expect_equal(get_table(conn, id("test.mtcars", conn)) |> dplyr::collect(), mtcars_t)
    t <- "test.mtcars"
    expect_equal(get_table(conn, t) |> dplyr::collect(), mtcars_t)
    t <- id("test.mtcars", conn)
    expect_equal(get_table(conn, t) |> dplyr::collect(), mtcars_t)

    DBI::dbDisconnect(conn)
  }
})


test_that("get_table() works when tables does not exist in default schema", {
  for (conn in get_test_conns()) {

    # Generate table in default schema that does not exist
    k <- 0
    while (k < 100) {
      invalid_table_name <- paste(sample(letters, size = 16, replace = TRUE), collapse = "")
      k <- k + 1
      if (DBI::dbExistsTable(conn, id(invalid_table_name, conn))) next
      break
    }

    if (k < 100) {

      expect_error(
        get_table(conn, invalid_table_name),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name, conn)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )

    } else {
      warning("Non-existing table in default schema could not be generated!")
    }

    DBI::dbDisconnect(conn)
  }
})


test_that("get_table() works when tables does not exist in non existing schema", {
  for (conn in get_test_conns()) {

    # Generate schema that does not exist
    k <- 0
    while (k < 100) {
      invalid_schema_name <- paste(sample(letters, size = 16, replace = TRUE), collapse = "")
      k <- k + 1
      if (schema_exists(conn, invalid_schema_name)) next
      break
    }

    if (k < 100) {

      # Test some malformed inputs
      invalid_table_name <- paste(invalid_schema_name, "mtcars", sep = ".")

      expect_error(
        get_table(conn, invalid_table_name),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name, conn)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name, conn))} could not be found!")
      )
      expect_error(
        get_table(conn, id(invalid_table_name)),
        regexp = glue::glue("Table {as.character(id(invalid_table_name))} could not be found!")
      )

    } else {
      warning("Non-existing schema could not be generated!")
    }

    DBI::dbDisconnect(conn)
  }
})
