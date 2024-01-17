test_that("get_tables() works", {
  for (conn in get_test_conns()) {

    tables <- get_tables(conn)
    expect_s3_class(tables, "data.frame")

    db_table_names <- tables |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)

    expect_true("test.mtcars" %in% db_table_names)
    expect_true("__mtcars" %in% db_table_names)

    # Now test with pattern
    db_table_names <- get_tables(conn, pattern = "__mt") |>
      tidyr::unite("db_table_name", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull(db_table_name)


    expect_false("test.mtcars" %in% db_table_names)
    expect_true("__mtcars" %in% db_table_names)

    DBI::dbDisconnect(conn)
  }
})


test_that("table_exists() works", {
  for (conn in get_test_conns()) {

    expect_true(table_exists(conn, "__mtcars"))
    expect_false(table_exists(conn, "__mctars"))

    expect_true(table_exists(conn, DBI::Id(table = "__mtcars")))
    expect_false(table_exists(conn, DBI::Id(table = "__mctars")))

    expect_true(table_exists(conn, "test.mtcars"))
    expect_false(table_exists(conn, "tset.mtcars"))

    expect_true(table_exists(conn, DBI::Id(schema = "test", table = "mtcars")))
    expect_false(table_exists(conn, DBI::Id(schema = "tset", table = "mtcars")))

    DBI::dbDisconnect(conn)
  }
})


test_that("get_table() works", {
  for (conn in get_test_conns()) {

    mtcars_t <- tibble::tibble(mtcars |> dplyr::mutate(name = rownames(mtcars)))

    # Lets try different ways to read __mtcars
    expect_mapequal(get_table(conn, "__mtcars") |> dplyr::collect(), mtcars_t)
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

    # Test some malformed inputs
    expect_error(get_table(conn, "tset.mtcars"),     regexp = "Table tset.mtcars is not found!")
    expect_error(get_table(conn, id("tset.mtcars", conn)), regexp = "Table tset.mtcars is not found!")
    t <- "tset.mtcars"
    expect_error(get_table(conn, t), regexp = "Table tset.mtcars is not found!")
    t <- id("tset.mtcars", conn)
    expect_error(get_table(conn, t), regexp = "Table tset.mtcars is not found!")

    DBI::dbDisconnect(conn)
  }
})

test_that("get_table returns list of tables if no table is requested", {
  for (conn in get_test_conns()) {

    expect_message(get_table(conn),
      regexp = "Select one of the following tables:"
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("table_exists fails when multiple matches are found", {
  conns <- get_test_conns()
  for (conn_id in seq_along(conns)) {

    conn <- conns[[conn_id]]

    # Not all data bases support schemas.
    # Here we filter out the data bases that do not support schema
    # NOTE: SQLite does support schema, but we test both with and without attaching schemas
    if (names(conns)[[conn_id]] != "SQLite") {

      DBI::dbExecute(conn, 'CREATE TABLE "test"."one.two"(a TEXT)')
      DBI::dbExecute(conn, 'CREATE TABLE "test.one"."two"(b TEXT)')

      expect_error(
        table_exists(conn, "test.one.two"),
        regex = "More than one table matching 'test.one.two' was found!"
      )

    }

    DBI::dbDisconnect(conn)
  }
})
