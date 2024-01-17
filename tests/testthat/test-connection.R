test_that("get_connection() works", {
  for (conn in get_test_conns()) {
    expect_true(DBI::dbIsValid(conn))

    DBI::dbDisconnect(conn)
  }
})


test_that("get_connection notifies if connection fails", {
  for (i in 1:100) {
    random_string <- paste(sample(letters, size = 32, replace = TRUE), collapse = "")

    if (dir.exists(random_string)) next

    expect_error(get_connection(drv = RSQLite::SQLite(), dbname = paste0(random_string, "/invalid_path")),
                 regexp = "Could not connect to database:\nunable to open database file")
  }
})


test_that("id() works", {
  for (conn in get_test_conns()) {

    expect_identical(id("test_mtcars"), DBI::Id(table = "test_mtcars"))
    expect_identical(id("test.mtcars"), DBI::Id(schema = "test", table = "mtcars"))

    expect_identical(id("test.mtcars", conn = conn, allow_table_only = FALSE),
                     DBI::Id(schema = "test", table = "mtcars"))

    DBI::dbDisconnect(conn)
  }
})


test_that('id() returns table = "schema.table" if schema does not exist', {
  for (conn in get_test_conns()) {

    while (TRUE) {
      schema_name <- paste(sample(letters, size = 16, replace = TRUE), collapse = "")
      if (schema_exists(conn, schema_name)) next

      break
    }

    table_name <- paste(schema_name, "mtcars", sep = ".")

    expect_identical(id(table_name, conn, allow_table_only = TRUE),
                     DBI::Id(table = table_name))

    expect_identical(id(table_name, conn, allow_table_only = FALSE),
                     DBI::Id(schema = schema_name, table = "mtcars"))

    DBI::dbDisconnect(conn)
  }
})


test_that("id() is consistent for DBI connections", {
  for (conn in get_test_conns()) {

    expectation <- id(dplyr::tbl(conn, id("test.mtcars", conn), check_from = FALSE))

    expect_identical(
      expectation,
      id.tbl_dbi(dplyr::tbl(conn, id("test.mtcars", conn), check_from = FALSE))
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("close_connection() works", {
  for (conn in get_test_conns()) {

    # Check that we can close the connection
    expect_true(DBI::dbIsValid(conn))
    close_connection(conn)
    expect_false(DBI::dbIsValid(conn))
  }
})
