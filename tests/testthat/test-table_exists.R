test_that("table_exists() works for default schema", {
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

      # Without explicit schema, table_exists assumes default schema
      expect_true(table_exists(conn, "__mtcars"))
      expect_false(table_exists(conn, invalid_table_name))

      expect_true(table_exists(conn, DBI::Id(table = "__mtcars")))
      expect_false(table_exists(conn, DBI::Id(table = invalid_table_name)))

      # Using the default schema should therefore yield the same results
      expect_true(table_exists(conn, paste(get_schema(conn), "__mtcars", sep = ".")))
      expect_false(table_exists(conn, paste(get_schema(conn), invalid_table_name, sep = ".")))

      expect_true(table_exists(conn, DBI::Id(schema = get_schema(conn), table = "__mtcars")))
      expect_false(table_exists(conn, DBI::Id(schema = get_schema(conn), table = invalid_table_name)))

    } else {
      warning("Non-existing table in default schema could not be generated!")
    }

    connection_clean_up(conn)
  }
})


test_that("table_exists() works for non-default schema", {
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

      # With an implied schema, table_exists should still determine existence correctly

      # Character inputs
      expect_true(table_exists(conn, "test.mtcars"))
      expect_false(table_exists(conn, paste(invalid_schema_name, "mtcars", sep = ".")))


      # DBI::Id inputs
      if (schema_exists(conn, "test")) {
        expect_true(table_exists(conn, DBI::Id(schema = "test", table = "mtcars")))
      } else {
        expect_false(table_exists(conn, DBI::Id(schema = "test", table = "mtcars")))
      }
      expect_false(table_exists(conn, DBI::Id(schema = invalid_schema_name, table = "mtcars")))

    } else {
      warning("Non-existing schema could not be generated!")
    }

    connection_clean_up(conn)
  }
})


test_that("table_exists() fails when multiple matches are found", {
  for (conn in get_test_conns()) {

    # Not all data bases support schemas.
    # Here we filter out the data bases that do not support schema
    # NOTE: SQLite does support schema, but we test both with and without attaching schemas
    if (schema_exists(conn, "test") && schema_exists(conn, "test.one")) {

      DBI::dbExecute(conn, 'CREATE TABLE "test"."one.two"(a TEXT)')
      DBI::dbExecute(conn, 'CREATE TABLE "test.one"."two"(b TEXT)')

      expect_error(
        table_exists(conn, "test.one.two"),
        regex = "More than one table matching 'test.one.two' was found!"
      )

    }

    connection_clean_up(conn)
  }
})


test_that("table_exists() works when starting from empty", {
  skip_if_not_installed("RSQLite")

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  expect_false(table_exists(conn, "mtcars"))

  dplyr::copy_to(conn, mtcars, "mtcars", temporary = FALSE)

  expect_true(table_exists(conn, "mtcars"))

  connection_clean_up(conn)
})
