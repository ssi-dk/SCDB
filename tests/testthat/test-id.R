test_that("id() works for character input without implied schema", {
  for (conn in get_test_conns()) {

    # Without schema, we expect:

    # ... no change of no conn is given
    expect_identical(id("test_mtcars"), DBI::Id(table = "test_mtcars"))

    # .. the defaults schema if conn is given
    expect_identical(id("test_mtcars", conn), DBI::Id(schema = SCDB::get_schema(conn), table = "test_mtcars"))

    DBI::dbDisconnect(conn)
  }
})


test_that("id() works for character input with implied schema", {
  # With schema we expect the implied schema.table to be resolved:

  # ... when no conn is given, we naively assume schema.table holds true
  expect_identical(id("test.mtcars"), DBI::Id(schema = "test", table = "mtcars"))


  for (conn in get_test_conns()) {

    # ... when conn is given, we check if implied schema exists.
    # NOTE: All testing connections should have the schema "test" (except SQLite without attached schemas)
    # therefore, in almost all cases, we shold resolve the schema correctly (except the SQLite case above)
    if (inherits(conn, "SQLiteConnection") && !schema_exists(conn, "test")) {
      expect_identical(id("test.mtcars", conn), DBI::Id(schema = "main", table = "test.mtcars"))
    } else {
      expect_identical(id("test.mtcars", conn), DBI::Id(schema = "test", table = "mtcars"))
    }

    DBI::dbDisconnect(conn)
  }
})


test_that("id() works for character input with implied schema when schema does not exist", {
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

      table_name <- paste(invalid_schema_name, "mtcars", sep = ".")

      # When schema does not exist and allow_table_only is TRUE, the schema should be the default schema
      expect_identical(id(table_name, conn = conn, allow_table_only = TRUE),
                       DBI::Id(schema = get_schema(conn), table = table_name))

      # When schema does not exist and allow_table_only is FALSE, the schema should be as implied
      expect_identical(id(table_name, conn = conn, allow_table_only = FALSE),
                       DBI::Id(schema = invalid_schema_name, table = "mtcars"))

    } else {
      warning("Non-existing schema could not be generated!")
    }

    DBI::dbDisconnect(conn)

    # When connection is closed, the existence of the schema cannot be validated and an error should be given
    expect_error(id(table_name, conn = conn), r"{DBI::dbIsValid\(conn\): FALSE}")
  }
})


test_that("id() works for DBI::Id inputs", {
  for (conn in get_test_conns()) {

    # When passing an Id without a schema, id should enrich the Id with the default schema
    expect_identical(
      id(DBI::Id(table = "mtcars"), conn),
      DBI::Id(schema = get_schema(conn), table = "mtcars")
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("id() is consistent for tbl_dbi inputs", {
  for (conn in get_test_conns()) {

    expectation <- id(dplyr::tbl(conn, id("test.mtcars", conn), check_from = FALSE))

    expect_identical(
      expectation,
      id.tbl_dbi(dplyr::tbl(conn, id("test.mtcars", conn), check_from = FALSE))
    )

    DBI::dbDisconnect(conn)
  }
})


test_that("as.character.id() works", {
  expect_identical(as.character(DBI::Id(table = "table")), "table")
  expect_identical(as.character(DBI::Id(schema = "schema", table = "table")), "schema.table")
  expect_identical(as.character(DBI::Id(catalog = "catalog", schema = "schema", table = "table")),
                   "catalog.schema.table")

  expect_identical(as.character(DBI::Id(table = "table", schema = "schema")), "schema.table")
  expect_identical(as.character(DBI::Id(table = "table", schema = "schema", catalog = "catalog")),
                   "catalog.schema.table")
})
