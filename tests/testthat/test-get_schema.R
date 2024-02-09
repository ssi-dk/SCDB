test_that("get_schema() works for tbl_dbi", {
  for (conn in get_test_conns()) {

    # Check for permanent tables
    table <- dplyr::tbl(conn, "__mtcars")
    expect_identical(get_schema(table), get_schema(conn))

    # Check for temporary tables
    mt <- dplyr::copy_to(conn, mtcars, unique_table_name(), temporary = TRUE)
    expect_identical(get_schema(mt), get_schema(conn, temporary = TRUE))

    DBI::dbDisconnect(conn)
  }
})


test_that("get_schema() works for Id", {
  expect_null(get_schema(DBI::Id(table = "table")))
  expect_identical(get_schema(DBI::Id(schema = "schema", table = "table")), "schema")
})


test_that("get_schema() works for NULL", {
  expect_null(get_schema(NULL))
})
