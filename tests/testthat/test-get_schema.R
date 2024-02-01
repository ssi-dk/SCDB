test_that("get_schema() works for tbl_dbi", {
  for (conn in get_test_conns()) {
    table <- dplyr::tbl(conn, "__mtcars")

    expect_null(get_schema(table), get_schema(conn))

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
