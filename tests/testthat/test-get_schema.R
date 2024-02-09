test_that("get_schema() and get_catalog() works for tbl_dbi", {
  for (conn in get_test_conns()) {

    # Check for permanent tables
    table <- dplyr::tbl(conn, "__mtcars")
    expect_identical(get_schema(table), get_schema(conn))
    expect_identical(get_catalog(table), get_catalog(conn))

    # Check for temporary tables
    mt <- dplyr::copy_to(conn, mtcars, unique_table_name(), temporary = TRUE)
    expect_identical(get_schema(mt), get_schema(conn, temporary = TRUE))
    expect_identical(get_catalog(mt), get_catalog(conn, temporary = TRUE))

    connection_clean_up(conn)
  }
})


test_that("get_schema() works for Id", {
  expect_null(get_schema(DBI::Id(table = "table")))
  expect_identical(get_schema(DBI::Id(schema = "schema", table = "table")), "schema")
})


test_that("get_catalog() works for Id", {
  expect_null(get_catalog(DBI::Id(table = "table")))
  expect_identical(get_catalog(DBI::Id(catalog = "catalog", table = "table")), "catalog")
})


test_that("get_schema() works for NULL", {
  expect_null(get_schema(NULL))
})


test_that("get_catalog() works for NULL", {
  expect_null(get_catalog(NULL))
})
