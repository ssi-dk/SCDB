test_that("get_schema() and get_catalog() works for tbl_dbi", {
  for (conn in get_test_conns()) {

    # Check for permanent tables in default schema
    table <- dplyr::tbl(conn, "__mtcars")
    expect_identical(get_schema(table), get_schema(conn))
    expect_identical(get_catalog(table), get_catalog(conn))

    table_id_inferred <- DBI::Id(
      catalog = get_catalog(table),
      schema = get_schema(table),
      table = "__mtcars"
    )

    expect_identical(
      dplyr::collect(dplyr::tbl(conn, table_id_inferred)),
      dplyr::collect(table)
    )


    # Check for temporary tables
    table_name <- unique_table_name()
    table <- dplyr::copy_to(conn, mtcars, table_name, temporary = TRUE)
    expect_identical(get_schema(table), get_schema(conn, temporary = TRUE))
    expect_identical(get_catalog(table), get_catalog(conn, temporary = TRUE))

    table_id_inferred <- DBI::Id(
      catalog = get_catalog(table),
      schema = get_schema(table),
      table = table_name
    )

    expect_identical(
      dplyr::collect(dplyr::tbl(conn, table_id_inferred)),
      dplyr::collect(table)
    )

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
