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
      table = paste0(ifelse(inherits(conn, "Microsoft SQL Server"), "#", ""), table_name)
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


for (conn in c(list(NULL), get_test_conns())) {

  if (is.null(conn)) {
    test_that("get_schema() works for NULL connection", {
      expect_null(get_schema(conn))
      expect_null(get_schema(conn, temporary = TRUE))
    })
  }

  if (inherits(conn, "SQLiteConnection")) {
    test_that("get_schema() works for SQLiteConnection", {
      expect_identical(get_schema(conn), "main")
      expect_identical(get_schema(conn, temporary = TRUE), "temp")
    })
  }

  if (inherits(conn, "PqConnection")) {
    test_that("get_schema() works for PqConnection", {
      expect_identical(get_schema(conn), "public")
      checkmate::expect_character(get_schema(conn, temporary = TRUE), pattern = "pq_temp_.*")
    })
  }

  if (inherits(conn, "Microsoft SQL Server")) {
    test_that("get_schema() works for Microsoft SQL Server", {
      expect_identical(get_schema(conn), "dbo")
      expect_identical(get_schema(conn, temporary = TRUE), "dbo")
    })
  }

  if (!is.null(conn)) connection_clean_up(conn)
}


for (conn in c(list(NULL), get_test_conns())) {

  if (is.null(conn)) {
    test_that("get_catalog() works for NULL connection", {
      expect_null(get_catalog(conn))
      expect_null(get_catalog(conn, temporary = TRUE))
    })
  }

  if (inherits(conn, "SQLiteConnection")) {
    test_that("get_catalog() works for SQLiteConnection", {
      expect_null(get_catalog(conn))
      expect_null(get_catalog(conn, temporary = TRUE))
    })
  }

  if (inherits(conn, "PqConnection")) {
    test_that("get_catalog() works for PqConnection", {
      expect_null(get_catalog(conn))
      expect_null(get_catalog(conn, temporary = TRUE))
    })
  }

  if (inherits(conn, "Microsoft SQL Server")) {
    test_that("get_catalog() works for Microsoft SQL Server", {
      expect_identical(get_catalog(conn), "master")
      expect_identical(get_catalog(conn, temporary = TRUE), "tempdb")
    })
  }

  if (!is.null(conn)) connection_clean_up(conn)
}
