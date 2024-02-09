test_that("create_table() refuses a historical table", {
  expect_error(
    cars |>
      dplyr::mutate(from_ts = NA) |>
      create_table(db_table_id = "fail.cars"),
    r"{checksum/from_ts/until_ts column\(s\) already exist\(s\) in .data!}"
  )
})


test_that("create_table() can create temporary tables", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = "cars", conn = conn, temporary = TRUE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref") |> utils::head(0))
    )

    connection_clean_up(conn)
  }
})


test_that("create_table() can create tables in default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = "cars", conn = conn, temporary = FALSE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref") |> utils::head(0))
    )

    DBI::dbRemoveTable(conn, id(table))
    connection_clean_up(conn)
  }
})


test_that("create_table() can create tables in non default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = id("test.cars", conn), conn = conn, temporary = FALSE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref") |> utils::head(0))
    )

    DBI::dbRemoveTable(conn, id(table))
    connection_clean_up(conn)
  }
})


test_that("create_table() works with no conn", {
  table <- create_table(cars, db_table_id = "cars", conn = NULL)

  expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
  expect_identical(
    dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts"))),
    cars |> utils::head(0)
  )
})


test_that("create_table() does not overwrite tables", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table_id = "cars", conn = conn, temporary = TRUE)

    expect_error(
      create_table(iris, db_table_id = "cars", conn = conn, temporary = TRUE),
      regexp = r"{(?:object named)|(?:table)|(?:relation) ["'`]#?cars["'`] (?:already exists)|(?:in the database)}"
    )

    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, "cars_ref") |> utils::head(0))
    )

    connection_clean_up(conn)
  }
})
