test_that("create_table() refuses a historical table", {
  expect_error(
    cars %>%
      dplyr::mutate(from_ts = NA) %>%
      create_table(db_table = "fail.cars"),
    r"{checksum/from_ts/until_ts column\(s\) already exist\(s\) in .data!}"
  )
})


test_that("create_table() can create temporary tables", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table = unique_table_name(), conn = conn, temporary = TRUE)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, unique_table_name()) %>% utils::head(0))
    )

    connection_clean_up(conn)
  }
})


test_that("create_table() can create tables in default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(cars, db_table = unique_table_name(), conn = conn, temporary = FALSE)
    defer_db_cleanup(table)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, unique_table_name()) %>% utils::head(0))
    )

    connection_clean_up(conn)
  }
})


test_that("create_table() can create tables in non default schema", {
  for (conn in get_test_conns()) {

    table <- create_table(
      cars, db_table = id(paste0("test.", unique_table_name()), conn), conn = conn, temporary = FALSE
    )
    defer_db_cleanup(table)

    expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, unique_table_name()) %>% utils::head(0))
    )

    connection_clean_up(conn)
  }
})


test_that("create_table() works with no conn", {
  table <- create_table(cars, db_table = unique_table_name(), conn = NULL)

  expect_identical(colnames(table), c(colnames(cars), "checksum", "from_ts", "until_ts"))
  expect_identical(
    dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts"))),
    cars %>% utils::head(0)
  )
})


test_that("create_table() does not overwrite tables", {
  for (conn in get_test_conns()) {

    table_name <- unique_table_name()
    table <- create_table(cars, db_table = table_name, conn = conn, temporary = TRUE)

    table_regex <- paste(
      paste(c(get_catalog(conn, temporary = TRUE), get_schema(conn, temporary = TRUE)), collapse = "."),
      paste0("#?", table_name),
      sep = "."
    )

    expect_error(
      create_table(iris, db_table = table_name, conn = conn, temporary = TRUE),
      regexp = paste("Table", table_regex, "already exists!")
    )

    expect_identical(
      dplyr::collect(dplyr::select(table, -tidyselect::all_of(c("checksum", "from_ts", "until_ts")))),
      dplyr::collect(dplyr::copy_to(conn, cars, unique_table_name()) %>% utils::head(0))
    )

    connection_clean_up(conn)
  }
})
