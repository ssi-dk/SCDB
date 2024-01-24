test_that("nrow() works", {
  for (conn in get_test_conns()) {
    x <- get_table(conn, "__mtcars")

    expect_equal(nrow(x), dplyr::pull(dplyr::count(x)))
    expect_equal(nrow(x), nrow(mtcars))

    connection_clean_up(conn)
  }
})


test_that("unique_table_name() works", {
  # Store options before tests and reset (tests modify options)
  opts <- options("SCDB_table_name" = NULL, "test_table_name" = NULL)

  expect_equal(unique_table_name(), "SCDB_001")
  expect_equal(unique_table_name(), "SCDB_002")

  expect_equal(unique_table_name("test"), "test_001")
  expect_equal(unique_table_name("test"), "test_002")

  # Reset options
  options(opts)
})
