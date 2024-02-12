test_that("nrow() works", {
  for (conn in get_test_conns()) {
    x <- get_table(conn, "__mtcars")

    expect_equal(nrow(x), dplyr::pull(dplyr::count(x)))
    expect_equal(nrow(x), nrow(mtcars))

    connection_clean_up(conn)
  }
})


test_that("defer_db_cleanup() works in function call", {
  for (conn in get_test_conns()) {
    name <- unique_table_name()

    test <- \() {
      mt <- dplyr::copy_to(conn, mtcars, name, temporary = FALSE)
      expect_true(DBI::dbExistsTable(conn, id(name, conn)))

      defer_db_cleanup(mt)
      expect_true(DBI::dbExistsTable(conn, id(name, conn)))
    }

    test()

    expect_false(DBI::dbExistsTable(conn, id(name, conn)))

    connection_clean_up(conn)
  }
})


test_that("defer_db_cleanup() works with withr::deferred_run", {
  for (conn in get_test_conns()) {
    mt <- dplyr::copy_to(conn, mtcars, unique_table_name())
    mt_id <- id(mt)
    defer_db_cleanup(mt)

    expect_true(DBI::dbExistsTable(conn, mt_id))

    expect_message(withr::deferred_run(), "Ran 1/1 deferred expressions")

    expect_false(DBI::dbExistsTable(conn, mt_id))

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
