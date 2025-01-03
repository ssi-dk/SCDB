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

    test <- function() {
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
  table_1 <- unique_table_name()
  table_2 <- unique_table_name()
  checkmate::expect_character(table_1, pattern = "SCDB_[a-zA-Z0-9]{10}")
  checkmate::expect_character(table_2, pattern = "SCDB_[a-zA-Z0-9]{10}")
  checkmate::expect_disjunct(table_1, table_2)

  table_1 <- unique_table_name("test")
  table_2 <- unique_table_name("test")
  checkmate::expect_character(table_1, pattern = "test_[a-zA-Z0-9]{10}")
  checkmate::expect_character(table_2, pattern = "test_[a-zA-Z0-9]{10}")
  checkmate::expect_disjunct(table_1, table_2)
})
