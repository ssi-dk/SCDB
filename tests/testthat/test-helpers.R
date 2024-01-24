test_that("nrow() works", {
  for (conn in get_test_conns()) {
    x <- get_table(conn, "__mtcars")

    expect_equal(nrow(x), dplyr::pull(dplyr::count(x)))
    expect_equal(nrow(x), nrow(mtcars))

    connection_clean_up(conn)
  }
})
