test_that("%notin% works", {
  expect_true(2 %notin% c(1, 3))
  expect_false(2 %notin% c(1, 2, 3))
})


test_that("nrow() works", {
  for (conn in get_test_conns()) {
    x <- get_table(conn, "__mtcars")

    expect_equal(nrow(x), dplyr::pull(dplyr::count(x)))
    expect_equal(nrow(x), nrow(mtcars))

    DBI::dbDisconnect(conn)
  }
})
