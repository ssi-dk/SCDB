test_that("is.historical() works", { for (conn in conns) { # nolint: brace_linter
  expect_true(is.historical(dplyr::tbl(conn, id("__mtcars_historical", conn))))
  expect_false(is.historical(dplyr::tbl(conn, id("__mtcars", conn))))
}})
