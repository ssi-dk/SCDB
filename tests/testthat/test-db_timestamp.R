test_that("db_timestamp produce consistent results", { for (conn in conns) { # nolint: brace_linter
  ts_posix <- Sys.time()
  ts_str <- format(ts_posix)

  expect_identical(
    db_timestamp(ts_posix, conn),
    db_timestamp(ts_str, conn)
  )

  # Test using NULL conn to trigger
  expect_identical(
    db_timestamp.default(ts_posix, conn = NULL),
    db_timestamp.default(ts_str, conn = NULL)
  )
}})
