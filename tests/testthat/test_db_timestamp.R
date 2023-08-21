test_that("db_timestamp produce consistent results", { for (conn in conns) {
  ts_posix <- Sys.time()
  ts_str <- format(ts_posix)

  expect_identical(
    db_timestamp(ts_posix, conn),
    db_timestamp(ts_str, conn)
  )
}})
