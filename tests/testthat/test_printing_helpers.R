test_that("format_danish() works", { for (conn in conns) { # nolint: brace_linter
  expect_identical(as.character(format_danish(1000)), "1.000,00")
}})


test_that("format_timestamp() works", { for (conn in conns) { # nolint: brace_linter
  ts <- as.POSIXlt("2022-09-15 09:02:04.204")
  expect_identical(format_timestamp(ts), "2022-09-15 09:02:04,204")
}})


test_that("printr() works", {
  the_string <- "This is the string to be printed"
  the_string2 <- "This is the second element of the string"

  expect_output(
    {
      printr(the_string)
      printr(the_string2)
    },
    paste(the_string, the_string2, sep = "\n")
  )
})
