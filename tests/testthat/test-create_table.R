test_that("create_table refuses a historical table", {
  mtcars |>
    dplyr::mutate(from_ts = NA) |>
    create_table(db_table_id = "fail.mtcars") |>
    expect_error()
})

test_that("create_table works with no conn", {
  mylocaltable <- create_table(mtcars, db_table_id = "test.mylocaltable", conn = NULL)

  expect_identical(colnames(mylocaltable), c(colnames(mtcars), "checksum", "from_ts", "until_ts"))
  expect_identical(dplyr::select(mylocaltable, -tidyselect::all_of(c("checksum", "from_ts", "until_ts"))), mtcars)
})

test_that("getTableSignature generates a signature for NULL connections", {
  expect_identical(
    lapply(mtcars, class),
    as.list(getTableSignature(mtcars, conn = NULL))
  )
})
