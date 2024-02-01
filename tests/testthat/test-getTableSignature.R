test_that("getTableSignature() generates a signature for NULL connections", {
  expect_identical(
    lapply(cars, class),
    as.list(getTableSignature(cars, conn = NULL))
  )
})
