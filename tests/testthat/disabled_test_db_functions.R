test_that("is.historical() works", {
  expect_true(is.historical(tbl(conn, in_schema("prod.basis_samples"))))
  expect_false(is.historical(tbl(conn, in_schema("prod.cpr_status"))))
})
