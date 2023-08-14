test_that("%notin% works", {
  expect_true(2 %notin% c(1, 3))
  expect_false(2 %notin% c(1, 2, 3))
})


test_that("age_label() works", {
  expect_identical(age_labels(c(10, 20, 25, 35)),
                   c("00-09", "10-19", "20-24", "25-34", "35+"))
  expect_identical(age_labels(0),
                   c("0+"))
  expect_identical(age_labels(c(1, 2, 5)),
                   c("0-0", "1-1", "2-4", "5+"))
  expect_identical(age_labels(c(10, 100)),
                   c("000-009", "010-099", "100+"))
})

test_that("mg_age_label() works", { for (conn in conns){
  expect_false(exists("mg_age_labels") && packageVersion("mg") > "1.0")
  # Time to fully deprecate mg_age_labels
}})


test_that("aggregate_age() works", {
  age_cuts <- c(10, 20, 25, 35)
  age <- c(5, 9, 10, 15, 20, 25, 26, 30, 35, 40)

  expect_identical(aggregate_age(age, age_cuts),
                   factor(c(1, 1, 2, 2, 3, 4, 4, 4, 5, 5),
                          labels = c("00-09", "10-19", "20-24", "25-34", "35+")))
})


test_that("aggregate_age_sql() works", { for (conn in conns){
  age_cuts <- c(10, 20, 25, 35)
  age <- c(5, 9, 10, 15, 20, 25, 26, 30, 35, 40)

  q <- dplyr::copy_to(conn, data.frame(age), overwrite = TRUE)

  expect_identical(q |> dplyr::mutate(age = !!aggregate_age_sql(age, age_cuts)) |> dplyr::pull(age),
                   c("00-09", "00-09", "10-19", "10-19", "20-24", "25-34", "25-34", "25-34", "35+", "35+"))

  expect_identical(q |> dplyr::mutate(age = !!aggregate_age_sql("age", age_cuts)) |> dplyr::pull(age),
                   c("00-09", "00-09", "10-19", "10-19", "20-24", "25-34", "25-34", "25-34", "35+", "35+"))

}})


test_that("nrow() works", { for (conn in conns){
  x <- get_table(conn, "__mtcars")

  expect_equal(nrow(x), dplyr::pull(dplyr::count(x)))
  expect_equal(nrow(x), nrow(mtcars))
}})
