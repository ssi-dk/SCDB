test_that("interlace.tbl_sql() works", {
  for (conn in get_test_conns()) {

    t1 <- data.frame(key = c("A", "A", "B"),
                     obs_1   = c(1, 2, 2),
                     valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-01-01")),
                     valid_until = as.Date(c("2021-02-01", "2021-03-01", NA)))


    t2 <- data.frame(key = c("A", "B"),
                     obs_2 = c("a", "b"),
                     valid_from  = as.Date(c("2021-01-01", "2021-01-01")),
                     valid_until = as.Date(c("2021-04-01", NA)))


    t_ref <- data.frame(key = c("A", "A", "A", "B"),
                        obs_1   = c(1, 2, NA, 2),
                        obs_2   = c("a", "a", "a", "b"),
                        valid_from  = as.Date(c("2021-01-01", "2021-02-01", "2021-03-01", "2021-01-01")),
                        valid_until = as.Date(c("2021-02-01", "2021-03-01", "2021-04-01", NA)))


    # Copy t1, t2 and t_ref to conn
    t1 <- dplyr::copy_to(conn, t1, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
    t2 <- dplyr::copy_to(conn, t2, name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)
    t_ref <- dplyr::copy_to(conn, t_ref, name = id("test.SCDB_tmp3", conn), overwrite = TRUE, temporary = FALSE)


    # Order of records may be different, so we arrange then check if they are identical
    expect_identical(interlace(list(t1, t2), by = "key") |>
                       dplyr::collect() |>
                       dplyr::arrange(.data$key, .data$valid_from),
                     t_ref |>
                       dplyr::collect() |>
                       dplyr::arrange(.data$key, .data$valid_from))

    # Order of columns will be different, so we only require a mapequal
    # .. but order of records can still be different
    expect_mapequal(interlace(list(t1, t2), by = "key") |>
                      dplyr::collect() |>
                      dplyr::arrange(.data$key, .data$valid_from),
                    interlace(list(t2, t1), by = "key") |>
                      dplyr::collect() |>
                      dplyr::arrange(.data$key, .data$valid_from))

    connection_clean_up(conn)
  }
})


test_that("interlace returns early if length(table) == 1", {
  expect_identical(mtcars["mpg"], interlace(list(mtcars["mpg"]), by = "mpg"))
})
