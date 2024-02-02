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


    # Copy t1, t2 and t_ref to conn (and suppress check_from message)
    t1 <- suppressMessages(
      dplyr::copy_to(conn, t1, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
    )

    t2 <- suppressMessages(
      dplyr::copy_to(conn, t2, name = id("test.SCDB_tmp2", conn), overwrite = TRUE, temporary = FALSE)
    )

    t_ref <- suppressMessages(
      dplyr::copy_to(conn, t_ref, name = id("test.SCDB_tmp3", conn), overwrite = TRUE, temporary = FALSE)
    )


    expect_identical(interlace(list(t1, t2), by = "key") |> dplyr::collect(),
                     t_ref |> dplyr::collect())

    expect_mapequal(interlace(list(t1, t2), by = "key") |> dplyr::collect(),
                    interlace(list(t2, t1), by = "key") |> dplyr::collect())

    connection_clean_up(conn)
  }
})


test_that("interlace returns early if length(table) == 1", {
  expect_identical(mtcars$mpg, interlace(mtcars["mpg"], by = "mpg"))
})