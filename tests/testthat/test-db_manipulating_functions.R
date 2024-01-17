test_that("unite.tbl_dbi() works", {
  for (conn in get_test_conns()) {

    q <- get_table(conn, "__mtcars") |> utils::head(1)
    qu_remove <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", mpg, hp) |> dplyr::compute()
    qu        <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", mpg, hp, remove = FALSE) |> dplyr::compute()
    qu_alt    <- tidyr::unite(dplyr::select(q, mpg, hp), "new_column", "mpg", "hp", remove = FALSE) |> dplyr::compute()

    expect_s3_class(qu_remove, "tbl_dbi")
    expect_s3_class(qu,        "tbl_dbi")
    expect_s3_class(qu_alt,    "tbl_dbi")

    expect_equal(colnames(qu_remove), "new_column")
    expect_equal(colnames(qu),     c("new_column", "mpg", "hp"))
    expect_equal(colnames(qu_alt), c("new_column", "mpg", "hp"))

    expect_equal(dplyr::collect(qu), dplyr::collect(qu_alt))

    # tidyr::unite has some quirky (and FUN!!! behavior) that we are forced to match here
    # specifically, the input "col" is converted to a symbol, so we have to do escape-bullshit
    # NOTE: the line "dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |> "
    # is to account for SQLite not having integer data-types. If we do not first convert to character,
    # there will be differences between the objects that are trivial, so we remove these with this operation
    # this way, the test should (hopefully) only fail if there are non-trivial differences
    expect_mapequal(get_table(conn, "__mtcars") |>
                      tidyr::unite("new_col", mpg, hp) |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect(),
                    get_table(conn, "__mtcars") |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect() |>
                      tidyr::unite("new_col", mpg, hp))

    col <- "new_col"
    expect_mapequal(get_table(conn, "__mtcars") |>
                      tidyr::unite(col, mpg, hp) |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect(),
                    get_table(conn, "__mtcars") |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect() |>
                      tidyr::unite(col, mpg, hp))

    expect_mapequal(get_table(conn, "__mtcars") |>
                      tidyr::unite(!!col, mpg, hp) |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect(),
                    get_table(conn, "__mtcars") |>
                      dplyr::mutate(dplyr::across(tidyselect::everything(), as.character)) |>
                      dplyr::collect() |>
                      tidyr::unite(!!col, mpg, hp))

    # Unite places cols in a particular way, lets be sure we match
    qq <- dplyr::mutate(q, dplyr::across(tidyselect::everything(), as.character)) # we convert to character since SQLite
    expect_identical(qq |> tidyr::unite("test_col", vs, am) |> dplyr::collect(),
                     qq |> dplyr::collect() |> tidyr::unite("test_col", vs, am))

    DBI::dbDisconnect(conn)
  }
})


test_that("interlace_sql() works", {
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


    expect_identical(interlace_sql(list(t1, t2), by = "key") |> dplyr::collect(),
                     t_ref |> dplyr::collect())

    expect_mapequal(interlace_sql(list(t1, t2), by = "key") |> dplyr::collect(),
                    interlace_sql(list(t2, t1), by = "key") |> dplyr::collect())

    DBI::dbDisconnect(conn)
  }
})


test_that("interlace_sql returns early if length(table) == 1", {
  expect_identical(mtcars$mpg, interlace_sql(mtcars["mpg"], by = "mpg"))
})


test_that("digest_to_checksum() works", {
  for (conn in get_test_conns()) {

    expect_s3_class(mtcars |> digest_to_checksum(), "data.frame")
    expect_s3_class(mtcars |> tibble::as_tibble() |> digest_to_checksum(), "tbl_df")
    expect_s3_class(get_table(conn, "__mtcars") |> digest_to_checksum(), "tbl_dbi")

    # Check that col argument works
    expect_equal(mtcars |> digest_to_checksum(col = "checky") |> dplyr::pull("checky"),
                 mtcars |> digest_to_checksum()               |> dplyr::pull("checksum"))


    expect_equal(mtcars |> dplyr::mutate(name = rownames(mtcars)) |> digest_to_checksum() |> colnames(),
                 get_table(conn, "__mtcars") |> digest_to_checksum() |> colnames())


    # Check that NA's generate unique checksums
    x <- data.frame(col1 = c("A", NA),
                    col2 = c(NA, "A"))

    # .. locally
    checksums <- x |> digest_to_checksum() |> dplyr::pull("checksum")
    expect_false(checksums[1] == checksums[2])

    # .. and on the remote
    x <- suppressMessages(
      dplyr::copy_to(conn, x, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)
    )

    checksums <- x |> digest_to_checksum() |> dplyr::pull("checksum")
    expect_false(checksums[1] == checksums[2])

    DBI::dbDisconnect(conn)
  }
})


test_that("digest_to_checksum() warns works correctly when overwriting", {
  for (conn in get_test_conns()) {

    checksum_vector <- mtcars |>
      digest_to_checksum() |>
      dplyr::pull(checksum)

    expect_warning(checksum_vector2 <- mtcars |>
                     digest_to_checksum(col = "checksum") |>
                     digest_to_checksum(col = "checksum", warn = TRUE) |>
                     dplyr::pull(checksum))

    expect_identical(checksum_vector, checksum_vector2)

    DBI::dbDisconnect(conn)
  }
})


test_that("slice_time() works", {
  for (conn in get_test_conns()) {

    # SQLite does not work with dates. But since we use ISO 8601 for dates, we can compare lexicographically
    xx <- get_table(conn, "__mtcars") |>
      dplyr::mutate(checksum = dplyr::row_number(),
                    from_ts = dplyr::if_else(checksum <= 20, "2022-06-01", "2022-06-15"),
                    until_ts = NA_character_)

    expect_equal(xx |> slice_time("2022-05-01") |> nrow(), 0)
    expect_equal(xx |> slice_time("2022-06-01") |> nrow(), 20)
    expect_equal(xx |> slice_time("2022-06-15") |> nrow(), nrow(mtcars))

    DBI::dbDisconnect(conn)
  }
})

test_that("filter_keys() works", {
  for (conn in get_test_conns()) {

    x <- get_table(conn, "__mtcars")

    expect_equal(x,
                 x |> filter_keys(NULL))

    filter <- x |> utils::head(10) |> dplyr::select(name)
    expect_equal(x |>
                   dplyr::filter(name %in% !!dplyr::pull(filter, name)) |>
                   dplyr::collect(),
                 x |>
                   filter_keys(filter) |>
                   dplyr::collect())

    filter <- x |> utils::head(10) |> dplyr::select(vs, am) |> dplyr::distinct()
    expect_equal(x |>
                   dplyr::inner_join(filter, by = c("vs", "am")) |>
                   dplyr::collect(),
                 x |>
                   filter_keys(filter) |>
                   dplyr::collect())

    # Filtering with null means no filtering is done
    m <- mtcars
    row.names(m) <- NULL
    filter <- NULL
    expect_identical(filter_keys(m, filter), m)

    # Filtering by vs = 0
    filter <- data.frame(vs = 0)
    expect_mapequal(filter_keys(m, filter), dplyr::filter(m, vs == 0))

    DBI::dbDisconnect(conn)
  }
})
