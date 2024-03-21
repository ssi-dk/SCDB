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
    x <- dplyr::copy_to(conn, x, name = id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)

    checksums <- x |> digest_to_checksum() |> dplyr::pull("checksum")
    expect_false(checksums[1] == checksums[2])

    connection_clean_up(conn)
  }
})


test_that("digest_to_checksum() warns works correctly when overwriting", {
  for (conn in get_test_conns()) {

    checksum_vector_1 <- mtcars |>
      digest_to_checksum() |>
      dplyr::pull(checksum)

    expect_warning(
      checksum_vector_2 <- mtcars |>                                                                                    # nolint: implicit_assignment_linter
        digest_to_checksum(col = "checksum") |>
        digest_to_checksum(col = "checksum") |>
        dplyr::pull(checksum),
      "Column checksum already exists in data and will be overwritten!"
    )

    expect_identical(checksum_vector_1, checksum_vector_2)

    connection_clean_up(conn)
  }
})
