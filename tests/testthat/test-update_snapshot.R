test_that("update_snapshot() works", { for (conn in conns) { # nolint: brace_linter

  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
  if (DBI::dbExistsTable(conn, id("test.SCDB_logs", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_logs", conn))

  target <- mtcars %>%
    dplyr::copy_to(conn, ., overwrite = TRUE) |>
    digest_to_checksum(col = "checksum", warn = FALSE) |>
    dplyr::mutate(from_ts  = !!db_timestamp("2022-10-01 09:00:00", conn),
                  until_ts = !!db_timestamp(NA, conn)) %>%
    dplyr::copy_to(conn, ., id("test.SCDB_tmp1", conn), overwrite = TRUE, temporary = FALSE)


  .data <- mtcars |>
    dplyr::mutate(hp = dplyr::if_else(hp > 130, hp - 10, hp)) %>%
    dplyr::copy_to(conn, ., overwrite = TRUE)

  # This is a simple update where 23 rows are replaced with 23 new ones on the given date
  db_table <- "test.SCDB_tmp1"
  log_table_id <- "test.SCDB_logs"
  timestamp <- "2022-10-03 09:00:00"
  log_path <- tempdir()
  dir(log_path) |>
    purrr::keep(~ endsWith(., ".log")) |>
    purrr::walk(~ unlink(file.path(log_path, .)))

  utils::capture.output(update_snapshot(.data, conn, db_table, timestamp,
                                        log_path = log_path, log_table_id = log_table_id))

  expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
  expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
               nrow(mtcars))

  expect_identical(slice_time(target, "2022-10-03 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   .data |> collect() |> dplyr::arrange(wt, qsec))
  expect_equal(nrow(slice_time(target, "2022-10-03 09:00:00")),
               nrow(mtcars))

  # Check log outputs exists
  log_pattern <- paste(stringr::str_replace_all(as.Date(timestamp), "-", "_"), "test.SCDB_tmp1.log", sep = ".")
  log_file <- purrr::keep(dir(log_path), ~stringr::str_detect(., log_pattern))
  expect_true(length(log_file) == 1)
  expect_true(file.info(file.path(log_path, log_file))$size > 0)
  expect_true(nrow(get_table(conn, "test.SCDB_logs")) == 1)
  expect_true(nrow(filter(get_table(conn, "test.SCDB_logs"), if_any(.cols = !c(log_file), .fns = ~ !is.na(.)))) == 1)


  # We now attempt to do another update on the same date
  .data <- mtcars |>
    dplyr::mutate(hp = dplyr::if_else(hp > 100, hp - 10, hp)) %>%
    dplyr::copy_to(conn, ., overwrite = TRUE)

  utils::capture.output(update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-03 09:00:00",
                                        log_path = NULL, log_table_id = log_table_id))

  # Even though we insert twice on the same date, we expect the data to be minimal (compacted)
  expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
  expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
               nrow(mtcars))

  expect_identical(slice_time(target, "2022-10-03 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   .data |> collect() |> dplyr::arrange(wt, qsec))
  expect_equal(nrow(slice_time(target, "2022-10-03 09:00:00")),
               nrow(mtcars))




  # We now attempt to an update between these two updates
  .data <- mtcars |>
    dplyr::mutate(hp = dplyr::if_else(hp > 150, hp - 10, hp)) %>%
    dplyr::copy_to(conn, ., overwrite = TRUE)

  # This should fail if we do not specify "enforce_chronological_order = FALSE"
  expect_error(utils::capture.output(update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00",
                                                     log_path = NULL, log_table_id = log_table_id)),
               regexp = "Given timestamp 2022-10-02 09:00:00 is earlier")

  # But not with the variable set
  utils::capture.output(update_snapshot(.data, conn, "test.SCDB_tmp1", "2022-10-02 09:00:00",
                                        log_path = NULL, log_table_id = log_table_id,
                                        enforce_chronological_order = FALSE))

  expect_identical(slice_time(target, "2022-10-01 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   mtcars |> dplyr::arrange(wt, qsec) |> tibble::tibble())
  expect_equal(nrow(slice_time(target, "2022-10-01 09:00:00")),
               nrow(mtcars))

  expect_identical(slice_time(target, "2022-10-02 09:00:00") |>
                     dplyr::select(!c("from_ts", "until_ts", "checksum")) |>
                     dplyr::collect() |>
                     dplyr::arrange(wt, qsec),
                   .data |> collect() |> dplyr::arrange(wt, qsec))
  expect_equal(nrow(slice_time(target, "2022-10-02 09:00:00")),
               nrow(mtcars))


  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
  expect_false(table_exists(conn, "test.SCDB_tmp1"))

  # Check deletion of redundant rows
  t0 <- data.frame(col1 = c("A", "B"),      col2 = c(NA_real_, NA_real_))
  t1 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        NA_real_, NA_real_))
  t2 <- data.frame(col1 = c("A", "B", "C"), col2 = c(1,        2,        3))

  t0 <- copy_to(conn, t0, id("test.SCDB_t0", conn), overwrite = TRUE, temporary = FALSE)
  t1 <- copy_to(conn, t1, id("test.SCDB_t1", conn), overwrite = TRUE, temporary = FALSE)
  t2 <- copy_to(conn, t2, id("test.SCDB_t2", conn), overwrite = TRUE, temporary = FALSE)

  utils::capture.output(update_snapshot(t0, conn, "test.SCDB_tmp1", "2022-01-01",
                                        log_path = NULL, log_table_id = log_table_id))
  expect_identical(dplyr::collect(t0) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

  utils::capture.output(update_snapshot(t1, conn, "test.SCDB_tmp1", "2022-02-01",
                                        log_path = NULL, log_table_id = log_table_id))
  expect_identical(dplyr::collect(t1) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

  utils::capture.output(update_snapshot(t2, conn, "test.SCDB_tmp1", "2022-02-01",
                                        log_path = NULL, log_table_id = log_table_id))
  expect_identical(dplyr::collect(t2) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

  t <- list(t0, t1, t2) |>
    purrr::reduce(union) |>
    dplyr::collect() |>
    dplyr::mutate(col2 = as.character(col2)) |>
    dplyr::arrange(col1, col2) |>
    utils::head(5)

  t_ref <- get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) |>
    dplyr::select(!any_of(c("from_ts", "until_ts", "checksum"))) |>
    dplyr::collect() |>
    dplyr::mutate(col2 = as.character(col2)) |>
    dplyr::arrange(col1, col2)

  expect_identical(t, t_ref)

  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))



  # Check non-chronological insertion
  utils::capture.output(update_snapshot(t0, conn, "test.SCDB_tmp1", "2022-01-01",
                                        log_path = NULL, log_table_id = log_table_id))
  expect_identical(dplyr::collect(t0) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

  utils::capture.output(update_snapshot(t2, conn, "test.SCDB_tmp1", "2022-03-01",
                                        log_path = NULL, log_table_id = log_table_id))
  expect_identical(dplyr::collect(t2) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1")) |> dplyr::arrange(col1))

  utils::capture.output(update_snapshot(t1, conn, "test.SCDB_tmp1", "2022-02-01",
                                        log_path = NULL, log_table_id = log_table_id,
                                        enforce_chronological_order = FALSE))
  expect_identical(dplyr::collect(t1) |> dplyr::arrange(col1),
                   dplyr::collect(get_table(conn, "test.SCDB_tmp1", slice_ts = "2022-02-01")) |> arrange(col1))

  t_ref <-
    tibble::tibble(col1     = c("A",          "B",          "A",          "C",          "B",          "C"),
                   col2     = c(NA_real_,     NA_real_,     1,            NA_real_,     2,            3),
                   from_ts  = c("2022-01-01", "2022-01-01", "2022-02-01", "2022-02-01", "2022-03-01", "2022-03-01"),
                   until_ts = c("2022-02-01", "2022-03-01", NA,           "2022-03-01", NA,           NA))

  expect_identical(get_table(conn, "test.SCDB_tmp1", slice_ts = NULL) |>
                     dplyr::select(!"checksum") |>
                     dplyr::collect() |>
                     dplyr::mutate(from_ts  = strftime(from_ts),
                                   until_ts = strftime(until_ts)) |>
                     dplyr::arrange(col1, from_ts),
                   t_ref |>
                     dplyr::arrange(col1, from_ts))

  if (DBI::dbExistsTable(conn, id("test.SCDB_tmp1", conn))) DBI::dbRemoveTable(conn, id("test.SCDB_tmp1", conn))
}})

test_that("update_snapshot checks table formats", {

  ops <- options(SCDB.log_path = tempdir())

  for (conn in conns) {
    mtcars_table <- tbl(conn, id("__mtcars_historical", conn = conn))
    timestamp <- Sys.time()

    # Test columns not matching
    broken_table <- copy_to(conn, dplyr::select(mtcars, -"mpg"), name = "mtcars_broken", overwrite = T)

    expect_error(
      utils::capture.output(update_snapshot(
        .data = broken_table,
        conn = conn,
        db_table = mtcars_table,
        timestamp = timestamp
      ),
    regex = "Columns do not match!"))

    file.remove(list.files(getOption("SCDB.log_path"), pattern = format(timestamp, "^%Y%m%d.%H%M"), full.names = T))

    # Test target table not being a historical table
    expect_error(utils::capture.output(update_snapshot(
      tbl(conn, id("test.mtcars", conn = conn), mtcars),
      conn,
      tbl(conn, "__mtcars"),
      timestamp = timestamp
    ),
    regex = "Table does not seem like a historical table"
    ))
  }

  options(ops)
})
