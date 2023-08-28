test_that("Code contains no non-ascii characters", {
  pkg_dir <- stringr::str_remove(getwd(), "/tests/testthat")

  r_files  <- list.files(path = file.path(pkg_dir, "R"),   pattern = r"{\.[Rr]$}",     full.names = TRUE)
  rd_files <- list.files(path = file.path(pkg_dir, "man"), pattern = r"{\.[Rr][Dd]$}", full.names = TRUE)

  files_to_check <- c(r_files, rd_files) |>
    purrr::discard(~ stringr::str_detect(.x, "SCDB-package"))

  for (file in files_to_check) {
    lines <- readLines(file, warn = FALSE)
    has_non_ascii <- any(grepl(r"{[^\x00-\x7f]}", lines, perl = TRUE))
    if (has_non_ascii) {
      print(grepl(r"{[^\x00-\x7f]}", lines, perl = TRUE))
    }
    expect_false(has_non_ascii, label = paste("File:", file))
  }
})
