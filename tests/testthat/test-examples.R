test_that(r"{.Rd files have \examples}", {
  pkg_dir <- stringr::str_remove(getwd(), "/tests/testthat")

  rd_files <- list.files(path = file.path(pkg_dir, "man"), pattern = r"{\.[Rr][Dd]$}", full.names = TRUE)

  files_to_check <- c(rd_files) |>
    purrr::discard(~ stringr::str_detect(.x, "SCDB-package"))

  for (file in files_to_check) {
    lines <- readLines(file, warn = FALSE)

    has_example <- any(stringr::str_detect(lines, r"{\\example}"))
    expect_true(has_example, label = paste("File:", file))
  }
})
