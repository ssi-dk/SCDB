test_field_in_documentation <- function(field) {

  # Load .Rd files based on environment
  # When using R-CMD-Check the deployment is different from when using devtools::test().
  # Deployment on Github is also different from running R-CMD-Check locally
  # Here we need to read directly from a .rdx database stored in the "help" folder
  # If we are testing locally, we read from the "man" folder
  # Note that `devtools::test()`, `devtools::check()` and GitHub workflows all have different
  # folder structures during testing, so we need to account for these differences

  # Look for the source of .Rd files
  help_dir <- system.file("help", package = testthat::testing_package())
  man_dir <- system.file("man", package = testthat::testing_package())

  testthat::expect_true(any(dir.exists(c(help_dir, man_dir))))

  if (checkmate::test_directory_exists(help_dir)) {

    rdx_file <- purrr::keep(dir(help_dir, full.names = TRUE), ~ stringr::str_detect(., ".rdx$"))
    rd_envir <- new.env()
    lazyLoad(stringr::str_remove(rdx_file, ".rdx$"), envir = rd_envir)
    rd_names <- ls(rd_envir)
    rd_files <- rd_names |>
      purrr::map(~ as.character(eval(purrr::pluck(rd_envir, .)))) |>
      purrr::map_chr(~ paste(., collapse = ""))
    names(rd_files) <- paste0(rd_names, ".Rd")

  } else if (checkmate::test_directory_exists(man_dir)) {

    rd_paths <- purrr::keep(dir(man_dir, full.names = TRUE), ~ stringr::str_detect(., ".[Rr][Dd]$"))
    rd_files <- purrr::map(rd_paths, readLines)
    names(rd_files) <- purrr::map_chr(rd_paths, basename)

  } else {

    stop(".Rd files could not be located", call. = FALSE)

  }


  # Skip the "*-package.Rd" file
  rd_files <- rd_files[!stringr::str_detect(names(rd_files), "-package.[Rr][Dd]$")]

  # Skip the "data" files
  rd_files <- purrr::discard(rd_files, ~ any(stringr::str_detect(., r"{\\+keyword\\?\{data\\?\}}")))                    # nolint: absolute_path_linter

  # Check renaming
  for (rd_id in seq_along(rd_files)) {
    has_field <- any(stringr::str_detect(rd_files[[rd_id]], paste0(r"{\\}", field)))
    testthat::expect_true(has_field, label = paste("File:", names(rd_files)[[rd_id]]))
  }
}


test_that(r"{.Rd files have \examples}", {
  test_field_in_documentation("example")
})


test_that(r"{.Rd files have \value}", {
  test_field_in_documentation("value")
})
