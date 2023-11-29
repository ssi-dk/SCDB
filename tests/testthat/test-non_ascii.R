test_that("Code contains no non-ASCII characters", {

  # Load .Rd files based on environment
  # When using R-CMD-Check the deployment is different from when using devtools::test().
  # Deployment on Github is also different from running R-CMD-Check locally
  # Here we need to read directly from a .rdx database stored in the "help" folder
  # If we are testing locally, we read from the "man" folder
  # Note that `devtools::test()`, `devtools::check()` and GitHub workflows all have different
  # folder structures during testing, so we need to account for these differences

  # Look for the source of .Rd files
  pkg_path <- base::system.file("", package = "SCDB")

  help_dir <- file.path(pkg_path, "help")
  man_dir  <- file.path(pkg_path, "man")

  expect_true(any(dir.exists(c(help_dir, man_dir))))

  if (checkmate::test_directory_exists(help_dir)) {

    rdx_file <- purrr::keep(dir(help_dir, full.names = TRUE), ~ stringr::str_detect(., ".rdx$"))
    rd_envir <- new.env()
    lazyLoad(stringr::str_remove(rdx_file, ".rdx$"), envir = rd_envir)
    rd_names <- ls(rd_envir)
    rd_files <- purrr::map(rd_names, ~ as.character(eval(purrr::pluck(rd_envir, .))))
    names(rd_files) <- paste0(rd_names, ".Rd")

  } else if (checkmate::test_directory_exists(man_dir)) {

    rd_paths <- dir(man_dir, pattern = "[.][Rr][Dd]$", full.names = TRUE)
    rd_files <- purrr::map(rd_paths, readLines)
    names(rd_files) <- purrr::map_chr(rd_paths, basename)

  } else {

    stop(".Rd files could not be located")

  }

  # Skip the "*-package.Rd" file
  rd_files <- rd_files[!stringr::str_detect(names(rd_files), "-package.[Rr][Dd]$")]

  r_dir <- file.path(pkg_path, "R")

  if (checkmate::test_directory_exists(r_dir)) {

    r_paths <- purrr::keep(dir(r_dir, full.names = TRUE), ~ stringr::str_detect(., ".[Rr]$"))
    r_files <- purrr::map(r_paths, readLines)
    names(r_paths) <- purrr::map_chr(r_paths, basename)

  } else {

    stop(".R files could not be located")

  }

  files_to_check <- c(r_files, rd_files)

  # Check the files
  for (file_id in seq_along(files_to_check)) {
    non_ascii_line <- stringr::str_detect(files_to_check[[file_id]], r"{[^\x00-\x7f]}")

    if (any(non_ascii_line)) {
      rel_path <- stringr::str_remove(c(r_paths, rd_paths)[file_id], paste0(pkg_path, "/?"))
      n_lines <- which(non_ascii_line)

      msg <- sprintf("Non-ASCII character(s) in file %s line(s) %s",
                     rel_path,
                     paste(n_lines, collapse = ", "))

      fail(msg)
    } else {
      succeed()
    }
  }
})
