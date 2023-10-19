test_that(r"{.Rd files have \Value}", {

  # Load .Rd files based on environment
  # When using R-CMD-Check the deployment is different from when using devtools::test().
  # Deployment on Github is also different from running R-CMD-Check locally
  # Here we need to read directly from a .rdx database stored in the "help" folder
  # If we are testing locally, we read from the "man" folder
  # Note that `devtools::test()`, `devtools::check()` and GitHub workflows all have different
  # folder structures during testing, so we need to account for these differences

  # Platform independent regex search to find root folder
  path_regex <- stringr::str_replace(r"{(.*\/\w*(.Rcheck)?)\/.*(?=\/testthat)}", "/", .Platform$file.sep)
  pkg_path <- stringr::str_extract(getwd(), path_regex, group = 1)

  # If path contains .Rcheck, we need to add the package name to the path
  pkg_path <- stringr::str_replace(pkg_path, r"{(\w*)(\.Rcheck)}", paste0("\\1\\2", .Platform$file.sep, "\\1"))

  # Look for the source of .Rd files
  help_dir <- file.path(pkg_path, "help")
  man_dir  <- file.path(pkg_path, "man")

  if (checkmate::test_directory_exists(help_dir)) {

    rdx_file <- purrr::keep(dir(help_dir, full.names = TRUE), ~ stringr::str_detect(., ".rdx$"))
    rd_envir <- new.env()
    lazyLoad(stringr::str_remove(rdx_file, ".rdx$"), envir = rd_envir)
    rd_names <- ls(rd_envir)
    rd_files <- purrr::map(rd_names, ~ as.character(eval(purrr::pluck(rd_envir, .))))
    names(rd_files) <- paste0(rd_names, ".Rd")

  } else if (checkmate::test_directory_exists(man_dir)) {

    rd_paths <- purrr::keep(dir(man_dir, full.names = TRUE), ~ stringr::str_detect(., ".[Rr][Dd]$"))
    rd_files <- purrr::map(rd_paths, readLines)
    names(rd_files) <- purrr::map_chr(rd_paths, basename)

  } else {

    stop(".Rd files could not be located")

  }


  # Skip the "*-package.Rd" file
  rd_files <- rd_files[!stringr::str_detect(names(rd_files), "-package.[Rr][Dd]$")]

  # Check renaming
  for (rd_id in seq_along(rd_files)) {
    has_value <- any(stringr::str_detect(rd_files[[rd_id]], r"{\\value}"))
    expect_true(has_value, label = paste("File:", names(rd_files)[[rd_id]]))
  }
})
