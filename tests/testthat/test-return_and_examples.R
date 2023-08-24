test_that("Exported code has @return and @examples", {
  pkg_dir <- stringr::str_remove(getwd(), "/tests/testthat")

  r_files  <- list.files(path = file.path(pkg_dir, "R"),   pattern = r"{\.[Rr]$}",     full.names = TRUE)

  files_to_check <- r_files

  for (file in files_to_check) {
    lines <- readLines(file, warn = FALSE)

    code_blocks <- lines |>
      stringr::str_detect("^#'") |>
      (\(.) . & !c(F, .[-length(.)]))() |>
      cumsum() |>
      (\(.) split(lines, .))()

    # Truncate
    code_blocks <- purrr::map(code_blocks, ~ purrr::discard(., cumprod(stringr::str_detect(., "^$")) == 1))

    # Look for @export tag
    exported_code_blocks <- purrr::keep(code_blocks, ~ any(stringr::str_detect(., "@export")))

    # Look for missing @return tag
    no_return <- purrr::discard(exported_code_blocks, ~ any(stringr::str_detect(., "@return")))

    # Look for missing @examples tag
    no_examples <- purrr::discard(exported_code_blocks, ~ any(stringr::str_detect(., "@example")))

    # Report issues
    for (block in no_return) {
      function_with_issue <- purrr::keep(block, ~ stringr::str_detect(., r"{(?<= <-) (function\(|R6::R6Class)}")) |>
        stringr::str_extract(r"{^[\w\.%`]*(?= <-)}")

      expect_true(any(stringr::str_detect(block, "@return")),
                  label = glue::glue("{function_with_issue} is exported but has no @return"))
    }

    # Report issues
    for (block in no_examples) {
      function_with_issue <- purrr::keep(block, ~ stringr::str_detect(., r"{(?<= <-) function\(}")) |>
        stringr::str_extract(r"{^[\w\.%`]*(?= <-)}")

      expect_true(any(stringr::str_detect(block, "@example")),
                  label = glue::glue("{function_with_issue} is exported but has no @example(s)"))
    }
  }
})
