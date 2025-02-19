#' @title
#'   The custom linters of `diseasy`
#' @description
#'   A curated list of linters to ensure adherence to the `diseasy` documentation and code standards
#' @name diseasy_linters
#' @examples
#'   diseasy_code_linters()
#' @return A list of linters
#' @noRd
diseasy_code_linters <- function() {
  linters <- c(
    list(
      "nolint_position_linter" = nolint_position_linter(length = 120L),
      "nolint_line_length_linter" = nolint_line_length_linter(length = 120L),
      "non_ascii_linter" = non_ascii_linter(),
      "param_and_field_linter" = param_and_field_linter(),
      "documentation_template_linter" = documentation_template_linter()
    ),
    lintr::all_linters(
      object_length_linter = lintr::object_length_linter(length = 40L), # We allow for longer variable names
      line_length_linter = NULL,          # We use 120, nolint-aware line length linter instead
      cyclocomp_linter = NULL,            # Not required in diseasy style guide
      keyword_quote_linter = NULL,        # Quoting variables names improves readability over R defaults
      implicit_integer_linter = NULL,     # Flags too many cases where integer status is not relevant
      nonportable_path_linter = NULL,     # Any \\ is flagged. Therefore fails when escaping backslashes
      undesirable_function_linter = NULL, # Library calls in vignettes are flagged and any call to options
      unnecessary_lambda_linter = NULL,   # Fails for purrr::map with additional function arguments
      unnecessary_nesting_linter = NULL,  # Fails for purrr::map with additional function arguments
      strings_as_factors_linter = NULL,   # Seems to be some backwards compatibility stuff.
      return_linter = NULL,               # We use explicit return statements
      one_call_pipe_linter = NULL,        # One-call pipes can improve readability and code mobility
      nested_pipe_linter = NULL,          # Nested pipes can improve readability and code mobility
      nzchar_linter = NULL,               # Testing for equality is simpler, more readable, and less janky
      object_overwrite_linter = NULL      # It is reasonable to use simple names for things. Use scoping (::) instead
    )
  )

  return(linters)
}


#' @rdname diseasy_linters
#' @description
#' nolint_position_linter: Ensure `nolint:` statements occur after the character limit
#'
#' @param length (`numeric`)\cr
#'  Maximum line length allowed. Default is 80L (Hollerith limit).
#' @returns A list of `lintr::Lint`
#' @examples
#' ## nolint_position_linter
#' # will produce lints
#' lintr::lint(
#'   text = paste0(strrep("x", 15L), "# nolint: object_name_linter"),
#'   linters = c(nolint_position_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' # okay
#' lintr::lint(
#'   text = paste0(strrep("x", 20L), "# nolint: object_name_linter"),
#'   linters = c(nolint_position_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' @seealso
#' - [lintr::linters] for a complete list of linters available in lintr.
#' - <https://style.tidyverse.org/syntax.html#long-lines>
#' @importFrom rlang .data
#' @noRd
nolint_position_linter <- function(length = 80L) {
  general_msg <- paste("`nolint:` statements start at", length + 1, "characters.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      nolint_info <- source_expression$content |>
        stringr::str_locate_all(stringr::regex(r"{# *nolint}", ignore_case = TRUE))

      nolint_info <- purrr::imap(nolint_info, ~ dplyr::mutate(as.data.frame(.x), line_number = .y)) |>
        purrr::reduce(rbind) |>
        dplyr::filter(!is.na(.data$start)) |>
        dplyr::filter(.data$start <= length)

      purrr::pmap(
        nolint_info,
        \(start, end, line_number) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            column_number = start,
            type = "style",
            message = paste(general_msg, "This statement starts at", start, "characters"),
            line = source_expression$file_lines[line_number],
            ranges = list(c(start, end))
          )
        }
      )
    }
  )
}


#' @rdname diseasy_linters
#' @description
#' nolint_line_length_linter: Ensure lines adhere to a given character limit, ignoring `nolint` statements
#'
#' @param length (`numeric`)\cr
#'  Maximum line length allowed.
#' @param code_block_length (`numeric`)\cr
#'  Maximum line length allowed for code blocks.
#' @examples
#' ## nolint_line_length_linter
#' # will produce lints
#' lintr::lint(
#'   text = paste0(strrep("x", 25L), "# nolint: object_name_linter."),
#'   linters = c(nolint_line_length_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' # okay
#' lintr::lint(
#'   text = paste0(strrep("x", 20L), "# nolint: object_name_linter."),
#'   linters = c(nolint_line_length_linter(length = 20L), lintr::object_name_linter())
#' )
#'
#' @importFrom rlang .data
#' @noRd
nolint_line_length_linter <- function(length = 80L, code_block_length = 85L) {
  general_msg <- paste("Lines should not be more than", length, "characters.")
  code_block_msg <- paste("Code blocks should not be more than", code_block_length, "characters.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      nolint_regex <- r"{\s*# ?no(lint|cov) ?(start|end)?:?.*}"

      file_lines_nolint_excluded <- source_expression$file_lines |>
        purrr::map_chr(\(s) stringr::str_remove(s, nolint_regex))

      # Switch mode based on extension
      # .Rmd uses code_block_length
      code_block <- endsWith(tolower(source_expression$filename), ".rmd")

      line_lengths <- nchar(file_lines_nolint_excluded)
      long_lines <- which(line_lengths > ifelse(code_block, code_block_length, length))
      Map(function(long_line, line_length) {
        lintr::Lint(
          filename = source_expression$filename,
          line_number = long_line,
          column_number = ifelse(code_block, code_block_length, length) + 1L, type = "style",
          message = paste(ifelse(code_block, code_block_msg, general_msg), "This line is", line_length, "characters."),
          line = source_expression$file_lines[long_line],
          ranges = list(c(1L, line_length))
        )
      }, long_lines, line_lengths[long_lines])
    }
  )
}


#' @rdname diseasy_linters
#' @description
#' non_ascii_linter: Ensure the code base only contains ASCII symbols
#'
#' @examples
#' ## non_ascii_linter
#' # will produce lints
#' lintr::lint(
#'   text = "a-å",                                                                                                      # nolint: non_ascii_linter
#'   linters = non_ascii_linter()
#' )
#'
#' # okay
#' lintr::lint(
#'   text = "a-z",                                                                                                      # nolint: non_ascii_linter
#'   linters = non_ascii_linter()
#' )
#'
#' @importFrom rlang .data
#' @noRd
non_ascii_linter <- function() {
  general_msg <- paste("Code should not contain non-ASCII characters")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      detection_info <- source_expression$file_lines |>
        stringr::str_locate_all(stringr::regex(r"{[^\x00-\x7f]}", ignore_case = TRUE))

      detection_info <- purrr::imap(detection_info, ~ dplyr::mutate(as.data.frame(.x), line_number = .y))

      detection_info <- detection_info |>
        purrr::reduce(rbind) |>
        dplyr::filter(!is.na(.data$start))

      purrr::pmap(
        detection_info,
        \(start, end, line_number) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            column_number = start,
            type = "style",
            message = paste(general_msg, "non-ASCII character found"),
            line = source_expression$file_lines[line_number],
            ranges = list(c(start, end))
          )
        }
      )
    }
  )
}


#' @name diseasy_linters
#' @description
#' param_and_field_linter: Ensure R6 @param and @field tags have a unit-type and carriage return
#'
#' @examples
#' ## param_and_field_linter
#' # will produce lints
#' lintr::lint(
#'   text = "#' @param test",                                                                                           # nolint: param_and_field_linter
#'   linters = param_and_field_linter()
#' )
#'
#' # okay
#' lintr::lint(
#'   text = "#' @param test (`numeric()`)\cr",
#'   linters = param_and_field_linter()
#' )
#' @importFrom rlang .data
#' @noRd
param_and_field_linter <- function() {
  general_msg <- "@param and @field should follow mlr3 format."

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      # Find all @param and @field lines. All other lines become NA
      detection_info <- source_expression$file_lines |>
        stringr::str_extract(r"{#' ?@(param|field).*}")

      # Convert to data.frame and determine line number
      detection_info <- data.frame(
        rd_line = detection_info,
        line_number = seq_along(detection_info)
      )

      # Remove non param/field lines and determine the type
      detection_info <- detection_info |>
        dplyr::filter(!is.na(.data$rd_line)) |>
        dplyr::mutate(rd_type = stringr::str_extract(.data$rd_line, r"{@(param|field)}"))

      # Remove triple-dot-ellipsis params
      detection_info <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, "@param +\\.{3}"))

      # Remove auto-generated documentation
      detection_info <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, r"{@(param|field) +[\.\,\w]+ +`r }"))



      # Look for malformed tags
      missing_backticks <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, stringr::fixed("`")))

      missing_cr <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, stringr::fixed(r"{\cr}")))

      # report issues
      backtick_lints <- purrr::pmap(
        missing_backticks,
        \(rd_line, line_number, rd_type) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            type = "style",
            message = glue::glue("{general_msg} {rd_type} type not declared (rd-tag is missing backticks ` )"),
            line = source_expression$file_lines[line_number]
          )
        }
      )

      cr_lints <- purrr::pmap(
        missing_cr,
        \(rd_line, line_number, rd_type) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            type = "style",
            message = glue::glue("{general_msg} {rd_type} is missing a carriage return (\\cr) after type-declaration"),
            line = source_expression$file_lines[line_number]
          )
        }
      )

      return(c(backtick_lints, cr_lints))
    }
  )
}


#' @rdname diseasy_linters
#' @description
#' documentation_template_linter: Ensure documentation templates are used if available.
#'
#' @examples
#' ## documentation_template_linter
#' rd_parameter <- "(`character`)\cr Description of parameter" # Create a template for the "parameter" parameter
#'
#' # will produce lints
#' lintr::lint(
#'   text = "#' @param parameter (`character`)\cr Description of parameter",                                            # nolint: documentation_template_linter
#'   linters = documentation_template_linter()
#' )
#'
#' # okay
#' lintr::lint(
#'   text = "#' @param parameter `r rd_parameter`",
#'   linters = documentation_template_linter()
#' )
#'
#' @importFrom rlang .data
#' @noRd
documentation_template_linter <- function() {
  general_msg <- paste("Documentation templates should used if available.")

  lintr::Linter(
    function(source_expression) {

      # Only go over complete file
      if (!lintr::is_lint_level(source_expression, "file")) {
        return(list())
      }

      # Find all @param and @field lines. All other lines become NA
      detection_info <- source_expression$file_lines |>
        stringr::str_extract(r"{#' ?@(param|field).*}")

      # Convert to data.frame and determine line number
      detection_info <- data.frame(
        rd_line = detection_info,
        line_number = seq_along(detection_info)
      )

      # Remove non param/field lines
      detection_info <- detection_info |>
        dplyr::filter(!is.na(.data$rd_line))

      # Remove triple-dot-ellipsis params
      detection_info <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, "@param +\\.{3}"))

      # Remove auto-generated documentation
      detection_info <- detection_info |>
        dplyr::filter(!stringr::str_detect(.data$rd_line, r"{@(param|field) +[\.\w]+ +`r }"))

      # Extract the parameter
      detection_info <- detection_info |>
        dplyr::mutate("param" = stringr::str_extract(.data$rd_line, r"{(@(param|field) +)([\.\w]+)}", group = 3))

      # Detect if template exists
      detection_info <- detection_info |>
        dplyr::mutate("rd_template" = paste0("rd_", .data$param)) |>
        dplyr::filter(.data$rd_template %in% names(as.list(base::getNamespace(devtools::as.package(".")$package)))) |>
        dplyr::select(!"param")

      purrr::pmap(
        detection_info,
        \(rd_line, line_number, rd_template) {
          lintr::Lint(
            filename = source_expression$filename,
            line_number = line_number,
            type = "style",
            message = paste(general_msg, "Template", rd_template, "available."),
            line = source_expression$file_lines[line_number]
          )
        }
      )
    }
  )
}
