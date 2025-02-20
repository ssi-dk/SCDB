#' Additional items for the `usethis::use_release_issue()` function
#' @noRd
release_bullets <- function() {
  return(
    c(
      "Update the benchmarks",
      "Run `styler::style_pkg(style = diseasy_style)` and check the changes"
    )
  )
}


#' @title
#'  The custom styler of `diseasy`
#' @description
#'  A custom styler that is based on the `tidyverse` style but with some modifications to better fit
#'  the `diseasy` codebase.
#' @name diseasy_style
#' @noRd
diseasy_style <- function(...) {
  transformers <- styler::tidyverse_style(...)

  # The transformers from tidyverse_style is a good starting point but
  # we to disable some transformers

  # Breaks spacing in square brackets (i.e. [1, , ])
  transformers$space$remove_space_before_closing_paren <- NULL
  transformers$space$remove_space_before_comma <- NULL

  # The following spacing rules breaks our 120 placing of the #nolint statements
  # spacing_around_op, remove_space_after_opening_paren, and spacing_before_comments

  # We disable two of them and modify the last
  transformers$space$spacing_around_op <- NULL
  transformers$space$spacing_before_comments <- NULL


  transformers$space$remove_space_after_opening_paren <- function(pd_flat) {
    paren_after <- pd_flat$token %in% c("'('", "'['", "LBB")
    if (!any(paren_after)) {
      return(pd_flat)
    }
    pd_flat$spaces[paren_after & (pd_flat$newlines == 0L) & pd_flat$token_after != "COMMENT"] <- 0L
    pd_flat
  }



  # The "remove_line_break_in_fun_call" rule works poorly with R6 classes
  # and condenses the the entire code base to an unreadable level.
  transformers$line_break$remove_line_break_in_fun_call <- NULL

  # The "style_line_break_around_curly" rule in general
  # removes spacing that helps readability.
  transformers$line_break$style_line_break_around_curly <- NULL

  # Breaks multi-line function declarations in R6 classes
  transformers$line_break$remove_line_breaks_in_fun_dec <- NULL
  transformers$indention$unindent_fun_dec <- NULL
  transformers$indention$update_indention_ref_fun_dec <- NULL

  # Breaks one-line switch statements
  transformers$line_break$set_line_break_after_opening_if_call_is_multi_line <- NULL

  transformers
}
