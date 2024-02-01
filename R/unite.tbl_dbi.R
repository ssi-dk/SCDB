#' tidyr::unite for tbl_dbi
#'
#' @inheritParams tidyr::unite
#' @examples
#' library(tidyr, warn.conflicts = FALSE)
#' df <- expand_grid(x = c("a", NA), y = c("b", NA))
#' df

#' df %>% unite("z", x:y, remove = FALSE)
#' # To remove missing values:
#* df %>% unite("z", x:y, na.rm = TRUE, remove = FALSE)
#'
#' # Separate is almost the complement of unite
#' df %>%
#'   unite("xy", x:y) %>%
#'   separate(xy, c("x", "y"))
# (but note `x` and `y` contain now "NA" not NA)
#' @importFrom rlang :=
#' @return A tbl_dbi with the specified columns united into a new column named according to "col"
#' @exportS3Method tidyr::unite tbl_dbi
unite.tbl_dbi <- function(data, col, ..., sep = "_", remove = TRUE, na.rm = FALSE) { # nolint: object_name_linter

  # Modified from
  # https://stackoverflow.com/questions/48536983/how-to-concatenate-strings-of-multiple-  -- continued below
  # columns-from-table-in-sql-server-using-dp

  # Check arguments
  checkmate::assert_class(data, "tbl_dbi")
  checkmate::assert_character(sep)
  checkmate::assert_logical(remove)
  checkmate::assert_logical(na.rm)

  # Code below is adapted from tidyr::unite.data.frame
  rlang::check_dots_unnamed()

  if (rlang::dots_n(...) == 0) {
    from_vars <- colnames(data)
  } else {
    from_vars <- colnames(dplyr::select(data, ...))
  }

  # We need add some support for how tidyr::unite accepts input of "col"
  col <- rlang::as_string(rlang::ensym(col))

  col_symbols <- purrr::map(from_vars, as.symbol)

  # We need to determine where col should be placed
  first_from <- which(colnames(data) %in% from_vars)[1]

  # CONCAT_WS does not exist in SQLite
  if (inherits(data, "tbl_SQLiteConnection")) {
    out <- data |>
      dplyr::mutate({{col}} := NULLIF(paste(!!!col_symbols, sep = sep), ""), .before = !!first_from)
  } else {
    out <- data |>
      dplyr::mutate({{col}} := NULLIF(CONCAT_WS(sep, !!!col_symbols), ""), .before = !!first_from)
  }

  if (remove) out <- out |> dplyr::select(!tidyselect::all_of(from_vars))

  return(out)
}
