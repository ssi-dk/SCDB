#' Filters .data according to all records in the filter
#'
#' @description
#'   If `filters` is `NULL`, no filtering is done.
#'   Otherwise, the `.data` object is filtered via an `inner_join()` using all columns of the filter:
#'   \code{inner_join(.data, filter, by = colnames(filter))}
#'
#'   `by` and `na_by` can overwrite the `inner_join()` columns used in the filtering.
#'
#' @template .data
#' @template filters
#' @inheritParams dbplyr::join.tbl_sql
#' @param na_by (`character()`)\cr
#'   Columns where NA should match with NA.
#' @param ... Further arguments passed to `dplyr::inner_join()`.
#' @template .data_return
#' @examples
#'   # Filtering with null means no filtering is done
#'   filter <- NULL
#'   identical(filter_keys(mtcars, filter), mtcars) # TRUE
#'
#'   # Filtering by vs = 0
#'   filter <- data.frame(vs = 0)
#'   identical(filter_keys(mtcars, filter), dplyr::filter(mtcars, vs == 0)) # TRUE
#'
#'   # Filtering by the specific combinations of vs = 0 and am = 1
#'   filter <- dplyr::distinct(mtcars, vs, am)
#'   filter_keys(mtcars, filter)
#'
#' @importFrom rlang .data
#' @export
filter_keys <- function(.data, filters, by = NULL, na_by = NULL, ...) {
  if (is.null(filters)) {
    return(.data)
  }

  assert_data_like(.data)
  assert_data_like(filters, null.ok = TRUE)
  checkmate::assert_subset(c(by, na_by), colnames(filters))

  UseMethod("filter_keys")
}

#' @export
filter_keys.tbl_sql <- function(.data, filters, by = NULL, na_by = NULL, ...) {

  if (is.null(by) && is.null(na_by)) {
    # Determine key types
    key_types <- filters %>%
      dplyr::ungroup() %>%
      dplyr::summarise(dplyr::across(
        .cols = tidyselect::everything(),
        .fns = ~ sum(ifelse(is.na(.), 0, 1), na.rm = TRUE)                                                              # nolint: redundant_ifelse_linter
      )) %>%
      tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

    by    <- key_types %>% dplyr::filter(.data$is_na > 0) %>% dplyr::pull("column_name")
    na_by <- key_types %>% dplyr::filter(.data$is_na == 0) %>% dplyr::pull("column_name")

    if (length(by) == 0)    by    <- NULL
    if (length(na_by) == 0) na_by <- NULL
  }
  return(dplyr::inner_join(.data, filters, by = by, na_by = na_by, ...))
}

#' @export
filter_keys.data.frame <- function(.data, filters, by = NULL, na_by = NULL, ...) {
  if (is.null(by) && is.null(na_by)) by <- colnames(filters)
  return(dplyr::inner_join(.data, filters, by = c(by, na_by), ...))
}
