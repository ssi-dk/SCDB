#' Filters .data according to all records in the filter
#'
#' @description
#' If filter = NULL, not filtering is done
#' If filter is different from NULL, the .data is filtered by a inner_join using all columns of the filter:
#' \code{inner_join(.data, filter, by = colnames(filter))}
#'
#' by and na_by can overwrite the inner_join columns used in the filtering
#'
#' @template .data
#' @template filters
#' @param by      passed to inner_join if different from NULL
#' @param na_by   passed to inner_join if different from NULL
#' @template .data_return
#' @examples
#' # Filtering with null means no filtering is done
#' filter <- NULL
#' identical(filter_keys(mtcars, filter), mtcars) # TRUE
#'
#' # Filtering by vs = 0
#' filter <- data.frame(vs = 0)
#' identical(filter_keys(mtcars, filter), dplyr::filter(mtcars, vs == 0)) # TRUE
#'
#' # Filtering by the specific combinations of vs = 0 and am = 1
#' filter <- dplyr::distinct(mtcars, vs, am)
#' filter_keys(mtcars, filter)
#'
#' @importFrom rlang .data
#' @export
filter_keys <- function(.data, filters, by = NULL, na_by = NULL) {
  if (is.null(filters)) {
    return(.data)
  }

  assert_data_like(.data)
  assert_data_like(filters, null.ok = TRUE)
  checkmate::assert_subset(c(by, na_by), colnames(filters))

  UseMethod("filter_keys")
}

#' @export
filter_keys.tbl_sql <- function(.data, filters, by = NULL, na_by = NULL) {

  if (is.null(by) && is.null(na_by)) {
    # Determine key types
    key_types <- filters |>
      dplyr::ungroup() |>
      dplyr::summarise(dplyr::across(
        .cols = tidyselect::everything(),
        .fns = ~ sum(as.numeric(is.na(.)), na.rm = TRUE)
      )) |>
      tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

    by    <- key_types |> dplyr::filter(.data$is_na > 0) |> dplyr::pull("column_name")
    na_by <- key_types |> dplyr::filter(.data$is_na == 0)  |> dplyr::pull("column_name")

    if (length(by) == 0)    by    <- NULL
    if (length(na_by) == 0) na_by <- NULL
  }
  return(dplyr::inner_join(.data, filters, by = by, na_by = na_by))
}

#' @export
filter_keys.data.frame <- function(.data, filters, by = NULL, na_by = NULL) {
  if (is.null(by) && is.null(na_by)) by <- colnames(filters)
  return(dplyr::inner_join(.data, filters, by = c(by, na_by)))
}
