#' @param filters (`data.frame(1)`, `tibble(1)`, `data.table(1)`, or `tbl_dbi(1)`)\cr
#'   A object subset data by.
#'   If filters is `NULL`, no filtering occurs.
#'   Otherwise, an `inner_join()` is performed using all columns of the filter object.
