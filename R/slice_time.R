#' Slices a data object based on time / date
#'
#' @template .data
#' @param slice_ts (`POSIXct(1)`, `Date(1)`, or `character(1)`)\cr
#'   The time / date to slice by
#' @param from_ts,until_ts (`character(1)`)\cr
#'   The name of the columns in .data specifying valid from and valid until time
#' @template .data_return
#' @examples
#'   conn <- get_connection(drv = RSQLite::SQLite())
#'
#'   m <- mtcars |>
#'     dplyr::mutate(
#'       "from_ts" = dplyr::if_else(dplyr::row_number() > 10,
#'                                  as.Date("2020-01-01"),
#'                                  as.Date("2021-01-01")),
#'       "until_ts" = as.Date(NA))
#'
#'   dplyr::copy_to(conn, m, name = "mtcars", temporary = FALSE)
#'
#'   q <- dplyr::tbl(conn, id("mtcars", conn))
#'
#'   nrow(slice_time(q, "2020-01-01")) # 10
#'   nrow(slice_time(q, "2021-01-01")) # nrow(mtcars)
#'
#'   close_connection(conn)
#' @export
slice_time <- function(.data, slice_ts, from_ts = "from_ts", until_ts = "until_ts") {

  # Check arguments
  coll <- checkmate::makeAssertCollection()
  assert_data_like(.data, add = coll)
  assert_timestamp_like(slice_ts, add = coll)
  checkmate::assert_character(from_ts, add = coll)
  checkmate::assert_character(until_ts, add = coll)
  checkmate::reportAssertions(coll)

  .data <- .data |>
    dplyr::filter(dplyr::if_any(tidyselect::all_of(from_ts), ~ . <= !!slice_ts),
                  dplyr::if_any(tidyselect::all_of(until_ts), ~ is.na(.) | !!slice_ts < .))

  return(.data)
}
