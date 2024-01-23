#' Slices a data object based on time / date
#'
#' @template .data
#' @param slice_ts The time / date to slice by
#' @param from_ts  The name of the column in .data specifying valid from time (note: must be unquoted)
#' @param until_ts The name of the column in .data specifying valid until time (note: must be unquoted)
#' @template .data_return
#' @examples
#' conn <- get_connection(drv = RSQLite::SQLite())
#'
#' m <- mtcars |>
#'   dplyr::mutate(from_ts = dplyr::if_else(dplyr::row_number() > 10,
#'                                          as.Date("2020-01-01"),
#'                                          as.Date("2021-01-01")),
#'                 until_ts = as.Date(NA))
#'
#' dplyr::copy_to(conn, m, name = "mtcars", temporary = FALSE)
#'
#' q <- dplyr::tbl(conn, id("mtcars", conn))
#'
#' nrow(slice_time(q, "2020-01-01")) # 10
#' nrow(slice_time(q, "2021-01-01")) # nrow(mtcars)
#'
#' close_connection(conn)
#' @export
slice_time <- function(.data, slice_ts, from_ts = from_ts, until_ts = until_ts) {

  # Check arguments
  assert_data_like(.data)
  assert_timestamp_like(slice_ts)

  from_ts  <- dplyr::enquo(from_ts)
  until_ts <- dplyr::enquo(until_ts)
  .data <- .data |>
    dplyr::filter(is.na({{until_ts}}) | slice_ts < {{until_ts}},
                  {{from_ts}} <= slice_ts)
  return(.data)
}
