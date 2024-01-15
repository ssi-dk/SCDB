#' Computes an MD5 checksum from columns
#'
#' @name digest_to_checksum
#'
#' @template .data
#' @param col Name of the column to put the checksums in
#' @param warn Flag to warn if target column already exists in data
#' @param exclude Columns to exclude from the checksum generation
#' @examples
#' digest_to_checksum(mtcars)
#'
#' @return .data with an checksum column added
#' @export
digest_to_checksum <- function(.data, col = "checksum", exclude = NULL, warn = TRUE) {

  # Check arguments
  checkmate::assert_character(col)
  checkmate::assert_logical(warn)

  if (as.character(dplyr::ensym(col)) %in% colnames(.data) && warn) {
    warning("Column ",
            as.character(dplyr::ensym(col)),
            " already exists in data and will be overwritten!")
  }

  colnames <- .data |>
    dplyr::select(!tidyselect::any_of(c(col, exclude))) |>
    colnames()

  .data <- .data |>
    dplyr::select(!tidyselect::any_of(col)) |>
    dplyr::mutate(dplyr::across(
      tidyselect::all_of(colnames),
      ~ dplyr::coalesce(as.character(.), ""),
      .names = "{.col}.__chr"
    ))

  return(digest_to_checksum_internal(.data, col))
}

#' @template .data
#' @param col The name of column the checksums will be placed in
#' @inherit digest_to_checksum return
#' @noRd
digest_to_checksum_internal <- function(.data, col) {
  UseMethod("digest_to_checksum_internal")
}

digest_to_checksum_internal.default <- function(.data, col) {

  # Compute checksums locally then join back onto original data
  checksums <- .data |>
    dplyr::collect() |>
    tidyr::unite(col, tidyselect::ends_with(".__chr")) |>
    dplyr::transmute(id__ = dplyr::row_number(),
                     checksum = openssl::md5({{ col }}))

  .data <- .data |>
    dplyr::mutate(id__ = dplyr::row_number()) |>
    dplyr::left_join(checksums, by = "id__", copy = TRUE) |>
    dplyr::select(!c(tidyselect::ends_with(".__chr"), "id__"))

  return(.data)
}

#'
# It seems we need to do more hacking since
# @importFrom openssl md5 does not work in the below usecase.
# defining md5 here succesfully causes local objects to use the openssl md5 function
# and remote objects to use their own md5 functions.
md5 <- openssl::md5

# Some backends have native md5 support, these use this function
digest_to_checksum_native_md5 <- function(.data, col) {

  .data <- .data |>
    tidyr::unite(!!col, tidyselect::ends_with(".__chr"), remove = TRUE) |>
    dplyr::mutate(dplyr::across(tidyselect::all_of(col), md5))

  return(.data)
}

digest_to_checksum_internal.tbl_PqConnection <- digest_to_checksum_native_md5

digest_to_checksum_internal.data.frame       <- digest_to_checksum_native_md5

digest_to_checksum_internal.tibble           <- digest_to_checksum_native_md5
