#' Computes an checksum from columns
#'
#' @details
#'   In most cases, the md5 algorithm is used to compute the checksums.
#'   For Microsoft SQL Server, the SHA-256 algorithm is used.
#'
#' @name digest_to_checksum
#'
#' @template .data
#' @param col (`character(1)`)\cr
#'   Name of the column to put the checksums in. Will be generated if missing.
#' @param exclude (`character()`)\cr
#'   Columns to exclude from the checksum generation.
#' @param warn (`logical(1)`)\cr
#'   Warn if `col` exists in the input data?
#' @return
#'   .data with a checksum column added.
#' @examples
#'   digest_to_checksum(mtcars)
#' @export
digest_to_checksum <- function(.data, col = "checksum", exclude = NULL, warn = TRUE) {

  # Check arguments
  assert_data_like(.data)
  checkmate::assert_character(col)
  checkmate::assert_character(exclude, null.ok = TRUE)

  if (warn && as.character(dplyr::ensym(col)) %in% colnames(.data)) {
    warning(
      glue::glue("Column {as.character(dplyr::ensym(col))} already exists in data and will be overwritten!"),
      call. = FALSE
    )
  }

  UseMethod("digest_to_checksum", .data)
}

#' @export
`digest_to_checksum.tbl_Microsoft SQL Server` <- function(
  .data,
  col = formals(digest_to_checksum)$col,
  exclude = formals(digest_to_checksum)$exclude,
  ...
) {

  hash_cols <- dbplyr::ident(setdiff(colnames(.data), c(col, exclude)))

  .data <- .data |>
    dplyr::mutate(
      {{ col }} := !!dplyr::sql(
        glue::glue("CONVERT(CHAR(64), HashBytes('SHA2_256', (SELECT {toString(hash_cols)} FOR XML RAW)), 2)")
      )
    )

  return(.data)
}


# The row serialization is length-prefixed and NA-aware:
#   NA  -> "N:"                                                                                                         # nolint start: commented_code_linter
#   ""  -> "V:0:"
#   "a" -> "V:1:a"                                                                                                      # nolint end: commented_code_linter
#
# This avoids collisions such as NA vs "" and c("a_b", "c") vs c("a", "b_c").
encode_checksum_columns <- function(.data, hash_cols) {
  .data %>%
    dplyr::mutate(dplyr::across(
      tidyselect::all_of(hash_cols),
      ~ dplyr::case_when(
        is.na(as.character(.)) ~ "N:",
        TRUE ~ paste0("V:", nchar(as.character(.)), ":", as.character(.))
      ),
      .names = paste0("{.col}", ".__SCDB_checksum_chr")
    ))
}


#' @export
digest_to_checksum.default <- function(
  .data,
  col = formals(digest_to_checksum)$col,
  exclude = formals(digest_to_checksum)$exclude,
  ...
) {

  hash_cols <- setdiff(colnames(.data), c(col, exclude))
  checksum_cols <- paste0(hash_cols, ".__SCDB_checksum_chr")

  # Compute checksums locally then join back onto original data.

  checksums <- .data |>
    dplyr::collect() |>
    encode_checksum_columns(hash_cols) |>
    tidyr::unite(
      !!col,
      tidyselect::all_of(checksum_cols),
      sep = "",
      remove = FALSE
    ) |>
    dplyr::transmute(
      id__ = dplyr::row_number(),
      dplyr::across(tidyselect::all_of(col), openssl::md5)
    ) |>
    dplyr::copy_to(
      dbplyr::remote_con(.data),
      df = _,
      name = unique_table_name("SCDB_digest_to_checksum_helper")
    )

  defer_db_cleanup(checksums)

  .data <- .data |>
    dplyr::mutate(id__ = dplyr::row_number()) |>
    dplyr::select(!dplyr::any_of(col)) |> # Remove checksum column if it already exists
    dplyr::left_join(checksums, by = "id__") |>
    dplyr::select(!"id__") |>
    dplyr::compute(unique_table_name("SCDB_digest_to_checksum"))

  return(.data)
}

# It seems we need to do more hacking since
# @importFrom openssl md5 does not work in the below use case.
# defining md5 here successfully causes local objects to use the openssl md5 function
# and remote objects to use their own md5 functions.
md5 <- openssl::md5

# Some backends have native md5 support, these use this function.
#' @noRd
digest_to_checksum_native_md5 <- function(
  .data,
  col = formals(digest_to_checksum)$col,
  exclude = formals(digest_to_checksum)$exclude,
  ...
) {

  hash_cols <- setdiff(colnames(.data), c(col, exclude))
  checksum_cols <- paste0(hash_cols, ".__SCDB_checksum_chr")

  # The md5 algorithm needs character inputs, so we encode each hash column
  # before concatenation. The encoding is length-prefixed and NA-aware to avoid
  # serialization collisions.
  .data <- .data |>
    encode_checksum_columns(hash_cols) |>
    tidyr::unite(
      !!col,
      tidyselect::all_of(checksum_cols),
      sep = "",
      remove = TRUE
    ) |>
    dplyr::mutate(dplyr::across(tidyselect::all_of(col), md5))

  return(.data)
}

#' @export
digest_to_checksum.tbl_PqConnection <- digest_to_checksum_native_md5

#' @export
digest_to_checksum.tbl_duckdb_connection <- digest_to_checksum_native_md5

#' @export
digest_to_checksum.data.frame <- digest_to_checksum_native_md5
