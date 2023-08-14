#' Groups known pango-lineages by the major variants provided
#'
#' @param pangolin_data
#'   A lazy-query of pangolin data on which to build the map
#' @param variant_major
#'   A list of lineages to subdivide variants by. Lineages are matched to the most specific
#'   variant_major provided. Default is BA.2 and BA.5
#' @return
#'   A lazy query that contains lineage and focus_linage (on the form "variant: sub-lineage")
#' @examples
#' conn <- get_connection()
#'
#' pangolin <- get_table(conn, "mg.lineage_info")
#'
#' focus_lineage_map(pangolin) # Defaults to "BA.2", "BA.5"
#' focus_lineage_map(pangolin, c("BA.2", "BA.2.12", "BA.5"))
#'
#' close_connection(conn)
#' @export
#' @importFrom rlang .data
focus_lineage_map <- function(pangolin_data, variant_major = c("BA.2", "BA.5")) { # nocov start

  # Check arguments
  assert_data_like(pangolin_data)
  checkmate::assert_character(variant_major)

  focus_lineages <- pangolin_data |>
    dplyr::filter(.data$lineage %in% variant_major) |>
    dplyr::transmute(focus_lineage = .data$lineage, .data$full)

  lin_map <- pangolin_data |>
    dplyr::left_join(focus_lineages,                                              # "||" is a concatenate operator
                     sql_on = '"LHS"."full" = "RHS"."full" OR "LHS"."full" SIMILAR TO ("RHS"."full" || \'.%\')') |>
    dplyr::group_by(.data$lineage) |>
    dplyr::slice_max(.data$focus_lineage) |>
    dplyr::ungroup() |>
    dplyr::select("lineage", "focus_lineage", "variant")

  return(lin_map)
} # nocov end
