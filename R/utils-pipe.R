#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
#' @param lhs (`any`)\cr A value or the magrittr placeholder.
#' @param rhs (`any`)\cr A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
#' @examples
#'   1:10 %>% sum()
NULL


# Suppress object_usage_linter for magrittr pipe placeholder calls
utils::globalVariables(".")
