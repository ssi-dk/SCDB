files <- list.files(
  path = c("R", "tests", "vignettes", "data-raw"),
  recursive = TRUE,
  full.names = TRUE,
  pattern = "\\.(R|Rmd)$"
)

stats::setNames(
  rep(list(list("pipe_consistency_linter" = Inf)), length(files)),
  files
)
