linters <- linters_with_defaults(
  line_length_linter(120)
)

exclude_linter <- r"{^ *: *([\w, ]*).?$}"
