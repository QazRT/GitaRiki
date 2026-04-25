project_lib <- file.path(getwd(), ".Rlibs")
if (dir.exists(project_lib)) {
  .libPaths(c(normalizePath(project_lib), .libPaths()))
}

shiny::runApp(
  appDir = file.path(getwd(), "shiny_app"),
  host = "127.0.0.1",
  port = 3838,
  launch.browser = FALSE
)
