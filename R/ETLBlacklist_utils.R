clear_osv_cache <- function() {
  if (exists("OSV_DATA", envir = .GlobalEnv)) rm("OSV_DATA", envir = .GlobalEnv)
  if (exists("OSV_INDEX", envir = .GlobalEnv)) rm("OSV_INDEX", envir = .GlobalEnv)
  invisible(TRUE)
}
