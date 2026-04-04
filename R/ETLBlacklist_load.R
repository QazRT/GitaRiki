#' Build in-memory indices for OSV vulnerability search
#'
#' @description
#' Constructs hash-based lookup indices for fast searching across
#' vulnerability records. The indices include package name, purl, and
#' version ranges. These indices are designed for `search_vulnerabilities()`.
#'
#' @param osv_data A list of OSV records returned by `download_osv()`.

library(archive)

load_osv <- function(zip_path) {
  tmp <- tempdir()
  message("Start to unzipping OSV")

  fast_unzip_safe(zip_path)


  message("Start to ETL OSV")
  files <- list.files(tmp, pattern = "\\.json$", full.names = TRUE)

  data <- vector("list", length(files))
  idx_name <- new.env(parent = emptyenv())
  idx_purl <- new.env(parent = emptyenv())

  for (i in seq_along(files)) {
    entry <- jsonlite::fromJSON(files[i], simplifyVector = FALSE) # ключ

    data[[i]] <- entry

    if (!is.null(entry$affected)) {
      for (aff in entry$affected) {
        # name
        name <- aff$package$name
        if (!is.null(name)) {
          key <- tolower(name)
          if (!exists(key, envir = idx_name, inherits = FALSE)) {
            assign(key, integer(0), envir = idx_name)
          }
          assign(key, c(get(key, envir = idx_name), i), envir = idx_name)
        }

        # purl
        purl <- aff$package$purl
        if (!is.null(purl)) {
          key <- tolower(purl)
          if (!exists(key, envir = idx_purl, inherits = FALSE)) {
            assign(key, integer(0), envir = idx_purl)
          }
          assign(key, c(get(key, envir = idx_purl), i), envir = idx_purl)
        }
      }
    }
  }

  assign("OSV_DATA", data, envir = .GlobalEnv)
  assign("OSV_INDEX_NAME", idx_name, envir = .GlobalEnv)
  assign("OSV_INDEX_PURL", idx_purl, envir = .GlobalEnv)

  invisible(TRUE)
}


fast_unzip_safe <- function(zip_path, out_dir = tempdir()) {

  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  options(archive.extract.filter = function(path) gsub(":", "_", path))
  archive_extract(zip_path, dir = out_dir)
}