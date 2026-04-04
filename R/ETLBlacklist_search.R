#' Search vulnerabilities by package name, purl and version
#'
#' @description
#' Performs hash-based lookup over the in-memory OSV index. The function
#' matches package names, purl identifiers, and evaluates version ranges,
#' including 'introduced', 'fixed', and explicit `versions`.
#'
#' @param index
#' An index object created by `build_osv_index()`.
#'
#' @param query
#' Character string. May be a package name (e.g. `"gh"`), a purl
#' (e.g. `"pkg:cran/gh"`), or partial.
#'
#' @param version
#' Optional version string. If supplied, only vulnerabilities affecting
#' the specified semantic version are returned. Versions equal to or above
#' a `fixed` value are excluded.
#'
#' @details
#' Version matching uses semantic comparison. A vulnerability applies if:
#' \itemize{
#'   \item version >= introduced
#'   \item AND version < fixed (if fixed exists)
#' }
#' OR if version is explicitly listed in `versions`.
#'
#' @return A list of matching OSV vulnerability records.
#'
#' @examples
#' \dontrun{
#' idx <- build_osv_index(download_osv())
#' search_vulnerabilities(idx, "gh")
#' search_vulnerabilities(idx, "pkg:cran/gh", version = "1.4.1")
#' }
#'
#' @export
search_vulnerabilities <- function(query, version = NULL) {
  if (!exists("OSV_DATA", envir = .GlobalEnv)) stop("Data not loaded.")
  if (!exists("OSV_INDEX_NAME", envir = .GlobalEnv)) stop("Data not loaded.")
  if (!exists("OSV_INDEX_PURL", envir = .GlobalEnv)) stop("Data not loaded.")

  data <- get("OSV_DATA", envir = .GlobalEnv)
  idx_name <- get("OSV_INDEX_NAME", envir = .GlobalEnv)
  idx_purl <- get("OSV_INDEX_PURL", envir = .GlobalEnv)
  clean_key <- function(x) tolower(trimws(x))


  q <- clean_key(query)
  positions <- integer(0)



  if (exists(q, envir = idx_name, inherits = FALSE)) {
    positions <- c(positions, get(q, envir = idx_name))
  }

  if (exists(q, envir = idx_purl, inherits = FALSE)) {
    positions <- c(positions, get(q, envir = idx_purl))
  }

  positions <- unique(positions)
  if (length(positions) == 0) {
    return(list())
  }

  if (length(positions) == 0) {
    return(list())
  }

  result <- list()
  count <- 1

  for (pos in positions) {
    entry <- data[[pos]]

    if (is_version_vulnerable(entry, version)) {
      result[[count]] <- entry
      count <- count + 1
    }
  }

  return(result)
}
