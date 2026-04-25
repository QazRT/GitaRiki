# Internal helpers for OSV-related workflows.

.gitariki_log <- function(...) {
  invisible(NULL)
}

.osv_assert_scalar_character <- function(x, arg_name, allow_empty = FALSE) {
  ok <- is.character(x) && length(x) == 1L && (allow_empty || nzchar(x))
  if (!isTRUE(ok)) {
    stop("`", arg_name, "` must be a ", if (allow_empty) "character scalar." else "non-empty character scalar.", call. = FALSE)
  }
  invisible(TRUE)
}

.osv_assert_scalar_logical <- function(x, arg_name) {
  if (!(is.logical(x) && length(x) == 1L && !is.na(x))) {
    stop("`", arg_name, "` must be TRUE/FALSE scalar.", call. = FALSE)
  }
  invisible(TRUE)
}

.osv_assert_scalar_numeric_ge <- function(x, arg_name, min_value = 0) {
  ok <- is.numeric(x) && length(x) == 1L && is.finite(x) && !is.na(x) && x >= min_value
  if (!isTRUE(ok)) {
    stop("`", arg_name, "` must be numeric scalar >= ", min_value, ".", call. = FALSE)
  }
  invisible(TRUE)
}

.osv_null <- function(x, y) {
  if (is.null(x)) y else x
}

.osv_non_empty <- function(x) {
  if (is.null(x) || length(x) == 0L) return(FALSE)
  value <- as.character(x[[1]])
  !is.na(value) && nzchar(trimws(value))
}

.osv_to_scalar <- function(x) {
  if (is.null(x) || length(x) == 0L) return(NA_character_)
  as.character(x[[1]])
}

.osv_parse_purl <- function(purl) {
  if (!.osv_non_empty(purl)) {
    stop("`purl` must be a non-empty string.", call. = FALSE)
  }

  purl <- trimws(as.character(purl))
  m <- regexec("^pkg:([^/]+)/([^@?#]+?)(?:@([^?#]+))?(?:\\?([^#]+))?(?:#(.*))?$", purl, perl = TRUE)
  parts <- regmatches(purl, m)[[1]]

  if (length(parts) == 0L) {
    stop("Invalid purl format: ", purl, call. = FALSE)
  }

  type <- tolower(utils::URLdecode(parts[[2]]))
  path <- utils::URLdecode(parts[[3]])
  version <- if (length(parts) >= 4L) utils::URLdecode(parts[[4]]) else NA_character_
  qualifiers <- if (length(parts) >= 5L) parts[[5]] else NA_character_
  subpath <- if (length(parts) >= 6L) parts[[6]] else NA_character_

  path_parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  path_parts <- path_parts[nzchar(path_parts)]
  if (length(path_parts) == 0L) {
    stop("Invalid purl path: ", purl, call. = FALSE)
  }

  name <- tolower(path_parts[[length(path_parts)]])
  namespace <- if (length(path_parts) > 1L) {
    tolower(paste(path_parts[-length(path_parts)], collapse = "/"))
  } else {
    NA_character_
  }

  list(
    raw = purl,
    type = type,
    namespace = namespace,
    name = name,
    version = if (.osv_non_empty(version)) version else NA_character_,
    qualifiers = if (.osv_non_empty(qualifiers)) qualifiers else NA_character_,
    subpath = if (.osv_non_empty(subpath)) subpath else NA_character_
  )
}

.osv_normalize_purl <- function(purl, drop_version = TRUE) {
  parsed <- .osv_parse_purl(purl)

  base <- if (.osv_non_empty(parsed$namespace)) {
    paste0("pkg:", parsed$type, "/", parsed$namespace, "/", parsed$name)
  } else {
    paste0("pkg:", parsed$type, "/", parsed$name)
  }

  if (!isTRUE(drop_version) && .osv_non_empty(parsed$version)) {
    base <- paste0(base, "@", parsed$version)
  }

  base
}

.osv_map_purl_type_to_ecosystem <- function(type) {
  if (!.osv_non_empty(type)) return(NA_character_)

  map <- c(
    cran = "CRAN",
    deb = "Debian",
    gem = "RubyGems",
    golang = "Go",
    hex = "Hex",
    maven = "Maven",
    npm = "npm",
    nuget = "NuGet",
    pypi = "PyPI",
    composer = "Packagist",
    pub = "Pub",
    cargo = "crates.io",
    apk = "Alpine",
    wolfi = "Wolfi"
  )

  key <- tolower(as.character(type))
  if (key %in% names(map)) map[[key]] else NA_character_
}

.osv_candidate_package_names_from_purl <- function(parsed_purl) {
  stopifnot(is.list(parsed_purl), !is.null(parsed_purl$name))

  out <- c(parsed_purl$name)

  if (.osv_non_empty(parsed_purl$namespace)) {
    out <- c(
      out,
      paste0(parsed_purl$namespace, "/", parsed_purl$name),
      paste0(parsed_purl$namespace, ":", parsed_purl$name),
      paste0("@", parsed_purl$namespace, "/", parsed_purl$name)
    )
  }

  unique(tolower(out))
}

.osv_normalize_package_key <- function(ecosystem, package_name) {
  if (!.osv_non_empty(ecosystem) || !.osv_non_empty(package_name)) {
    return(NA_character_)
  }

  paste0(tolower(trimws(as.character(ecosystem))), "::", tolower(trimws(as.character(package_name))))
}

.osv_compare_versions_loose <- function(left, right) {
  if (!.osv_non_empty(left) || !.osv_non_empty(right)) return(NA_integer_)

  left <- trimws(as.character(left))
  right <- trimws(as.character(right))
  if (identical(left, right)) return(0L)

  strict_cmp <- tryCatch(
    utils::compareVersion(left, right),
    warning = function(w) NA_integer_,
    error = function(e) NA_integer_
  )

  if (!is.na(strict_cmp)) {
    return(as.integer(sign(strict_cmp)))
  }

  tokenize <- function(x) {
    parts <- strsplit(tolower(x), "[^a-z0-9]+", perl = TRUE)[[1]]
    parts[nzchar(parts)]
  }

  left_parts <- tokenize(left)
  right_parts <- tokenize(right)
  max_len <- max(length(left_parts), length(right_parts))

  if (max_len == 0L) {
    return(if (left < right) -1L else 1L)
  }

  for (i in seq_len(max_len)) {
    l <- if (i <= length(left_parts)) left_parts[[i]] else ""
    r <- if (i <= length(right_parts)) right_parts[[i]] else ""
    if (identical(l, r)) next

    l_num <- grepl("^[0-9]+$", l)
    r_num <- grepl("^[0-9]+$", r)

    if (l_num && r_num) {
      l_int <- suppressWarnings(as.numeric(l))
      r_int <- suppressWarnings(as.numeric(r))
      if (!is.na(l_int) && !is.na(r_int) && l_int != r_int) {
        return(if (l_int < r_int) -1L else 1L)
      }
    } else {
      return(if (l < r) -1L else 1L)
    }
  }

  if (left < right) -1L else 1L
}
