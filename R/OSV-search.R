.osv_search_runtime_cache <- local({
  new.env(parent = emptyenv(), hash = TRUE)
})

.osv_build_pos_index <- function(values) {
  env <- new.env(parent = emptyenv(), hash = TRUE)
  if (is.null(values) || length(values) == 0L) return(env)

  keep <- which(!is.na(values) & nzchar(values))
  if (length(keep) == 0L) return(env)

  vals <- as.character(values[keep])
  groups <- split(keep, vals, drop = TRUE)
  for (nm in names(groups)) {
    assign(nm, as.integer(groups[[nm]]), envir = env)
  }
  env
}

.osv_pos_lookup <- function(index_env, keys) {
  if (is.null(index_env) || !is.environment(index_env) || length(keys) == 0L) return(integer(0))
  out <- integer(0)
  for (k in unique(keys)) {
    if (!.osv_non_empty(k)) next
    if (exists(k, envir = index_env, inherits = FALSE)) {
      out <- c(out, get(k, envir = index_env, inherits = FALSE))
    }
  }
  unique(out)
}

.osv_get_db_cache <- function(db) {
  db_key <- .osv_to_scalar(.osv_null(db$index_path, .osv_null(db$db_dir, "default")))
  if (!.osv_non_empty(db_key)) {
    db_key <- "default"
  }

  if (!exists(db_key, envir = .osv_search_runtime_cache, inherits = FALSE)) {
    assign(
      db_key,
      list(
        index_lc = NULL,
        entry_cache = new.env(parent = emptyenv(), hash = TRUE),
        search_cache = new.env(parent = emptyenv(), hash = TRUE)
      ),
      envir = .osv_search_runtime_cache
    )
  }

  cache <- get(db_key, envir = .osv_search_runtime_cache, inherits = FALSE)
  idx <- db$index
  n <- nrow(idx)
  idx_sig <- paste(n, paste(names(idx), collapse = "|"), sep = "::")

  if (is.null(cache$index_lc) || !identical(cache$index_lc$signature, idx_sig)) {
    package_purl_norm <- if ("package_purl_norm" %in% names(idx)) idx$package_purl_norm else NULL
    package_key_lc <- if ("package_key" %in% names(idx)) tolower(idx$package_key) else NULL
    package_name_lc <- if ("package_name" %in% names(idx)) tolower(idx$package_name) else NULL
    json_path_lc <- if ("json_path" %in% names(idx)) tolower(idx$json_path) else NULL

    cache$index_lc <- list(
      signature = idx_sig,
      package_purl_norm = package_purl_norm,
      package_key_lc = package_key_lc,
      package_name_lc = package_name_lc,
      json_path_lc = json_path_lc,
      purl_pos = .osv_build_pos_index(package_purl_norm),
      key_pos = .osv_build_pos_index(package_key_lc),
      name_pos = .osv_build_pos_index(package_name_lc)
    )
    assign(db_key, cache, envir = .osv_search_runtime_cache)
  }

  cache
}

.osv_get_entry_cached <- function(db, rel_path, cache) {
  if (!.osv_non_empty(rel_path)) return(NULL)
  key <- paste0("entry::", rel_path)
  if (exists(key, envir = cache$entry_cache, inherits = FALSE)) {
    cached <- get(key, envir = cache$entry_cache, inherits = FALSE)
    if (identical(cached, FALSE)) return(NULL)
    return(cached)
  }

  json_abs <- file.path(db$raw_dir, rel_path)
  if (!file.exists(json_abs)) {
    assign(key, FALSE, envir = cache$entry_cache)
    return(NULL)
  }

  entry <- tryCatch(
    jsonlite::fromJSON(json_abs, simplifyVector = FALSE),
    error = function(e) FALSE
  )
  assign(key, entry, envir = cache$entry_cache)
  if (identical(entry, FALSE)) return(NULL)
  entry
}

.osv_resolve_db <- function(osv_db) {
  if (inherits(osv_db, "osv_mongo_database")) {
    return(osv_db)
  }

  if (inherits(osv_db, "osv_database")) {
    if (is.null(osv_db$index)) {
      osv_db$index <- utils::read.csv(osv_db$index_path, stringsAsFactors = FALSE, na.strings = c("", "NA"))
    }
    return(osv_db)
  }

  if (is.character(osv_db) && length(osv_db) == 1L && nzchar(osv_db)) {
    return(load_osv_database(db_dir = osv_db, load_index = TRUE))
  }

  stop("`osv_db` must be an `osv_database` object or a path to a prepared OSV directory.", call. = FALSE)
}

.osv_extract_candidate_rows <- function(index_df, parsed_purl, index_lc = NULL) {
  purl_norm <- .osv_normalize_purl(parsed_purl$raw, drop_version = TRUE)
  ecosystem <- .osv_map_purl_type_to_ecosystem(parsed_purl$type)
  candidate_names <- .osv_candidate_package_names_from_purl(parsed_purl)

  candidate_keys <- unique(stats::na.omit(vapply(
    candidate_names,
    function(nm) .osv_normalize_package_key(ecosystem, nm),
    character(1)
  )))

  by_path <- rep(FALSE, nrow(index_df))

  purl_pos <- if (!is.null(index_lc)) .osv_null(index_lc$purl_pos, NULL) else NULL
  key_pos <- if (!is.null(index_lc)) .osv_null(index_lc$key_pos, NULL) else NULL
  name_pos <- if (!is.null(index_lc)) .osv_null(index_lc$name_pos, NULL) else NULL

  idx_by_purl <- integer(0)
  idx_by_key <- integer(0)
  idx_by_name <- integer(0)

  if (is.environment(purl_pos)) {
    idx_by_purl <- .osv_pos_lookup(purl_pos, purl_norm)
  } else if ("package_purl_norm" %in% names(index_df)) {
    idx_by_purl <- which(!is.na(index_df$package_purl_norm) & index_df$package_purl_norm == purl_norm)
  }

  if (length(candidate_keys) > 0L) {
    keys_lc <- tolower(candidate_keys)
    if (is.environment(key_pos)) {
      idx_by_key <- .osv_pos_lookup(key_pos, keys_lc)
    } else if ("package_key" %in% names(index_df)) {
      package_key_lc <- if (!is.null(index_lc) && !is.null(index_lc$package_key_lc)) {
        index_lc$package_key_lc
      } else {
        tolower(index_df$package_key)
      }
      idx_by_key <- which(!is.na(package_key_lc) & package_key_lc %in% keys_lc)
    }
  }

  # Fast precise stage: if we already have purl/key matches, avoid broad scans.
  precise_idx <- unique(c(idx_by_purl, idx_by_key))
  if (length(precise_idx) > 0L) {
    return(index_df[precise_idx, , drop = FALSE])
  }

  # Broader fallback by package name only when precise stage yielded nothing.
  if ("package_name" %in% names(index_df) && length(candidate_names) > 0L) {
    names_lc <- tolower(candidate_names)
    if (is.environment(name_pos)) {
      idx_by_name <- .osv_pos_lookup(name_pos, names_lc)
    } else {
      package_name_lc <- if (!is.null(index_lc) && !is.null(index_lc$package_name_lc)) {
        index_lc$package_name_lc
      } else {
        tolower(index_df$package_name)
      }
      idx_by_name <- which(!is.na(package_name_lc) & package_name_lc %in% names_lc)
    }
  }

  if (length(idx_by_name) > 0L) {
    return(index_df[unique(idx_by_name), , drop = FALSE])
  }

  # Last-resort path fallback only when all other strategies found nothing.
  if ("json_path" %in% names(index_df) && length(candidate_names) > 0L) {
    path_low <- if (!is.null(index_lc) && !is.null(index_lc$json_path_lc)) {
      index_lc$json_path_lc
    } else {
      tolower(index_df$json_path)
    }
    escaped <- vapply(candidate_names, function(nm) {
      gsub("([\\^\\$\\.\\|\\(\\)\\[\\]\\*\\+\\?\\\\{}])", "\\\\\\1", tolower(nm), perl = TRUE)
    }, character(1))

    for (enm in escaped) {
      by_path <- by_path | grepl(paste0("(^|/)", enm, "(/|\\.|$)"), path_low, perl = TRUE)
    }
    matched <- by_path
  } else {
    matched <- rep(FALSE, nrow(index_df))
  }

  index_df[matched, , drop = FALSE]
}

.osv_is_package_match <- function(affected_pkg, query_purl) {
  parsed_query <- .osv_parse_purl(query_purl)
  query_norm <- .osv_normalize_purl(query_purl, drop_version = TRUE)
  expected_ecosystem <- .osv_map_purl_type_to_ecosystem(parsed_query$type)
  expected_names <- .osv_candidate_package_names_from_purl(parsed_query)

  affected_purl <- .osv_to_scalar(affected_pkg$purl)
  affected_name <- tolower(.osv_to_scalar(affected_pkg$name))
  affected_ecosystem <- .osv_to_scalar(affected_pkg$ecosystem)

  purl_match <- FALSE
  if (.osv_non_empty(affected_purl)) {
    purl_match <- tryCatch(
      identical(.osv_normalize_purl(affected_purl, drop_version = TRUE), query_norm),
      error = function(e) FALSE
    )
  }

  key_match <- FALSE
  if (.osv_non_empty(affected_name) && .osv_non_empty(expected_ecosystem) && .osv_non_empty(affected_ecosystem)) {
    key_match <- identical(
      .osv_normalize_package_key(affected_ecosystem, affected_name),
      .osv_normalize_package_key(expected_ecosystem, affected_name)
    ) && affected_name %in% expected_names
  } else if (.osv_non_empty(affected_name)) {
    key_match <- affected_name %in% expected_names
  }

  purl_match || key_match
}

.osv_eval_events <- function(events, version) {
  if (is.null(events) || length(events) == 0L) return(NA)

  active <- FALSE
  any_decision <- FALSE

  for (ev in events) {
    introduced <- .osv_to_scalar(ev$introduced)
    fixed <- .osv_to_scalar(ev$fixed)
    last_affected <- .osv_to_scalar(ev$last_affected)
    limit <- .osv_to_scalar(ev$limit)

    if (.osv_non_empty(introduced)) {
      any_decision <- TRUE
      if (identical(introduced, "0")) {
        active <- TRUE
      } else {
        cmp <- .osv_compare_versions_loose(version, introduced)
        if (is.na(cmp)) return(NA)
        active <- cmp >= 0L
      }
    }

    if (.osv_non_empty(fixed) && isTRUE(active)) {
      any_decision <- TRUE
      cmp <- .osv_compare_versions_loose(version, fixed)
      if (is.na(cmp)) return(NA)
      if (cmp >= 0L) active <- FALSE
    }

    if (.osv_non_empty(last_affected) && isTRUE(active)) {
      any_decision <- TRUE
      cmp <- .osv_compare_versions_loose(version, last_affected)
      if (is.na(cmp)) return(NA)
      if (cmp > 0L) active <- FALSE
    }

    if (.osv_non_empty(limit) && isTRUE(active)) {
      any_decision <- TRUE
      cmp <- .osv_compare_versions_loose(version, limit)
      if (is.na(cmp)) return(NA)
      if (cmp >= 0L) active <- FALSE
    }
  }

  if (!any_decision) NA else active
}

.osv_is_version_affected <- function(affected, version = NULL) {
  if (!.osv_non_empty(version)) {
    return(list(affected = NA, reason = "version_not_provided"))
  }
  version <- trimws(as.character(version))

  exact_versions <- affected$versions
  if (!is.null(exact_versions) && length(exact_versions) > 0L) {
    exact_versions <- as.character(unlist(exact_versions))
    if (version %in% exact_versions) {
      return(list(affected = TRUE, reason = "exact_version_match"))
    }
  }

  ranges <- affected$ranges
  if (is.null(ranges) || length(ranges) == 0L) {
    return(list(affected = NA, reason = "no_ranges_for_comparison"))
  }

  evaluations <- lapply(ranges, function(rng) .osv_eval_events(rng$events, version))
  if (any(vapply(evaluations, function(x) identical(x, TRUE), logical(1)))) {
    return(list(affected = TRUE, reason = "range_match"))
  }
  if (all(vapply(evaluations, function(x) identical(x, FALSE), logical(1)))) {
    return(list(affected = FALSE, reason = "range_not_matched"))
  }

  list(affected = NA, reason = "unable_to_compare_version")
}

.osv_collect_entry_match <- function(entry, query_purl, version, include_uncertain) {
  if (is.null(entry$affected) || length(entry$affected) == 0L) return(NULL)

  matches <- list()
  idx <- 0L

  for (a in entry$affected) {
    pkg <- .osv_null(a$package, list())
    if (!.osv_is_package_match(pkg, query_purl)) next

    check <- .osv_is_version_affected(a, version = version)
    include <- isTRUE(check$affected) || (!isFALSE(include_uncertain) && is.na(check$affected))

    if (include) {
      idx <- idx + 1L
      matches[[idx]] <- list(
        affected = check$affected,
        reason = check$reason,
        matched_purl = .osv_to_scalar(pkg$purl),
        matched_package = .osv_to_scalar(pkg$name),
        matched_ecosystem = .osv_to_scalar(pkg$ecosystem)
      )
    }
  }

  if (idx == 0L) return(NULL)

  # Prefer deterministic positive matches over uncertain ones.
  positive_idx <- which(vapply(matches, function(x) identical(x$affected, TRUE), logical(1)))
  if (length(positive_idx) > 0L) {
    return(matches[[positive_idx[[1]]]])
  }

  matches[[1]]
}

#' Search vulnerabilities in prepared OSV database by purl and version.
#'
#' @param osv_db Prepared OSV DB object or path to DB directory.
#' @param purl Package URL (`pkg:` format).
#' @param version Package version (optional). If missing, returns potential matches.
#' @param include_uncertain Include records where version comparison is inconclusive.
#' @param max_results Max number of rows in result.
#'
#' @return Data frame with matched vulnerabilities.
#' @export
search_osv_vulnerabilities <- function(
    osv_db,
    purl,
    version = NULL,
    include_uncertain = TRUE,
    max_results = Inf
) {
  stopifnot(is.character(purl), length(purl) == 1L, nzchar(purl))
  stopifnot(is.logical(include_uncertain), length(include_uncertain) == 1L)

  db <- .osv_resolve_db(osv_db)
  if (inherits(db, "osv_mongo_database")) {
    return(search_osv_vulnerabilities_mongo(
      osv_db = db,
      purl = purl,
      version = version,
      include_uncertain = include_uncertain,
      max_results = max_results
    ))
  }

  db_cache <- .osv_get_db_cache(db)
  parsed <- .osv_parse_purl(purl)

  if (!.osv_non_empty(version) && .osv_non_empty(parsed$version)) {
    version <- parsed$version
  }
  if (.osv_non_empty(version)) {
    version <- as.character(version)
  } else {
    version <- NULL
  }

  query_cache_key <- paste(
    .osv_normalize_purl(parsed$raw, drop_version = TRUE),
    .osv_null(version, "<NA>"),
    as.character(include_uncertain),
    sep = "::"
  )
  if (exists(query_cache_key, envir = db_cache$search_cache, inherits = FALSE)) {
    cached <- get(query_cache_key, envir = db_cache$search_cache, inherits = FALSE)
    if (!is.data.frame(cached) || nrow(cached) == 0L) {
      return(data.frame())
    }
    if (is.finite(max_results)) {
      max_n <- max(0L, as.integer(max_results))
      if (nrow(cached) > max_n) {
        cached <- cached[seq_len(max_n), , drop = FALSE]
      }
    }
    return(cached[seq_len(nrow(cached)), , drop = FALSE])
  }

  candidate_rows <- .osv_extract_candidate_rows(db$index, parsed, index_lc = db_cache$index_lc)
  if (nrow(candidate_rows) == 0L) {
    assign(query_cache_key, data.frame(), envir = db_cache$search_cache)
    return(data.frame())
  }

  candidate_json <- unique(candidate_rows$json_path)
  out <- list()
  idx <- 0L

  for (rel_path in candidate_json) {
    entry <- .osv_get_entry_cached(db = db, rel_path = rel_path, cache = db_cache)
    if (is.null(entry)) next

    match_info <- .osv_collect_entry_match(
      entry = entry,
      query_purl = purl,
      version = version,
      include_uncertain = include_uncertain
    )
    if (is.null(match_info)) next

    references <- entry$references
    ref_urls <- character(0)
    if (!is.null(references) && length(references) > 0L) {
      ref_urls <- stats::na.omit(vapply(references, function(x) .osv_to_scalar(x$url), character(1)))
    }
    aliases <- entry$aliases
    if (is.null(aliases)) aliases <- character(0)

    idx <- idx + 1L
    out[[idx]] <- data.frame(
      osv_id = .osv_to_scalar(entry$id),
      summary = .osv_to_scalar(entry$summary),
      details = .osv_to_scalar(entry$details),
      published = .osv_to_scalar(entry$published),
      modified = .osv_to_scalar(entry$modified),
      aliases = paste(as.character(aliases), collapse = ","),
      references = paste(as.character(ref_urls), collapse = ","),
      query_purl = purl,
      query_version = if (is.null(version)) NA_character_ else version,
      matched_purl = match_info$matched_purl,
      matched_package = match_info$matched_package,
      matched_ecosystem = match_info$matched_ecosystem,
      affected = if (isTRUE(match_info$affected)) TRUE else if (identical(match_info$affected, FALSE)) FALSE else NA,
      match_reason = match_info$reason,
      source_json = rel_path,
      stringsAsFactors = FALSE
    )
  }

  if (length(out) == 0L) {
    assign(query_cache_key, data.frame(), envir = db_cache$search_cache)
    return(data.frame())
  }

  result <- do.call(rbind, out)
  result <- unique(result)
  rownames(result) <- NULL
  assign(query_cache_key, result, envir = db_cache$search_cache)

  result_out <- result
  if (is.finite(max_results)) {
    max_n <- max(0L, as.integer(max_results))
    if (nrow(result_out) > max_n) {
      result_out <- result_out[seq_len(max_n), , drop = FALSE]
    }
  }

  rownames(result_out) <- NULL
  result_out
}
