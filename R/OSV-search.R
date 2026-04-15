.osv_resolve_db <- function(osv_db) {
  if (inherits(osv_db, "osv_mongo_database")) {
    return(osv_db)
  }

  if (is.list(osv_db) &&
      all(c("mongo_url", "db_name", "collection") %in% names(osv_db))) {
    return(load_osv_mongo_database(
      mongo_url = as.character(osv_db$mongo_url),
      db_name = as.character(osv_db$db_name),
      collection = as.character(osv_db$collection),
      validate = FALSE
    ))
  }

  stop(
    "Only MongoDB OSV backend is supported. ",
    "Pass object from `load_osv_mongo_database()` or `prepare_osv_database()`.",
    call. = FALSE
  )
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
  normalize_events <- function(x) {
    out <- list()
    event_fields <- c("introduced", "fixed", "last_affected", "limit")

    append_item <- function(item) {
      while (is.list(item) && length(item) == 1L && is.null(names(item))) {
        item <- item[[1L]]
      }

      if (is.null(item) || length(item) == 0L) return(invisible(NULL))

      if (is.data.frame(item)) {
        if (nrow(item) == 0L) return(invisible(NULL))
        for (i in seq_len(nrow(item))) {
          out[[length(out) + 1L]] <<- as.list(item[i, , drop = FALSE])
        }
        return(invisible(NULL))
      }

      if (is.list(item) && !is.null(names(item)) && any(event_fields %in% names(item))) {
        out[[length(out) + 1L]] <<- item
        return(invisible(NULL))
      }

      if (is.list(item)) {
        for (sub in item) append_item(sub)
      }
      invisible(NULL)
    }

    append_item(x)
    out
  }

  events <- normalize_events(events)
  if (length(events) == 0L) return(NA)

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

  normalize_ranges <- function(x) {
    out <- list()
    range_fields <- c("type", "events")

    append_item <- function(item) {
      while (is.list(item) && length(item) == 1L && is.null(names(item))) {
        item <- item[[1L]]
      }

      if (is.null(item) || length(item) == 0L) return(invisible(NULL))

      if (is.data.frame(item)) {
        if (nrow(item) == 0L) return(invisible(NULL))
        if ("events" %in% names(item)) {
          type_vals <- tolower(trimws(as.character(.osv_null(item$type, NA_character_))))
          type_vals <- unique(type_vals[!is.na(type_vals) & nzchar(type_vals)])

          if (nrow(item) > 1L && length(type_vals) <= 1L) {
            out[[length(out) + 1L]] <<- list(
              type = .osv_to_scalar(.osv_null(item$type, NA_character_)),
              events = as.list(item$events)
            )
          } else {
            for (i in seq_len(nrow(item))) {
              out[[length(out) + 1L]] <<- as.list(item[i, , drop = FALSE])
            }
          }
        } else {
          for (i in seq_len(nrow(item))) {
            out[[length(out) + 1L]] <<- as.list(item[i, , drop = FALSE])
          }
        }
        return(invisible(NULL))
      }

      if (is.list(item) && !is.null(names(item)) && any(range_fields %in% names(item))) {
        out[[length(out) + 1L]] <<- item
        return(invisible(NULL))
      }

      if (is.list(item)) {
        for (sub in item) append_item(sub)
      }
      invisible(NULL)
    }

    append_item(x)
    out
  }

  ranges <- normalize_ranges(affected$ranges)

  if (is.null(ranges) || length(ranges) == 0L) {
    return(list(affected = NA, reason = "no_ranges_for_comparison"))
  }

  evaluations <- lapply(ranges, function(rng) {
    .osv_eval_events(rng$events, version)
  })
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

  normalize_affected <- function(x) {
    out <- list()
    affected_fields <- c("package", "ranges", "versions")

    append_item <- function(item) {
      while (is.list(item) && length(item) == 1L && is.null(names(item))) {
        item <- item[[1L]]
      }

      if (is.null(item) || length(item) == 0L) return(invisible(NULL))

      if (is.data.frame(item)) {
        if (nrow(item) == 0L) return(invisible(NULL))
        for (i in seq_len(nrow(item))) {
          out[[length(out) + 1L]] <<- as.list(item[i, , drop = FALSE])
        }
        return(invisible(NULL))
      }

      if (is.list(item) && !is.null(names(item)) && any(affected_fields %in% names(item))) {
        out[[length(out) + 1L]] <<- item
        return(invisible(NULL))
      }

      if (is.list(item)) {
        for (sub in item) append_item(sub)
      }
      invisible(NULL)
    }

    append_item(x)
    out
  }

  normalize_package <- function(x) {
    pkg <- x
    while (is.list(pkg) && length(pkg) == 1L && is.null(names(pkg))) {
      pkg <- pkg[[1L]]
    }
    if (is.data.frame(pkg)) {
      if (nrow(pkg) == 0L) return(list())
      return(as.list(pkg[1, , drop = FALSE]))
    }
    if (is.list(pkg) && !is.null(names(pkg))) return(pkg)
    if (is.list(pkg) && length(pkg) > 0L) {
      first_pkg <- pkg[[1L]]
      if (is.data.frame(first_pkg)) {
        if (nrow(first_pkg) == 0L) return(list())
        return(as.list(first_pkg[1, , drop = FALSE]))
      }
      if (is.list(first_pkg)) return(first_pkg)
    }
    list()
  }

  affected_list <- normalize_affected(entry$affected)

  matches <- list()
  idx <- 0L

  for (a in affected_list) {
    pkg <- normalize_package(.osv_null(a$package, list()))

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

  positive_idx <- which(vapply(matches, function(x) identical(x$affected, TRUE), logical(1)))
  if (length(positive_idx) > 0L) {
    return(matches[[positive_idx[[1L]]]])
  }

  matches[[1L]]
}

#' Search vulnerabilities in OSV database by purl and version.
#'
#' MongoDB backend only.
#'
#' @param osv_db Mongo OSV DB object from `load_osv_mongo_database()`.
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
  .osv_assert_scalar_character(purl, "purl")
  .osv_assert_scalar_logical(include_uncertain, "include_uncertain")

  db <- .osv_resolve_db(osv_db)
  search_osv_vulnerabilities_mongo(
    osv_db = db,
    purl = purl,
    version = version,
    include_uncertain = include_uncertain,
    max_results = max_results
  )
}
