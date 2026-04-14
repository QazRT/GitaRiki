.gh_extract_username_fallback <- function(input) {
  if (grepl("^[a-zA-Z0-9_-]+$", input)) {
    return(input)
  }

  m <- regexec("^https?://github\\.com/([^/]+)/?.*$", input, perl = TRUE)
  parts <- regmatches(input, m)[[1]]
  if (length(parts) >= 2L) {
    return(parts[[2]])
  }

  NA_character_
}

.resolve_executable_path <- function(executable) {
  if (!is.character(executable) || length(executable) != 1L || !nzchar(trimws(executable))) {
    return(NA_character_)
  }

  if (file.exists(executable)) {
    return(normalizePath(executable, winslash = "/", mustWork = FALSE))
  }

  in_path <- Sys.which(executable)
  if (nzchar(in_path)) {
    return(normalizePath(in_path, winslash = "/", mustWork = FALSE))
  }

  NA_character_
}

default_dependency_manifest_patterns <- function() {
  c(
    "(^|/)pom\\.xml$",
    "(^|/)build\\.gradle(\\.kts)?$",
    "(^|/)settings\\.gradle(\\.kts)?$",
    "(^|/)gradle\\.lockfile$",
    "(^|/)package(-lock)?\\.json$",
    "(^|/)npm-shrinkwrap\\.json$",
    "(^|/)yarn\\.lock$",
    "(^|/)pnpm-lock\\.ya?ml$",
    "(^|/)requirements(\\.[^/]+)?\\.txt$",
    "(^|/)Pipfile(\\.lock)?$",
    "(^|/)pyproject\\.toml$",
    "(^|/)poetry\\.lock$",
    "(^|/)setup\\.(py|cfg)$",
    "(^|/)go\\.(mod|sum)$",
    "(^|/)Cargo\\.(toml|lock)$",
    "(^|/)composer\\.(json|lock)$",
    "(^|/)Gemfile(\\.lock)?$",
    "(^|/)mix\\.(exs|lock)$",
    "(^|/)project\\.clj$",
    "(^|/)deps\\.edn$",
    "(^|/)rebar\\.config(\\.lock)?$",
    "(^|/)packages\\.config$",
    "(^|/)Directory\\.Packages\\.props$",
    "(^|/)paket\\.(dependencies|lock)$",
    "(^|/)\\.github/dependabot\\.ya?ml$",
    "(^|/)renv\\.lock$",
    "(^|/)DESCRIPTION$",
    "\\.csproj$"
  )
}

commit_has_dependency_changes <- function(changed_files, patterns = default_dependency_manifest_patterns()) {
  if (length(changed_files) == 0L) return(logical(0))

  norm <- gsub("\\\\", "/", as.character(changed_files))
  vapply(norm, function(path) {
    any(vapply(patterns, function(rx) grepl(rx, path, perl = TRUE, ignore.case = TRUE), logical(1)))
  }, logical(1))
}

.gh_require <- function() {
  if (!requireNamespace("gh", quietly = TRUE)) {
    stop("Package 'gh' is required but not installed.", call. = FALSE)
  }
}

.gh_repo_commits <- function(repo_full, token = NULL, max_commits = NULL) {
  .gh_require()
  args <- list(
    endpoint = "GET /repos/{repo_full}/commits",
    repo_full = repo_full,
    .token = token,
    .progress = FALSE
  )
  args$.limit <- if (is.null(max_commits)) Inf else as.integer(max_commits)
  do.call(gh::gh, args)
}

.gh_commit_detail <- function(repo_full, sha, token = NULL) {
  .gh_require()
  gh::gh(
    "GET /repos/{repo_full}/commits/{sha}",
    repo_full = repo_full,
    sha = sha,
    .token = token,
    .progress = FALSE
  )
}

.download_repo_snapshot <- function(repo_full, sha, token = NULL, temp_root = tempdir()) {
  if (!requireNamespace("curl", quietly = TRUE)) {
    stop("Package 'curl' is required but not installed.", call. = FALSE)
  }

  stamp <- gsub("[^A-Za-z0-9._-]", "-", paste0(repo_full, "-", sha))
  work_dir <- file.path(temp_root, paste0("repo-snapshot-", stamp, "-", as.integer(stats::runif(1, 100000, 999999))))
  dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)

  zip_path <- file.path(work_dir, "snapshot.zip")
  extract_dir <- file.path(work_dir, "content")
  dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)

  api_url <- paste0("https://api.github.com/repos/", repo_full, "/zipball/", sha)
  h <- curl::new_handle()
  headers <- c(
    "User-Agent" = "GitaRiki-OSV-Scanner",
    "Accept" = "application/vnd.github+json"
  )
  if (!is.null(token) && nzchar(token)) {
    headers["Authorization"] <- paste("Bearer", token)
  }
  curl::handle_setheaders(h, .list = headers)
  curl::curl_download(url = api_url, destfile = zip_path, mode = "wb", handle = h)

  utils::unzip(zip_path, exdir = extract_dir)
  roots <- list.dirs(extract_dir, recursive = FALSE, full.names = TRUE)
  repo_dir <- if (length(roots) == 1L) roots[[1]] else extract_dir

  list(work_dir = work_dir, repo_dir = repo_dir, zip_path = zip_path)
}

.df_or_empty <- function(rows, template_cols) {
  if (length(rows) == 0L) {
    out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
    return(out[0, , drop = FALSE])
  }
  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  out
}

.rbind_data_frames <- function(dfs, template_cols = NULL) {
  valid <- Filter(function(x) is.data.frame(x), dfs)
  valid <- Filter(function(x) nrow(x) > 0L, valid)

  if (length(valid) == 0L) {
    if (is.null(template_cols)) return(data.frame())
    out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
    return(out[0, , drop = FALSE])
  }

  out <- do.call(rbind, valid)
  rownames(out) <- NULL
  out
}

.repo_progress_bar <- function(ratio, width = 20L) {
  ratio <- max(0, min(1, as.numeric(ratio)))
  width <- as.integer(width)
  done <- floor(ratio * width)
  paste0(strrep("#", done), strrep("-", max(0L, width - done)))
}

.repo_format_duration <- function(seconds) {
  seconds <- as.numeric(seconds)
  if (!is.finite(seconds) || is.na(seconds) || seconds < 0) return("n/a")
  if (seconds < 60) return(sprintf("%.0fs", seconds))
  mins <- floor(seconds / 60)
  secs <- round(seconds %% 60)
  if (mins < 60) return(sprintf("%dm %02ds", mins, secs))
  hrs <- floor(mins / 60)
  rem_mins <- mins %% 60
  sprintf("%dh %02dm %02ds", hrs, rem_mins, secs)
}

.parse_commit_datetime <- function(x) {
  if (!.osv_non_empty(x)) return(as.POSIXct(NA))
  ts <- suppressWarnings(as.POSIXct(as.character(x), tz = "UTC"))
  if (is.na(ts)) {
    ts <- suppressWarnings(as.POSIXct(as.character(x), format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  }
  ts
}

.vuln_lifecycle_key <- function(row_df) {
  get_val <- function(nm) {
    if (!is.data.frame(row_df) || nrow(row_df) == 0L || !(nm %in% names(row_df))) return(NA_character_)
    .osv_to_scalar(row_df[[nm]])
  }

  osv_id <- get_val("osv_id")
  comp_purl <- get_val("component_purl")
  query_purl <- get_val("query_purl")
  matched_purl <- get_val("matched_purl")
  matched_package <- get_val("matched_package")
  matched_ecosystem <- get_val("matched_ecosystem")

  comp_id <- NA_character_
  if (.osv_non_empty(comp_purl)) {
    comp_id <- tolower(comp_purl)
  } else if (.osv_non_empty(query_purl)) {
    comp_id <- tolower(query_purl)
  } else if (.osv_non_empty(matched_purl)) {
    comp_id <- tolower(matched_purl)
  } else if (.osv_non_empty(matched_package) || .osv_non_empty(matched_ecosystem)) {
    comp_id <- paste0(tolower(.osv_null(matched_ecosystem, "")), "::", tolower(.osv_null(matched_package, "")))
  } else {
    comp_id <- "unknown_component"
  }

  paste(.osv_null(osv_id, "unknown_osv"), comp_id, sep = "::")
}

.build_vulnerability_lifecycle <- function(repo_full, scan_snapshots) {
  template_cols <- c(
    "repository", "vulnerability_key", "osv_id",
    "query_purl", "matched_purl", "matched_package", "matched_ecosystem",
    "component_name", "component_purl",
    "introduced_sha", "introduced_date", "introduced_message", "introduced_dependency_files",
    "fixed_sha", "fixed_date", "fixed_message", "fixed_dependency_files",
    "status"
  )

  if (length(scan_snapshots) == 0L) {
    out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
    return(out[0, , drop = FALSE])
  }

  row_get <- function(row_df, nm) {
    if (!is.data.frame(row_df) || nrow(row_df) == 0L || !(nm %in% names(row_df))) return(NA_character_)
    .osv_to_scalar(row_df[[nm]])
  }

  snapshots <- scan_snapshots
  ts_num <- vapply(snapshots, function(s) {
    dt <- .parse_commit_datetime(s$date)
    if (is.na(dt)) Inf else as.numeric(dt)
  }, numeric(1))
  ord_num <- vapply(snapshots, function(s) as.numeric(.osv_null(s$order, NA_integer_)), numeric(1))
  ord <- order(ts_num, ord_num)
  snapshots <- snapshots[ord]

  open_map <- new.env(parent = emptyenv(), hash = TRUE)
  rows <- list()
  idx <- 0L

  emit_row <- function(state, close_snapshot = NULL, status = c("fixed", "open")) {
    status <- match.arg(status)
    row_df <- state$row

    idx <<- idx + 1L
    rows[[idx]] <<- data.frame(
      repository = repo_full,
      vulnerability_key = state$key,
      osv_id = row_get(row_df, "osv_id"),
      query_purl = row_get(row_df, "query_purl"),
      matched_purl = row_get(row_df, "matched_purl"),
      matched_package = row_get(row_df, "matched_package"),
      matched_ecosystem = row_get(row_df, "matched_ecosystem"),
      component_name = row_get(row_df, "component_name"),
      component_purl = row_get(row_df, "component_purl"),
      introduced_sha = .osv_null(state$introduced$sha, NA_character_),
      introduced_date = .osv_null(state$introduced$date, NA_character_),
      introduced_message = .osv_null(state$introduced$message, NA_character_),
      introduced_dependency_files = .osv_null(state$introduced$dependency_files, NA_character_),
      fixed_sha = if (!is.null(close_snapshot)) .osv_null(close_snapshot$sha, NA_character_) else NA_character_,
      fixed_date = if (!is.null(close_snapshot)) .osv_null(close_snapshot$date, NA_character_) else NA_character_,
      fixed_message = if (!is.null(close_snapshot)) .osv_null(close_snapshot$message, NA_character_) else NA_character_,
      fixed_dependency_files = if (!is.null(close_snapshot)) .osv_null(close_snapshot$dependency_files, NA_character_) else NA_character_,
      status = status,
      stringsAsFactors = FALSE
    )
  }

  for (snap in snapshots) {
    current_keys <- unique(as.character(.osv_null(snap$keys, character(0))))
    current_keys <- current_keys[nzchar(current_keys)]
    current_key_rows <- .osv_null(snap$key_rows, list())

    for (k in current_keys) {
      if (!exists(k, envir = open_map, inherits = FALSE)) {
        assign(
          k,
          list(
            key = k,
            row = .osv_null(current_key_rows[[k]], data.frame()),
            introduced = list(
              sha = .osv_null(snap$sha, NA_character_),
              date = .osv_null(snap$date, NA_character_),
              message = .osv_null(snap$message, NA_character_),
              dependency_files = .osv_null(snap$dependency_files, NA_character_)
            )
          ),
          envir = open_map
        )
      }
    }

    open_keys <- ls(open_map, all.names = TRUE)
    to_close <- setdiff(open_keys, current_keys)
    if (length(to_close) > 0L) {
      for (k in to_close) {
        state <- get(k, envir = open_map, inherits = FALSE)
        emit_row(state, close_snapshot = snap, status = "fixed")
        rm(list = k, envir = open_map)
      }
    }
  }

  remaining <- ls(open_map, all.names = TRUE)
  if (length(remaining) > 0L) {
    for (k in remaining) {
      state <- get(k, envir = open_map, inherits = FALSE)
      emit_row(state, close_snapshot = NULL, status = "open")
    }
  }

  out <- .rbind_data_frames(rows, template_cols = template_cols)
  if (nrow(out) > 0L) {
    out <- unique(out)
    rownames(out) <- NULL
  }
  out
}

.prefetch_commit_details <- function(
    repo_full,
    commit_shas,
    token = NULL,
    parallel = FALSE,
    workers = 1L,
    emit_messages = FALSE,
    cluster = NULL
) {
  n <- length(commit_shas)
  if (n == 0L) return(list())

  fetch_one <- function(sha) {
    tryCatch(
      list(
        sha = sha,
        detail = gh::gh(
          "GET /repos/{repo_full}/commits/{sha}",
          repo_full = repo_full,
          sha = sha,
          .token = token,
          .progress = FALSE
        ),
        error = NULL
      ),
      error = function(e) {
        list(sha = sha, detail = NULL, error = conditionMessage(e))
      }
    )
  }

  use_parallel <- isTRUE(parallel) &&
    as.integer(workers) > 1L &&
    n > 1L &&
    requireNamespace("parallel", quietly = TRUE)

  use_existing_cluster <- !is.null(cluster) && inherits(cluster, "cluster") && n > 1L

  if (use_existing_cluster) {
    return(parallel::parLapply(
      cl = cluster,
      X = as.list(commit_shas),
      fun = function(sha, repo_full, token) {
        tryCatch(
          list(
            sha = sha,
            detail = gh::gh(
              "GET /repos/{repo_full}/commits/{sha}",
              repo_full = repo_full,
              sha = sha,
              .token = token,
              .progress = FALSE
            ),
            error = NULL
          ),
          error = function(e) {
            list(sha = sha, detail = NULL, error = conditionMessage(e))
          }
        )
      },
      repo_full = repo_full,
      token = token
    ))
  }

  if (!use_parallel) {
    return(lapply(commit_shas, fetch_one))
  }

  w <- min(as.integer(workers), n)
  if (isTRUE(emit_messages)) {
    message("[repo ", repo_full, "] prefetch commit details in parallel: workers=", w, ", commits=", n)
  }

  cl <- parallel::makeCluster(w)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  parallel::parLapply(
    cl = cl,
    X = as.list(commit_shas),
    fun = function(sha, repo_full, token) {
      tryCatch(
        list(
          sha = sha,
          detail = gh::gh(
            "GET /repos/{repo_full}/commits/{sha}",
            repo_full = repo_full,
            sha = sha,
            .token = token,
            .progress = FALSE
          ),
          error = NULL
        ),
        error = function(e) {
          list(sha = sha, detail = NULL, error = conditionMessage(e))
        }
      )
    },
    repo_full = repo_full,
    token = token
  )
}

.build_repo_prefetch_plan <- function(
    repo_full,
    token = NULL,
    max_commits_per_repo = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 1L,
    emit_messages = FALSE,
    prefetch_cluster = NULL
) {
  errors <- list()
  add_err <- function(sha, stage, err) {
    errors[[length(errors) + 1L]] <<- data.frame(
      repository = repo_full,
      sha = sha,
      stage = stage,
      error = err,
      stringsAsFactors = FALSE
    )
  }

  commits <- tryCatch(
    .gh_repo_commits(repo_full, token = token, max_commits = max_commits_per_repo),
    error = function(e) {
      add_err(NA_character_, "list_commits", conditionMessage(e))
      NULL
    }
  )

  if (is.null(commits) || length(commits) == 0L) {
    return(list(
      repo_full = repo_full,
      commits = NULL,
      details = list(),
      errors = .rbind_data_frames(errors, template_cols = c("repository", "sha", "stage", "error"))
    ))
  }

  commit_shas <- vapply(commits, function(cmt) .osv_to_scalar(cmt$sha), character(1))
  details_raw <- .prefetch_commit_details(
    repo_full = repo_full,
    commit_shas = commit_shas,
    token = token,
    parallel = isTRUE(commit_info_parallel),
    workers = as.integer(commit_info_workers),
    emit_messages = emit_messages,
    cluster = prefetch_cluster
  )

  details <- lapply(details_raw, function(d) {
    if (!is.null(d$error)) {
      add_err(d$sha, "commit_detail", as.character(d$error))
      return(list(sha = d$sha, filenames = character(0), error = as.character(d$error)))
    }

    files <- d$detail$files
    filenames <- if (!is.null(files) && length(files) > 0L) {
      stats::na.omit(vapply(files, function(f) .osv_to_scalar(f$filename), character(1)))
    } else {
      character(0)
    }

    list(sha = d$sha, filenames = filenames, error = NULL)
  })

  list(
    repo_full = repo_full,
    commits = commits,
    details = details,
    errors = .rbind_data_frames(errors, template_cols = c("repository", "sha", "stage", "error"))
  )
}

.analyze_repository_commits <- function(
    repo_full,
    token,
    max_commits_per_repo,
    dependency_patterns,
    force_scan,
    temp_root,
    keep_snapshots,
    include_uncertain,
    syft_resolved,
    osv_db_obj,
    emit_messages = TRUE,
    repo_progress = FALSE,
    repo_progress_every = 10L,
    repo_progress_details = FALSE,
    syft_timeout_sec = 0L,
    syft_excludes = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 1L,
    prefetched_commits = NULL,
    prefetched_details = NULL,
    prefetched_errors = NULL
) {
  summary_rows <- list()
  vulnerability_rows <- list()
  error_rows <- list()
  scan_snapshots <- list()

  append_error <- function(sha, stage, err) {
    error_rows[[length(error_rows) + 1L]] <<- data.frame(
      repository = repo_full,
      sha = sha,
      stage = stage,
      error = err,
      stringsAsFactors = FALSE
    )
  }

  if (isTRUE(emit_messages)) {
    message("Processing repository: ", repo_full)
  }

  if (is.data.frame(prefetched_errors) && nrow(prefetched_errors) > 0L) {
    for (i in seq_len(nrow(prefetched_errors))) {
      append_error(
        sha = as.character(prefetched_errors$sha[[i]]),
        stage = as.character(prefetched_errors$stage[[i]]),
        err = as.character(prefetched_errors$error[[i]])
      )
    }
  }

  commits <- if (!is.null(prefetched_commits)) {
    prefetched_commits
  } else {
    tryCatch(
      .gh_repo_commits(repo_full, token = token, max_commits = max_commits_per_repo),
      error = function(e) {
        append_error(NA_character_, "list_commits", conditionMessage(e))
        NULL
      }
    )
  }

  if (is.null(commits) || length(commits) == 0L) {
    return(list(
      summary = data.frame(),
      vulnerabilities = data.frame(),
      vulnerability_lifecycle = data.frame(),
      errors = .rbind_data_frames(error_rows)
    ))
  }

  total_commits <- length(commits)
  repo_started_at <- Sys.time()
  scan_attempts <- 0L
  scanned_success <- 0L
  vuln_rows_total <- 0L
  repo_progress_every <- max(1L, as.integer(repo_progress_every))

  progress_tick <- function(done, force = FALSE) {
    if (!isTRUE(repo_progress)) return(invisible(NULL))
    if (!isTRUE(force) && (done %% repo_progress_every != 0L) && done != total_commits) {
      return(invisible(NULL))
    }

    elapsed <- as.numeric(difftime(Sys.time(), repo_started_at, units = "secs"))
    ratio <- if (total_commits > 0) done / total_commits else 1
    rate <- if (elapsed > 0) done / elapsed else NA_real_
    remaining <- max(0L, total_commits - done)
    eta <- if (!is.na(rate) && is.finite(rate) && rate > 0) remaining / rate else NA_real_

    message(
      sprintf(
        "[repo %s] %5.1f%% [%s] %d/%d | scans %d ok %d | vulns %d | elapsed %s | ETA %s",
        repo_full,
        ratio * 100,
        .repo_progress_bar(ratio),
        done,
        total_commits,
        scan_attempts,
        scanned_success,
        vuln_rows_total,
        .repo_format_duration(elapsed),
        .repo_format_duration(eta)
      )
    )

    invisible(NULL)
  }

  if (isTRUE(repo_progress)) {
    message("[repo ", repo_full, "] commit analysis started: ", total_commits, " commits")
  }

  stage_msg <- function(msg) {
    if (isTRUE(repo_progress) && isTRUE(repo_progress_details)) {
      message("[repo ", repo_full, "] ", msg)
    }
  }

  commit_details <- if (!is.null(prefetched_details)) {
    prefetched_details
  } else {
    commit_shas <- vapply(commits, function(cmt) .osv_to_scalar(cmt$sha), character(1))
    .prefetch_commit_details(
      repo_full = repo_full,
      commit_shas = commit_shas,
      token = token,
      parallel = isTRUE(commit_info_parallel),
      workers = as.integer(commit_info_workers),
      emit_messages = isTRUE(repo_progress) && isTRUE(repo_progress_details)
    )
  }

  for (commit_idx in seq_along(commits)) {
    cmt <- commits[[commit_idx]]
    sha <- .osv_to_scalar(cmt$sha)
    commit_date <- .osv_to_scalar(cmt$commit$author$date)
    commit_message <- .osv_to_scalar(cmt$commit$message)

    detail_info <- if (commit_idx <= length(commit_details)) commit_details[[commit_idx]] else NULL
    if (!is.null(detail_info) && !is.null(detail_info$error)) {
      append_error(sha, "commit_detail", as.character(detail_info$error))
    }
    filenames <- NULL
    if (!is.null(detail_info) && !is.null(detail_info$filenames)) {
      filenames <- as.character(detail_info$filenames)
    } else {
      detail <- if (!is.null(detail_info)) detail_info$detail else NULL
      if (is.null(detail)) next

      files <- detail$files
      filenames <- if (!is.null(files) && length(files) > 0L) {
        stats::na.omit(vapply(files, function(f) .osv_to_scalar(f$filename), character(1)))
      } else {
        character(0)
      }
    }

    dep_mask <- commit_has_dependency_changes(filenames, patterns = dependency_patterns)
    dep_files <- filenames[dep_mask]
    requires_scan <- length(dep_files) > 0L || isTRUE(force_scan)

    if (!requires_scan) {
      summary_rows[[length(summary_rows) + 1L]] <- data.frame(
        repository = repo_full,
        sha = sha,
        date = commit_date,
        message = commit_message,
        scanned = FALSE,
        dependency_files = "",
        vulnerability_count = 0L,
          status = "skipped_no_dependency_changes",
          stringsAsFactors = FALSE
        )
      progress_tick(commit_idx)
      next
    }

    scan_attempts <- scan_attempts + 1L
    stage_msg(paste0("commit ", commit_idx, "/", total_commits, " (", substr(sha, 1, 8), "): downloading snapshot"))

    snapshot <- tryCatch(
      .download_repo_snapshot(repo_full, sha = sha, token = token, temp_root = temp_root),
      error = function(e) {
        append_error(sha, "download_snapshot", conditionMessage(e))
        NULL
      }
    )

    if (is.null(snapshot)) {
      summary_rows[[length(summary_rows) + 1L]] <- data.frame(
        repository = repo_full,
        sha = sha,
        date = commit_date,
        message = commit_message,
        scanned = FALSE,
        dependency_files = paste(dep_files, collapse = ";"),
        vulnerability_count = NA_integer_,
          status = "failed_snapshot_download",
          stringsAsFactors = FALSE
        )
      progress_tick(commit_idx)
      next
    }

    stage_msg(paste0("commit ", commit_idx, "/", total_commits, " (", substr(sha, 1, 8), "): running syft scan"))
    scan <- tryCatch(
      scan_path_with_syft_and_osv(
        target_path = snapshot$repo_dir,
        osv_db = osv_db_obj,
        syft_path = syft_resolved,
        include_uncertain = include_uncertain,
        keep_sbom = FALSE,
        syft_timeout_sec = syft_timeout_sec,
        syft_excludes = syft_excludes
      ),
      error = function(e) {
        append_error(sha, "syft_or_osv_scan", conditionMessage(e))
        NULL
      }
    )

    if (!isTRUE(keep_snapshots)) {
      unlink(snapshot$work_dir, recursive = TRUE, force = TRUE)
    }

    vuln_count <- 0L
    scan_commit_rows <- data.frame()
    if (!is.null(scan) && is.data.frame(scan$vulnerabilities) && nrow(scan$vulnerabilities) > 0L) {
      vulns <- scan$vulnerabilities
      vulns$repository <- repo_full
      vulns$sha <- sha
      vulns$commit_date <- commit_date
      vulns$commit_message <- commit_message
      vulns$dependency_files <- paste(dep_files, collapse = ";")
      scan_commit_rows <- vulns
      vulnerability_rows[[length(vulnerability_rows) + 1L]] <- vulns
      vuln_count <- nrow(vulns)
      vuln_rows_total <- vuln_rows_total + vuln_count
    }
    if (!is.null(scan)) {
      scanned_success <- scanned_success + 1L
      key_rows <- list()
      keys <- character(0)
      if (is.data.frame(scan_commit_rows) && nrow(scan_commit_rows) > 0L) {
        for (ri in seq_len(nrow(scan_commit_rows))) {
          row_i <- scan_commit_rows[ri, , drop = FALSE]
          key_i <- .vuln_lifecycle_key(row_i)
          if (!key_i %in% names(key_rows)) {
            key_rows[[key_i]] <- row_i
          }
          keys <- c(keys, key_i)
        }
      }
      scan_snapshots[[length(scan_snapshots) + 1L]] <- list(
        order = commit_idx,
        sha = sha,
        date = commit_date,
        message = commit_message,
        dependency_files = paste(dep_files, collapse = ";"),
        keys = unique(keys),
        key_rows = key_rows
      )
      stage_msg(paste0("commit ", commit_idx, "/", total_commits, " (", substr(sha, 1, 8), "): scan complete, vulnerabilities=", vuln_count))
    }

    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      repository = repo_full,
      sha = sha,
      date = commit_date,
      message = commit_message,
      scanned = !is.null(scan),
      dependency_files = paste(dep_files, collapse = ";"),
      vulnerability_count = as.integer(vuln_count),
      status = if (is.null(scan)) "scan_failed" else "scanned",
      stringsAsFactors = FALSE
    )
    progress_tick(commit_idx)
  }

  progress_tick(total_commits, force = TRUE)

  lifecycle_df <- .build_vulnerability_lifecycle(
    repo_full = repo_full,
    scan_snapshots = scan_snapshots
  )

  list(
    summary = .rbind_data_frames(summary_rows),
    vulnerabilities = .rbind_data_frames(vulnerability_rows),
    vulnerability_lifecycle = lifecycle_df,
    errors = .rbind_data_frames(error_rows)
  )
}

#' Analyze user repositories commit-by-commit with targeted dependency scans.
#'
#' The function checks each commit in each repository and runs Syft+OSV scan only
#' when dependency-manifest files were changed in that commit.
#'
#' @param profile GitHub username or profile URL.
#' @param osv_db Prepared OSV DB object or path.
#' @param token GitHub token (recommended to avoid strict API limits).
#' @param syft_path Path to `syft` executable.
#' @param dependency_patterns Regex patterns for dependency-manifest files.
#' @param max_repos Optional max repositories to process.
#' @param max_commits_per_repo Optional max commits per repository.
#' @param include_uncertain Include uncertain version matches from OSV.
#' @param keep_snapshots Keep downloaded commit snapshots.
#' @param temp_root Temp directory for commit snapshots.
#' @param force_scan If `TRUE`, run Syft scan even when dependency files were not changed.
#' @param parallel_strategy Parallel strategy:
#' `manual` (use flags as-is), `auto` (choose one safe parallel axis),
#' or `staged_all` (parallel commit prefetch stage, then parallel repo scan stage).
#' @param parallel_repos If `TRUE`, process repositories in parallel.
#' @param repo_workers Number of worker processes for repository-level parallelism.
#' @param auto_workers Optional worker budget for `parallel_strategy = "auto"`.
#' @param auto_min_repos_for_repo_parallel Minimum repos to prefer repository-level parallelism in auto mode.
#' @param auto_min_commits_for_commit_parallel Minimum expected commits to enable commit-details parallel prefetch in auto mode.
#' @param repo_progress If `TRUE`, show per-repository progress bar.
#' @param repo_progress_every Progress update step in commits for per-repository progress.
#' @param repo_progress_details If `TRUE`, show detailed per-commit stages inside repository progress.
#' @param syft_timeout_sec Timeout for each syft run in seconds (`0` means no timeout).
#' @param syft_excludes Character vector of syft `--exclude` patterns.
#' @param commit_info_parallel If `TRUE`, fetch commit detail payloads in parallel per repository.
#' @param commit_info_workers Number of workers for commit-details prefetch.
#'
#' @return List with `summary`, `vulnerabilities`, `vulnerability_lifecycle`, `errors`.
#' @export
analyze_user_commits_with_syft_osv <- function(
    profile,
    osv_db,
    token = NULL,
    syft_path = "syft.exe",
    dependency_patterns = default_dependency_manifest_patterns(),
    max_repos = NULL,
    max_commits_per_repo = NULL,
    include_uncertain = TRUE,
    keep_snapshots = FALSE,
    temp_root = tempdir(),
    force_scan = FALSE,
    parallel_strategy = c("manual", "auto", "staged_all"),
    parallel_repos = FALSE,
    repo_workers = 1L,
    auto_workers = NULL,
    auto_min_repos_for_repo_parallel = 3L,
    auto_min_commits_for_commit_parallel = 25L,
    repo_progress = FALSE,
    repo_progress_every = 10L,
    repo_progress_details = FALSE,
    syft_timeout_sec = 0L,
    syft_excludes = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 4L
) {
  stopifnot(is.character(profile), length(profile) == 1L, nzchar(profile))
  stopifnot(is.character(syft_path), length(syft_path) == 1L, nzchar(syft_path))
  stopifnot(is.logical(include_uncertain), length(include_uncertain) == 1L)
  stopifnot(is.logical(keep_snapshots), length(keep_snapshots) == 1L)
  stopifnot(is.logical(force_scan), length(force_scan) == 1L)
  parallel_strategy <- match.arg(parallel_strategy)
  stopifnot(is.logical(parallel_repos), length(parallel_repos) == 1L)
  stopifnot(is.numeric(repo_workers), length(repo_workers) == 1L, repo_workers >= 1)
  if (!is.null(auto_workers)) {
    stopifnot(is.numeric(auto_workers), length(auto_workers) == 1L, auto_workers >= 1)
  }
  stopifnot(is.numeric(auto_min_repos_for_repo_parallel), length(auto_min_repos_for_repo_parallel) == 1L, auto_min_repos_for_repo_parallel >= 1)
  stopifnot(is.numeric(auto_min_commits_for_commit_parallel), length(auto_min_commits_for_commit_parallel) == 1L, auto_min_commits_for_commit_parallel >= 1)
  stopifnot(is.logical(repo_progress), length(repo_progress) == 1L)
  stopifnot(is.numeric(repo_progress_every), length(repo_progress_every) == 1L, repo_progress_every >= 1)
  stopifnot(is.logical(repo_progress_details), length(repo_progress_details) == 1L)
  stopifnot(is.numeric(syft_timeout_sec), length(syft_timeout_sec) == 1L, syft_timeout_sec >= 0)
  stopifnot(is.logical(commit_info_parallel), length(commit_info_parallel) == 1L)
  stopifnot(is.numeric(commit_info_workers), length(commit_info_workers) == 1L, commit_info_workers >= 1)

  profile_trim <- trimws(profile)
  if (grepl("username_or_url|github_username", profile_trim, ignore.case = TRUE)) {
    stop(
      "`profile` looks like a placeholder value ('", profile_trim, "'). ",
      "Pass a real GitHub username (e.g. 'torvalds') or profile URL (e.g. 'https://github.com/torvalds').",
      call. = FALSE
    )
  }

  db_obj <- .osv_resolve_db(osv_db)
  syft_resolved <- .resolve_executable_path(syft_path)
  if (!.osv_non_empty(syft_resolved)) {
    stop(
      "Syft executable not found: '", syft_path, "'. ",
      "Pass full path to syft.exe or add it to PATH.",
      call. = FALSE
    )
  }

  if (is.character(token) && length(token) == 1L && !nzchar(token)) {
    token <- NULL
  }

  username <- NA_character_
  if (exists("extract_github_username", mode = "function")) {
    username <- tryCatch(extract_github_username(profile), error = function(e) NA_character_)
  }
  if (!.osv_non_empty(username)) {
    username <- .gh_extract_username_fallback(profile)
  }
  if (!.osv_non_empty(username)) {
    stop("Could not extract GitHub username from `profile`.", call. = FALSE)
  }

  repos_df <- NULL
  if (exists("get_github_repos", mode = "function")) {
    repos_df <- tryCatch(
      get_github_repos(username, token = token),
      error = function(e) {
        msg <- conditionMessage(e)
        if (grepl("404", msg, fixed = TRUE)) {
          stop(
            "GitHub user not found: '", username, "'. ",
            "Check the username/profile URL and try again.",
            call. = FALSE
          )
        }
        stop(msg, call. = FALSE)
      }
    )
  } else {
    .gh_require()
    repos <- tryCatch(
      gh::gh(
        "GET /users/{username}/repos",
        username = username,
        .limit = Inf,
        .token = token,
        .progress = FALSE
      ),
      error = function(e) {
        msg <- conditionMessage(e)
        if (grepl("404", msg, fixed = TRUE)) {
          stop(
            "GitHub user not found: '", username, "'. ",
            "Check the username/profile URL and try again.",
            call. = FALSE
          )
        }
        stop(msg, call. = FALSE)
      }
    )
    repos_df <- do.call(rbind, lapply(repos, function(r) {
      data.frame(full_name = .osv_null(r$full_name, NA_character_), stringsAsFactors = FALSE)
    }))
  }

  repos_df <- repos_df[!is.na(repos_df$full_name) & nzchar(repos_df$full_name), , drop = FALSE]
  if (!is.null(max_repos)) {
    repos_df <- head(repos_df, as.integer(max_repos))
  }
  if (nrow(repos_df) == 0L) {
    return(list(
      summary = data.frame(),
      vulnerabilities = data.frame(),
      vulnerability_lifecycle = data.frame(),
      errors = data.frame()
    ))
  }

  repo_list <- repos_df$full_name
  repo_count <- length(repo_list)

  parallel_repos_effective <- isTRUE(parallel_repos)
  repo_workers_effective <- as.integer(repo_workers)
  commit_info_parallel_effective <- isTRUE(commit_info_parallel)
  commit_info_workers_effective <- as.integer(commit_info_workers)
  staged_all_mode <- identical(parallel_strategy, "staged_all")
  repo_prefetch_plans <- NULL

  if (identical(parallel_strategy, "auto")) {
    if (!requireNamespace("parallel", quietly = TRUE)) {
      parallel_repos_effective <- FALSE
      repo_workers_effective <- 1L
      commit_info_parallel_effective <- FALSE
      commit_info_workers_effective <- 1L
      message("[auto] package 'parallel' is unavailable; running sequentially.")
    } else {
      detected <- if (is.null(auto_workers)) {
        suppressWarnings(parallel::detectCores(logical = TRUE))
      } else {
        as.integer(auto_workers)
      }
      if (!is.finite(detected) || is.na(detected)) {
        detected <- 1L
      }
      worker_budget <- max(1L, as.integer(detected) - if (is.null(auto_workers)) 1L else 0L)

      if (repo_count >= as.integer(auto_min_repos_for_repo_parallel) && worker_budget >= 2L) {
        parallel_repos_effective <- TRUE
        repo_workers_effective <- min(worker_budget, repo_count)
        commit_info_parallel_effective <- FALSE
        commit_info_workers_effective <- 1L
        message("[auto] selected repository-level parallelism: workers=", repo_workers_effective, ", repos=", repo_count)
      } else {
        expected_commits <- if (!is.null(max_commits_per_repo)) as.integer(max_commits_per_repo) else NA_integer_
        commit_parallel_ok <- worker_budget >= 2L &&
          (is.na(expected_commits) || expected_commits >= as.integer(auto_min_commits_for_commit_parallel))

        parallel_repos_effective <- FALSE
        repo_workers_effective <- 1L
        commit_info_parallel_effective <- isTRUE(commit_parallel_ok)
        commit_info_workers_effective <- if (isTRUE(commit_parallel_ok)) min(worker_budget, 8L) else 1L

        if (isTRUE(commit_info_parallel_effective)) {
          message("[auto] selected commit-details parallel prefetch: workers=", commit_info_workers_effective, ".")
        } else {
          message("[auto] selected sequential mode (insufficient workload/workers for safe speedup).")
        }
      }
    }
  }

  if (staged_all_mode) {
    detected <- if (is.null(auto_workers)) {
      if (requireNamespace("parallel", quietly = TRUE)) suppressWarnings(parallel::detectCores(logical = TRUE)) else 1L
    } else {
      as.integer(auto_workers)
    }
    if (!is.finite(detected) || is.na(detected)) detected <- 1L
    worker_budget <- max(1L, as.integer(detected) - if (is.null(auto_workers)) 1L else 0L)

    parallel_repos_effective <- repo_count > 1L && worker_budget >= 2L && requireNamespace("parallel", quietly = TRUE)
    repo_workers_effective <- if (parallel_repos_effective) min(worker_budget, repo_count) else 1L
    commit_info_parallel_effective <- worker_budget >= 2L && requireNamespace("parallel", quietly = TRUE)
    commit_info_workers_effective <- if (commit_info_parallel_effective) min(worker_budget, 8L) else 1L

    message(
      "[staged_all] stage 1/2 prefetch: commit_parallel=", commit_info_parallel_effective,
      " (workers=", commit_info_workers_effective, "); ",
      "stage 2/2 scan: repo_parallel=", parallel_repos_effective,
      " (workers=", repo_workers_effective, ")."
    )

    repo_prefetch_plans <- vector("list", length(repo_list))
    names(repo_prefetch_plans) <- repo_list
    prefetch_cl <- NULL

    if (isTRUE(commit_info_parallel_effective) && as.integer(commit_info_workers_effective) > 1L) {
      prefetch_cl <- parallel::makeCluster(as.integer(commit_info_workers_effective))
      on.exit(parallel::stopCluster(prefetch_cl), add = TRUE)
      message("[staged_all] prefetch worker pool started: workers=", as.integer(commit_info_workers_effective))
    }

    for (repo_full in repo_list) {
      message("[staged_all][prefetch] ", repo_full)
      repo_prefetch_plans[[repo_full]] <- .build_repo_prefetch_plan(
        repo_full = repo_full,
        token = token,
        max_commits_per_repo = max_commits_per_repo,
        commit_info_parallel = commit_info_parallel_effective,
        commit_info_workers = commit_info_workers_effective,
        emit_messages = FALSE,
        prefetch_cluster = prefetch_cl
      )
    }
  }

  use_parallel <- isTRUE(parallel_repos_effective) &&
    length(repo_list) > 1L &&
    as.integer(repo_workers_effective) > 1L &&
    requireNamespace("parallel", quietly = TRUE)

  repo_results <- list()

  if (use_parallel) {
    workers <- min(as.integer(repo_workers_effective), length(repo_list))
    message("Processing repositories in parallel: workers=", workers, ", repos=", length(repo_list))
    if (isTRUE(repo_progress)) {
      message("`repo_progress` is disabled in parallel mode (worker output is not streamed reliably).")
    }
    if (isTRUE(commit_info_parallel_effective) && !isTRUE(staged_all_mode)) {
      message("`commit_info_parallel` is disabled when `parallel_repos=TRUE` to avoid nested parallel workers.")
    }

    is_mongo_backend <- inherits(db_obj, "osv_mongo_database")
    worker_db_dir <- if (!is_mongo_backend) db_obj$db_dir else NULL
    worker_mongo_cfg <- if (is_mongo_backend) {
      list(
        mongo_url = db_obj$mongo_url,
        db_name = db_obj$db_name,
        collection = .osv_null(db_obj$collection, "vulns")
      )
    } else {
      NULL
    }
    export_env <- environment(analyze_user_commits_with_syft_osv)
    required_fns <- c(
      # repository analysis
      ".analyze_repository_commits",
      ".prefetch_commit_details",
      ".gh_require",
      ".gh_repo_commits",
      ".gh_commit_detail",
      ".download_repo_snapshot",
      "commit_has_dependency_changes",
      ".osv_to_scalar",
      ".osv_null",
      ".osv_non_empty",
      ".repo_progress_bar",
      ".repo_format_duration",
      ".parse_commit_datetime",
      ".vuln_lifecycle_key",
      ".build_vulnerability_lifecycle",
      ".rbind_data_frames",
      # syft + osv chain
      "scan_path_with_syft_and_osv",
      "scan_sbom_components_with_osv",
      "parse_cyclonedx_sbom",
      "generate_syft_cyclonedx_sbom",
      "search_osv_vulnerabilities",
      "load_osv_database",
      "load_osv_mongo_database",
      "search_osv_vulnerabilities_mongo",
      ".osv_get_mongo_connection",
      # osv search helpers
      ".osv_build_pos_index",
      ".osv_pos_lookup",
      ".osv_get_db_cache",
      ".osv_get_entry_cached",
      ".osv_resolve_db",
      ".osv_extract_candidate_rows",
      ".osv_is_package_match",
      ".osv_eval_events",
      ".osv_is_version_affected",
      ".osv_collect_entry_match",
      # osv generic helpers
      ".osv_parse_purl",
      ".osv_normalize_purl",
      ".osv_map_purl_type_to_ecosystem",
      ".osv_candidate_package_names_from_purl",
      ".osv_normalize_package_key",
      ".osv_compare_versions_loose",
      # sbom helpers
      ".syft_collect_components"
    )

    missing_fns <- required_fns[!vapply(required_fns, exists, logical(1), mode = "function", inherits = TRUE)]
    if (length(missing_fns) > 0L) {
      stop(
        "Parallel worker setup failed: required functions are not loaded: ",
        paste(missing_fns, collapse = ", "),
        ". Source all package R scripts (or install/load the package) before running parallel mode.",
        call. = FALSE
      )
    }
    required_objects <- c(".osv_search_runtime_cache")
    if (exists(".osv_mongo_connection_cache", inherits = TRUE)) {
      required_objects <- c(required_objects, ".osv_mongo_connection_cache")
    }
    missing_objects <- required_objects[!vapply(required_objects, exists, logical(1), inherits = TRUE)]
    if (length(missing_objects) > 0L) {
      stop(
        "Parallel worker setup failed: required objects are not loaded: ",
        paste(missing_objects, collapse = ", "),
        ". Source all package R scripts (or install/load the package) before running parallel mode.",
        call. = FALSE
      )
    }

    cl <- parallel::makeCluster(workers)
    on.exit(parallel::stopCluster(cl), add = TRUE)

    parallel::clusterExport(cl, varlist = required_fns, envir = export_env)
    parallel::clusterExport(cl, varlist = required_objects, envir = export_env)
    if (!is_mongo_backend) {
      parallel::clusterExport(cl, varlist = c("worker_db_dir"), envir = environment())
      parallel::clusterEvalQ(cl, {
        .gitariki_worker_osv_db <- load_osv_database(db_dir = worker_db_dir, load_index = TRUE)
        NULL
      })
    } else {
      parallel::clusterExport(cl, varlist = c("worker_mongo_cfg"), envir = environment())
      parallel::clusterEvalQ(cl, {
        .gitariki_worker_osv_db <- load_osv_mongo_database(
          mongo_url = worker_mongo_cfg$mongo_url,
          db_name = worker_mongo_cfg$db_name,
          collection = worker_mongo_cfg$collection,
          validate = FALSE
        )
        NULL
      })
    }

    repo_tasks <- lapply(repo_list, function(repo_full) {
      list(
        repo_full = repo_full,
        plan = if (isTRUE(staged_all_mode)) repo_prefetch_plans[[repo_full]] else NULL
      )
    })

    repo_results <- parallel::parLapply(
      cl = cl,
      X = repo_tasks,
      fun = function(task, token, max_commits_per_repo, dependency_patterns, force_scan, temp_root,
                     keep_snapshots, include_uncertain, syft_resolved, repo_progress_every, syft_timeout_sec, syft_excludes,
                     staged_all_mode) {
        repo_full <- task$repo_full
        plan <- task$plan
        tryCatch(
          .analyze_repository_commits(
            repo_full = repo_full,
            token = token,
            max_commits_per_repo = max_commits_per_repo,
            dependency_patterns = dependency_patterns,
            force_scan = force_scan,
            temp_root = temp_root,
            keep_snapshots = keep_snapshots,
            include_uncertain = include_uncertain,
            syft_resolved = syft_resolved,
            osv_db_obj = .gitariki_worker_osv_db,
            emit_messages = FALSE,
            repo_progress = FALSE,
            repo_progress_every = repo_progress_every,
            repo_progress_details = FALSE,
            syft_timeout_sec = syft_timeout_sec,
            syft_excludes = syft_excludes,
            commit_info_parallel = FALSE,
            commit_info_workers = 1L,
            prefetched_commits = if (!is.null(plan)) plan$commits else NULL,
            prefetched_details = if (!is.null(plan)) plan$details else NULL,
            prefetched_errors = if (!is.null(plan)) plan$errors else NULL
          ),
          error = function(e) {
            list(
              summary = data.frame(
                repository = repo_full,
                sha = NA_character_,
                date = NA_character_,
                message = NA_character_,
                scanned = FALSE,
                dependency_files = "",
                vulnerability_count = NA_integer_,
                status = "parallel_repo_failed",
                stringsAsFactors = FALSE
              ),
              vulnerabilities = data.frame(),
              vulnerability_lifecycle = data.frame(),
              errors = data.frame(
                repository = repo_full,
                sha = NA_character_,
                stage = "parallel_repo_worker",
                error = conditionMessage(e),
                stringsAsFactors = FALSE
              )
            )
          }
        )
      },
      token = token,
      max_commits_per_repo = max_commits_per_repo,
      dependency_patterns = dependency_patterns,
      force_scan = force_scan,
      temp_root = temp_root,
      keep_snapshots = keep_snapshots,
      include_uncertain = include_uncertain,
      syft_resolved = syft_resolved,
      repo_progress_every = as.integer(repo_progress_every),
      syft_timeout_sec = as.integer(syft_timeout_sec),
      syft_excludes = syft_excludes,
      staged_all_mode = staged_all_mode
    )
  } else {
    if (isTRUE(parallel_repos_effective) && as.integer(repo_workers_effective) > 1L && !requireNamespace("parallel", quietly = TRUE)) {
      message("Package 'parallel' is unavailable; falling back to sequential repository processing.")
    }
    for (repo_full in repo_list) {
      repo_results[[length(repo_results) + 1L]] <- .analyze_repository_commits(
        repo_full = repo_full,
        token = token,
        max_commits_per_repo = max_commits_per_repo,
        dependency_patterns = dependency_patterns,
        force_scan = force_scan,
        temp_root = temp_root,
        keep_snapshots = keep_snapshots,
        include_uncertain = include_uncertain,
        syft_resolved = syft_resolved,
        osv_db_obj = db_obj,
        emit_messages = TRUE,
        repo_progress = repo_progress,
        repo_progress_every = repo_progress_every,
        repo_progress_details = repo_progress_details,
        syft_timeout_sec = syft_timeout_sec,
        syft_excludes = syft_excludes,
        commit_info_parallel = if (isTRUE(staged_all_mode)) FALSE else commit_info_parallel_effective,
        commit_info_workers = if (isTRUE(staged_all_mode)) 1L else as.integer(commit_info_workers_effective),
        prefetched_commits = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$commits, NULL) else NULL,
        prefetched_details = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$details, NULL) else NULL,
        prefetched_errors = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$errors, NULL) else NULL
      )
    }
  }

  summary_df <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$summary, data.frame())),
    template_cols = c("repository", "sha", "date", "message", "scanned", "dependency_files", "vulnerability_count", "status")
  )
  vulnerabilities_df <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$vulnerabilities, data.frame())),
    template_cols = c(
      "osv_id", "summary", "details", "published", "modified", "aliases", "references",
      "query_purl", "query_version", "matched_purl", "matched_package", "matched_ecosystem",
      "affected", "match_reason", "source_json",
      "component_bom_ref", "component_type", "component_name", "component_version", "component_purl",
      "repository", "sha", "commit_date", "commit_message", "dependency_files"
    )
  )
  lifecycle_df <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$vulnerability_lifecycle, data.frame())),
    template_cols = c(
      "repository", "vulnerability_key", "osv_id",
      "query_purl", "matched_purl", "matched_package", "matched_ecosystem",
      "component_name", "component_purl",
      "introduced_sha", "introduced_date", "introduced_message", "introduced_dependency_files",
      "fixed_sha", "fixed_date", "fixed_message", "fixed_dependency_files",
      "status"
    )
  )
  errors_df <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$errors, data.frame())),
    template_cols = c("repository", "sha", "stage", "error")
  )

  list(
    summary = summary_df,
    vulnerabilities = vulnerabilities_df,
    vulnerability_lifecycle = lifecycle_df,
    errors = errors_df
  )
}

#' Quick diagnostic summary for commit scan results.
#'
#' @param result Object returned by `analyze_user_commits_with_syft_osv()`.
#'
#' @return List with status counts and top errors.
#' @export
diagnose_commit_scan_result <- function(result) {
  if (!is.list(result) || is.null(result$summary) || is.null(result$errors)) {
    stop("`result` must be output of analyze_user_commits_with_syft_osv().", call. = FALSE)
  }

  summary_df <- result$summary
  errors_df <- result$errors
  vulns_df <- .osv_null(result$vulnerabilities, data.frame())
  lifecycle_df <- .osv_null(result$vulnerability_lifecycle, data.frame())

  status_counts <- if (nrow(summary_df) > 0L && "status" %in% names(summary_df)) {
    as.data.frame(table(summary_df$status), stringsAsFactors = FALSE)
  } else {
    data.frame(Var1 = character(0), Freq = integer(0), stringsAsFactors = FALSE)
  }
  names(status_counts) <- c("status", "n")

  stage_counts <- if (nrow(errors_df) > 0L && "stage" %in% names(errors_df)) {
    as.data.frame(table(errors_df$stage), stringsAsFactors = FALSE)
  } else {
    data.frame(Var1 = character(0), Freq = integer(0), stringsAsFactors = FALSE)
  }
  names(stage_counts) <- c("stage", "n")

  lifecycle_status_counts <- if (nrow(lifecycle_df) > 0L && "status" %in% names(lifecycle_df)) {
    as.data.frame(table(lifecycle_df$status), stringsAsFactors = FALSE)
  } else {
    data.frame(Var1 = character(0), Freq = integer(0), stringsAsFactors = FALSE)
  }
  names(lifecycle_status_counts) <- c("status", "n")

  list(
    total_commits = nrow(summary_df),
    scanned_commits = if ("scanned" %in% names(summary_df)) sum(summary_df$scanned %in% TRUE, na.rm = TRUE) else 0L,
    vulnerability_rows = nrow(vulns_df),
    lifecycle_rows = nrow(lifecycle_df),
    fixed_vulnerabilities = if ("status" %in% names(lifecycle_df)) sum(lifecycle_df$status %in% "fixed", na.rm = TRUE) else 0L,
    open_vulnerabilities = if ("status" %in% names(lifecycle_df)) sum(lifecycle_df$status %in% "open", na.rm = TRUE) else 0L,
    status_counts = status_counts[order(-status_counts$n), , drop = FALSE],
    lifecycle_status_counts = lifecycle_status_counts[order(-lifecycle_status_counts$n), , drop = FALSE],
    error_stage_counts = stage_counts[order(-stage_counts$n), , drop = FALSE],
    sample_errors = head(errors_df, 20)
  )
}
