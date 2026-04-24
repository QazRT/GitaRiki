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

.debug_log <- function(enabled, stage, ...) {
  if (!isTRUE(enabled)) return(invisible(NULL))
  message("[debug][", stage, "] ", paste0(..., collapse = ""))
  invisible(NULL)
}

.gh_repo_commits <- function(repo_full, token = NULL, max_commits = NULL, since = NULL) {
  .gh_require()
  args <- list(
    endpoint = "GET /repos/{repo_full}/commits",
    repo_full = repo_full,
    .token = token,
    .progress = FALSE
  )
  if (.osv_non_empty(since)) {
    args$since <- as.character(since)
  }
  args$.limit <- if (is.null(max_commits)) Inf else as.integer(max_commits)
  do.call(gh::gh, args)
}

.as_logical_flag <- function(x) {
  if (is.logical(x)) {
    return(ifelse(is.na(x), FALSE, x))
  }
  if (is.numeric(x)) {
    return(!is.na(x) & x != 0)
  }
  vals <- tolower(trimws(as.character(x)))
  vals %in% c("1", "true", "t", "yes", "y")
}

.dedupe_keep_last <- function(df, key_cols) {
  if (!is.data.frame(df) || nrow(df) <= 1L) return(df)
  keys <- key_cols[key_cols %in% names(df)]
  if (length(keys) == 0L) {
    out <- unique(df)
    rownames(out) <- NULL
    return(out)
  }

  key_vec <- do.call(paste, c(lapply(keys, function(k) as.character(df[[k]])), sep = "\r"))
  out <- df[!duplicated(key_vec, fromLast = TRUE), , drop = FALSE]
  rownames(out) <- NULL
  out
}

.ch_cache_is_available <- function(conn) {
  !is.null(conn) &&
    exists("query_clickhouse", mode = "function") &&
    exists("load_df_to_clickhouse", mode = "function")
}

.ch_cache_tables <- function(prefix = "github_commit_scan") {
  list(
    summary = paste0(prefix, "_summary"),
    vulnerabilities = paste0(prefix, "_vulnerabilities"),
    lifecycle = paste0(prefix, "_lifecycle"),
    errors = paste0(prefix, "_errors")
  )
}

.ch_escape_sql <- function(x) {
  if (exists("escape_sql_string", mode = "function")) {
    return(escape_sql_string(as.character(x)))
  }
  y <- gsub("\\\\", "\\\\\\\\", as.character(x))
  gsub("'", "\\\\'", y)
}

.ch_table_ident <- function(conn, table_name) {
  if (exists("quote_table_ident", mode = "function") && is.list(conn) && !is.null(conn$dbname)) {
    return(quote_table_ident(table_name, conn$dbname))
  }
  table_name
}

.ch_query_safe <- function(conn, sql) {
  tryCatch(
    query_clickhouse(conn, sql),
    error = function(e) data.frame()
  )
}

.ch_read_profile_table <- function(conn, table_name, profile) {
  table_sql <- .ch_table_ident(conn, table_name)
  sql <- paste0(
    "SELECT * FROM ",
    table_sql,
    " WHERE profile = '",
    .ch_escape_sql(profile),
    "'"
  )
  .ch_query_safe(conn, sql)
}

.ch_delete_profile_rows <- function(conn, table_name, profile) {
  table_sql <- .ch_table_ident(conn, table_name)
  sql_sync <- paste0(
    "ALTER TABLE ",
    table_sql,
    " DELETE WHERE profile = '",
    .ch_escape_sql(profile),
    "' SETTINGS mutations_sync = 1"
  )
  sql_async <- paste0(
    "ALTER TABLE ",
    table_sql,
    " DELETE WHERE profile = '",
    .ch_escape_sql(profile),
    "'"
  )

  if (exists("clickhouse_request", mode = "function")) {
    tryCatch(
      clickhouse_request(conn, sql_sync, parse_json = FALSE),
      error = function(e) {
        # Fallback for ClickHouse versions/settings where inline SETTINGS is unavailable.
        tryCatch(
          clickhouse_request(conn, sql_async, parse_json = FALSE),
          error = function(e2) NULL
        )
      }
    )
  }
}

.ch_add_column_if_missing <- function(conn, table_name, column_name, column_type = "Nullable(String)") {
  if (!exists("clickhouse_request", mode = "function")) return(invisible(FALSE))
  if (!is.character(column_name) || length(column_name) != 1L || !nzchar(column_name)) return(invisible(FALSE))
  if (!grepl("^[A-Za-z_][A-Za-z0-9_]*$", column_name)) return(invisible(FALSE))

  table_sql <- .ch_table_ident(conn, table_name)
  sql <- paste0(
    "ALTER TABLE ",
    table_sql,
    " ADD COLUMN IF NOT EXISTS ",
    column_name,
    " ",
    column_type
  )

  tryCatch(
    {
      clickhouse_request(conn, sql, parse_json = FALSE)
      TRUE
    },
    error = function(e) FALSE
  )
}

.merge_commit_scan_frames <- function(cached_df, new_df, template_cols, key_cols) {
  as_template <- function(df) {
    if (!is.data.frame(df) || nrow(df) == 0L) {
      out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
      return(out[0, , drop = FALSE])
    }
    out <- df
    missing_cols <- setdiff(template_cols, names(out))
    if (length(missing_cols) > 0L) {
      for (nm in missing_cols) out[[nm]] <- NA
    }
    out <- out[, template_cols, drop = FALSE]
    rownames(out) <- NULL
    out
  }

  merged <- .rbind_data_frames(
    list(as_template(.osv_null(cached_df, data.frame())), as_template(.osv_null(new_df, data.frame()))),
    template_cols = template_cols
  )
  .dedupe_keep_last(merged, key_cols = key_cols)
}

.rebuild_lifecycle_from_summary_and_vulnerabilities <- function(summary_df, vulnerabilities_df) {
  template_cols <- c(
    "repository", "vulnerability_key", "osv_id",
    "query_purl", "matched_purl", "matched_package", "matched_ecosystem",
    "component_name", "component_purl",
    "introduced_sha", "introduced_date", "introduced_message", "introduced_author", "introduced_dependency_files",
    "fixed_sha", "fixed_date", "fixed_message", "fixed_author", "fixed_dependency_files",
    "status"
  )

  if (!is.data.frame(summary_df) || nrow(summary_df) == 0L || !"repository" %in% names(summary_df)) {
    out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
    return(out[0, , drop = FALSE])
  }

  summary_df$scanned <- if ("scanned" %in% names(summary_df)) .as_logical_flag(summary_df$scanned) else FALSE
  scanned_rows <- summary_df[summary_df$scanned %in% TRUE, , drop = FALSE]
  if (nrow(scanned_rows) == 0L) {
    out <- as.data.frame(setNames(vector("list", length(template_cols)), template_cols), stringsAsFactors = FALSE)
    return(out[0, , drop = FALSE])
  }

  repos <- unique(as.character(scanned_rows$repository))
  per_repo <- vector("list", length(repos))

  for (ri in seq_along(repos)) {
    repo_full <- repos[[ri]]
    srepo <- scanned_rows[as.character(scanned_rows$repository) == repo_full, , drop = FALSE]
    if (nrow(srepo) == 0L) next

    ts <- if ("date" %in% names(srepo)) {
      vapply(seq_len(nrow(srepo)), function(i) {
        dt <- .parse_commit_datetime(srepo$date[[i]])
        if (is.na(dt)) Inf else as.numeric(dt)
      }, numeric(1))
    } else {
      rep(Inf, nrow(srepo))
    }

    ord <- order(ts, seq_len(nrow(srepo)))
    srepo <- srepo[ord, , drop = FALSE]
    snapshots <- vector("list", nrow(srepo))

    for (i in seq_len(nrow(srepo))) {
      sha_i <- if ("sha" %in% names(srepo)) as.character(srepo$sha[[i]]) else NA_character_
      vr <- vulnerabilities_df
      if (!is.data.frame(vr) || nrow(vr) == 0L || !all(c("repository", "sha") %in% names(vr))) {
        vr <- data.frame()
      } else {
        vr <- vr[as.character(vr$repository) == repo_full & as.character(vr$sha) == sha_i, , drop = FALSE]
      }

      keys <- character(0)
      key_rows <- list()
      if (is.data.frame(vr) && nrow(vr) > 0L) {
        for (vj in seq_len(nrow(vr))) {
          row_j <- vr[vj, , drop = FALSE]
          key_j <- .vuln_lifecycle_key(row_j)
          if (!key_j %in% names(key_rows)) key_rows[[key_j]] <- row_j
          keys <- c(keys, key_j)
        }
      }

      snapshots[[i]] <- list(
        order = i,
        sha = sha_i,
        date = if ("date" %in% names(srepo)) as.character(srepo$date[[i]]) else NA_character_,
        message = if ("message" %in% names(srepo)) as.character(srepo$message[[i]]) else NA_character_,
        author = if ("commit_author" %in% names(srepo)) as.character(srepo$commit_author[[i]]) else NA_character_,
        dependency_files = if ("dependency_files" %in% names(srepo)) as.character(srepo$dependency_files[[i]]) else "",
        keys = unique(keys),
        key_rows = key_rows
      )
    }

    per_repo[[ri]] <- .build_vulnerability_lifecycle(repo_full = repo_full, scan_snapshots = snapshots)
  }

  .rbind_data_frames(per_repo, template_cols = template_cols)
}

.extract_commit_author_from_detail <- function(detail) {
  if (is.null(detail)) return(NA_character_)
  login <- .osv_to_scalar(detail$author$login)
  if (.osv_non_empty(login)) return(login)
  name <- .osv_to_scalar(detail$commit$author$name)
  if (.osv_non_empty(name)) return(name)
  NA_character_
}

.enrich_lifecycle_authors <- function(summary_df, lifecycle_df, token = NULL, debug = FALSE) {
  out <- list(summary = summary_df, lifecycle = lifecycle_df)
  if (!is.data.frame(lifecycle_df) || nrow(lifecycle_df) == 0L) return(out)
  if (!all(c("repository", "introduced_sha", "fixed_sha") %in% names(lifecycle_df))) return(out)

  is_missing_author <- function(x) {
    v <- as.character(.osv_null(x, NA_character_))
    is.na(v) | !nzchar(trimws(v))
  }

  lc <- lifecycle_df
  if (!("introduced_author" %in% names(lc))) lc$introduced_author <- NA_character_
  if (!("fixed_author" %in% names(lc))) lc$fixed_author <- NA_character_
  lc$introduced_author <- as.character(lc$introduced_author)
  lc$fixed_author <- as.character(lc$fixed_author)

  sm <- summary_df
  if (!is.data.frame(sm)) sm <- data.frame()
  if (!("commit_author" %in% names(sm))) sm$commit_author <- NA_character_
  if ("commit_author" %in% names(sm)) sm$commit_author <- as.character(sm$commit_author)

  summary_map <- list()
  if (nrow(sm) > 0L && all(c("repository", "sha", "commit_author") %in% names(sm))) {
    for (i in seq_len(nrow(sm))) {
      repo_i <- as.character(sm$repository[[i]])
      sha_i <- as.character(sm$sha[[i]])
      author_i <- as.character(sm$commit_author[[i]])
      if (!.osv_non_empty(repo_i) || !.osv_non_empty(sha_i) || !.osv_non_empty(author_i)) next
      summary_map[[paste(repo_i, sha_i, sep = "\r")]] <- author_i
    }
  }

  fill_from_summary <- function(sha_col, author_col) {
    for (i in seq_len(nrow(lc))) {
      if (!is_missing_author(lc[[author_col]][[i]])) next
      repo_i <- as.character(lc$repository[[i]])
      sha_i <- as.character(lc[[sha_col]][[i]])
      if (!.osv_non_empty(repo_i) || !.osv_non_empty(sha_i)) next
      key <- paste(repo_i, sha_i, sep = "\r")
      author <- summary_map[[key]]
      if (.osv_non_empty(author)) lc[[author_col]][[i]] <<- author
    }
  }

  fill_from_summary("introduced_sha", "introduced_author")
  fill_from_summary("fixed_sha", "fixed_author")

  need_fetch <- data.frame(repository = character(0), sha = character(0), stringsAsFactors = FALSE)
  for (i in seq_len(nrow(lc))) {
    repo_i <- as.character(lc$repository[[i]])
    intro_sha <- as.character(lc$introduced_sha[[i]])
    fixed_sha <- as.character(lc$fixed_sha[[i]])
    if (is_missing_author(lc$introduced_author[[i]]) && .osv_non_empty(repo_i) && .osv_non_empty(intro_sha)) {
      need_fetch <- rbind(need_fetch, data.frame(repository = repo_i, sha = intro_sha, stringsAsFactors = FALSE))
    }
    if (is_missing_author(lc$fixed_author[[i]]) && .osv_non_empty(repo_i) && .osv_non_empty(fixed_sha)) {
      need_fetch <- rbind(need_fetch, data.frame(repository = repo_i, sha = fixed_sha, stringsAsFactors = FALSE))
    }
  }

  if (nrow(need_fetch) > 0L) {
    need_fetch <- unique(need_fetch)
  }

  can_fetch <- .osv_non_empty(token) && nrow(need_fetch) > 0L
  fetched_map <- list()
  if (isTRUE(can_fetch)) {
    .debug_log(debug, "authors", "fetching missing commit authors from GitHub: ", nrow(need_fetch), " commits")
    for (i in seq_len(nrow(need_fetch))) {
      repo_i <- as.character(need_fetch$repository[[i]])
      sha_i <- as.character(need_fetch$sha[[i]])
      key <- paste(repo_i, sha_i, sep = "\r")
      detail_info <- .gh_commit_detail_safe(repo_full = repo_i, sha = sha_i, token = token)
      if (!is.null(detail_info$error) || is.null(detail_info$detail)) next
      author_i <- .extract_commit_author_from_detail(detail_info$detail)
      if (.osv_non_empty(author_i)) {
        fetched_map[[key]] <- author_i
      }
    }
    .debug_log(debug, "authors", "fetched authors: ", length(fetched_map), "/", nrow(need_fetch))
  }

  fill_from_fetched <- function(sha_col, author_col) {
    for (i in seq_len(nrow(lc))) {
      if (!is_missing_author(lc[[author_col]][[i]])) next
      repo_i <- as.character(lc$repository[[i]])
      sha_i <- as.character(lc[[sha_col]][[i]])
      if (!.osv_non_empty(repo_i) || !.osv_non_empty(sha_i)) next
      key <- paste(repo_i, sha_i, sep = "\r")
      author <- fetched_map[[key]]
      if (.osv_non_empty(author)) lc[[author_col]][[i]] <<- author
    }
  }

  fill_from_fetched("introduced_sha", "introduced_author")
  fill_from_fetched("fixed_sha", "fixed_author")

  if (length(fetched_map) > 0L && nrow(sm) > 0L && all(c("repository", "sha", "commit_author") %in% names(sm))) {
    for (i in seq_len(nrow(sm))) {
      repo_i <- as.character(sm$repository[[i]])
      sha_i <- as.character(sm$sha[[i]])
      key <- paste(repo_i, sha_i, sep = "\r")
      if (!is_missing_author(sm$commit_author[[i]]) || is.null(fetched_map[[key]])) next
      sm$commit_author[[i]] <- fetched_map[[key]]
    }
  }

  out$summary <- sm
  out$lifecycle <- lc
  out
}

.ch_load_commit_scan_cache <- function(conn, profile, repos = NULL, table_prefix = "github_commit_scan") {
  empty <- list(
    summary = data.frame(),
    vulnerabilities = data.frame(),
    lifecycle = data.frame(),
    errors = data.frame(),
    since_by_repo = list(),
    known_shas_by_repo = list()
  )
  if (!.ch_cache_is_available(conn)) return(empty)

  tables <- .ch_cache_tables(prefix = table_prefix)
  summary_df <- .ch_read_profile_table(conn, tables$summary, profile)
  vulnerabilities_df <- .ch_read_profile_table(conn, tables$vulnerabilities, profile)
  lifecycle_df <- .ch_read_profile_table(conn, tables$lifecycle, profile)
  errors_df <- .ch_read_profile_table(conn, tables$errors, profile)

  strip_service <- function(df) {
    if (!is.data.frame(df)) return(data.frame())
    drop_cols <- intersect(c("profile", "cached_at"), names(df))
    if (length(drop_cols) > 0L) {
      df <- df[, setdiff(names(df), drop_cols), drop = FALSE]
    }
    df
  }

  summary_df <- strip_service(summary_df)
  vulnerabilities_df <- strip_service(vulnerabilities_df)
  lifecycle_df <- strip_service(lifecycle_df)
  errors_df <- strip_service(errors_df)

  if (!is.null(repos) && length(repos) > 0L) {
    repos_chr <- unique(as.character(repos))
    by_repo_filter <- function(df) {
      if (!is.data.frame(df) || nrow(df) == 0L || !("repository" %in% names(df))) return(df)
      df[as.character(df$repository) %in% repos_chr, , drop = FALSE]
    }
    summary_df <- by_repo_filter(summary_df)
    vulnerabilities_df <- by_repo_filter(vulnerabilities_df)
    lifecycle_df <- by_repo_filter(lifecycle_df)
    errors_df <- by_repo_filter(errors_df)
  }

  if ("scanned" %in% names(summary_df)) {
    summary_df$scanned <- .as_logical_flag(summary_df$scanned)
  }

  known_shas_by_repo <- list()
  if (nrow(summary_df) > 0L && all(c("repository", "sha") %in% names(summary_df))) {
    summary_non_empty <- summary_df[!is.na(summary_df$sha) & nzchar(as.character(summary_df$sha)), , drop = FALSE]
    if (nrow(summary_non_empty) > 0L) {
      known_shas_by_repo <- split(as.character(summary_non_empty$sha), as.character(summary_non_empty$repository))
      known_shas_by_repo <- lapply(known_shas_by_repo, unique)
    }
  }

  since_by_repo <- list()
  if (nrow(summary_df) > 0L && all(c("repository", "date", "scanned") %in% names(summary_df))) {
    scanned_df <- summary_df[summary_df$scanned %in% TRUE, , drop = FALSE]
    repo_vals <- unique(as.character(scanned_df$repository))
    for (repo_full in repo_vals) {
      srepo <- scanned_df[as.character(scanned_df$repository) == repo_full, , drop = FALSE]
      if (nrow(srepo) == 0L) next
      ts <- vapply(seq_len(nrow(srepo)), function(i) {
        dt <- .parse_commit_datetime(srepo$date[[i]])
        if (is.na(dt)) NA_real_ else as.numeric(dt)
      }, numeric(1))
      ts <- ts[is.finite(ts)]
      if (length(ts) == 0L) next
      # 1 second overlap to avoid edge misses; duplicates are removed by SHA.
      since_dt <- as.POSIXct(max(ts), origin = "1970-01-01", tz = "UTC") - 1
      since_by_repo[[repo_full]] <- format(since_dt, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    }
  }

  list(
    summary = summary_df,
    vulnerabilities = vulnerabilities_df,
    lifecycle = lifecycle_df,
    errors = errors_df,
    since_by_repo = since_by_repo,
    known_shas_by_repo = known_shas_by_repo
  )
}

.ch_save_commit_scan_cache <- function(conn, profile, summary_df, vulnerabilities_df, lifecycle_df, errors_df, table_prefix = "github_commit_scan") {
  if (!.ch_cache_is_available(conn)) return(invisible(FALSE))
  tables <- .ch_cache_tables(prefix = table_prefix)

  enrich <- function(df) {
    if (!is.data.frame(df)) df <- data.frame()
    out <- df
    n <- nrow(out)
    out$profile <- rep(as.character(profile), n)
    out$cached_at <- rep(format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), n)
    out
  }

  summary_store <- enrich(summary_df)
  vulnerabilities_store <- enrich(vulnerabilities_df)
  lifecycle_store <- enrich(lifecycle_df)
  errors_store <- enrich(errors_df)

  # Schema evolution for previously created cache tables.
  .ch_add_column_if_missing(conn, tables$summary, "commit_author", "Nullable(String)")
  .ch_add_column_if_missing(conn, tables$lifecycle, "introduced_author", "Nullable(String)")
  .ch_add_column_if_missing(conn, tables$lifecycle, "fixed_author", "Nullable(String)")

  .ch_delete_profile_rows(conn, tables$summary, profile)
  .ch_delete_profile_rows(conn, tables$vulnerabilities, profile)
  .ch_delete_profile_rows(conn, tables$lifecycle, profile)
  .ch_delete_profile_rows(conn, tables$errors, profile)

  if (nrow(summary_store) > 0L) {
    load_df_to_clickhouse(df = summary_store, table_name = tables$summary, conn = conn, overwrite = FALSE, append = TRUE)
  }
  if (nrow(vulnerabilities_store) > 0L) {
    load_df_to_clickhouse(df = vulnerabilities_store, table_name = tables$vulnerabilities, conn = conn, overwrite = FALSE, append = TRUE)
  }
  if (nrow(lifecycle_store) > 0L) {
    load_df_to_clickhouse(df = lifecycle_store, table_name = tables$lifecycle, conn = conn, overwrite = FALSE, append = TRUE)
  }
  if (nrow(errors_store) > 0L) {
    load_df_to_clickhouse(df = errors_store, table_name = tables$errors, conn = conn, overwrite = FALSE, append = TRUE)
  }

  invisible(TRUE)
}

.gh_list_user_repos_fallback <- function(username, token = NULL) {
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

  if (length(repos) == 0L) {
    return(data.frame(full_name = character(0), stringsAsFactors = FALSE))
  }

  do.call(rbind, lapply(repos, function(r) {
    data.frame(full_name = .osv_null(r$full_name, NA_character_), stringsAsFactors = FALSE)
  }))
}

.resolve_repos_df <- function(username, token = NULL, conn = NULL) {
  if (exists("get_github_repos", mode = "function")) {
    etl_fn <- get("get_github_repos", mode = "function")
    etl_formals <- names(formals(etl_fn))
    supports_conn <- "conn" %in% etl_formals

    # ETL ClickHouse variant requires `conn`; use GitHub fallback when it is unavailable.
    if (!supports_conn || !is.null(conn)) {
      args <- list(username, token = token)
      if (supports_conn) args$conn <- conn

      repos_df <- tryCatch(
        do.call(etl_fn, args),
        error = function(e) {
          msg <- conditionMessage(e)
          if (grepl("404", msg, fixed = TRUE)) {
            stop(
              "GitHub user not found: '", username, "'. ",
              "Check the username/profile URL and try again.",
              call. = FALSE
            )
          }
          conn_required <- grepl("conn", msg, ignore.case = TRUE) &&
            (grepl("clickhouse", msg, ignore.case = TRUE) || grepl("connection", msg, ignore.case = TRUE))
          etl_glue_missing <- grepl(
            "table_exists_custom|clickhouse_request|load_df_to_clickhouse|could not find function",
            msg,
            ignore.case = TRUE
          )
          if (isTRUE(conn_required) || isTRUE(etl_glue_missing)) return(NULL)
          stop(msg, call. = FALSE)
        }
      )

      if (is.data.frame(repos_df)) {
        return(repos_df)
      }
    }
  }

  .gh_list_user_repos_fallback(username = username, token = token)
}

.gh_commit_detail_safe <- function(repo_full, sha, token = NULL) {
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
    "introduced_sha", "introduced_date", "introduced_message", "introduced_author", "introduced_dependency_files",
    "fixed_sha", "fixed_date", "fixed_message", "fixed_author", "fixed_dependency_files",
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
      introduced_author = .osv_null(state$introduced$author, NA_character_),
      introduced_dependency_files = .osv_null(state$introduced$dependency_files, NA_character_),
      fixed_sha = if (!is.null(close_snapshot)) .osv_null(close_snapshot$sha, NA_character_) else NA_character_,
      fixed_date = if (!is.null(close_snapshot)) .osv_null(close_snapshot$date, NA_character_) else NA_character_,
      fixed_message = if (!is.null(close_snapshot)) .osv_null(close_snapshot$message, NA_character_) else NA_character_,
      fixed_author = if (!is.null(close_snapshot)) .osv_null(close_snapshot$author, NA_character_) else NA_character_,
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
              author = .osv_null(snap$author, NA_character_),
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
    cluster = NULL
) {
  n <- length(commit_shas)
  if (n == 0L) return(list())

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
        .gh_commit_detail_safe(repo_full = repo_full, sha = sha, token = token)
      },
      repo_full = repo_full,
      token = token
    ))
  }

  if (!use_parallel) {
    return(lapply(commit_shas, function(sha) .gh_commit_detail_safe(repo_full = repo_full, sha = sha, token = token)))
  }

  w <- min(as.integer(workers), n)
  cl <- parallel::makeCluster(w)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  parallel::parLapply(
    cl = cl,
    X = as.list(commit_shas),
    fun = function(sha, repo_full, token) {
      .gh_commit_detail_safe(repo_full = repo_full, sha = sha, token = token)
    },
    repo_full = repo_full,
    token = token
  )
}

.build_repo_prefetch_plan <- function(
    repo_full,
    token = NULL,
    max_commits_per_repo = NULL,
    since_date = NULL,
    known_shas = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 1L,
    prefetch_cluster = NULL,
    debug = FALSE
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
    .gh_repo_commits(repo_full, token = token, max_commits = max_commits_per_repo, since = since_date),
    error = function(e) {
      add_err(NA_character_, "list_commits", conditionMessage(e))
      NULL
    }
  )
  .debug_log(debug, "prefetch", repo_full, ": fetched commits=", if (is.null(commits)) 0L else length(commits), ", since=", .osv_null(since_date, "none"))

  if (is.null(commits) || length(commits) == 0L) {
    return(list(
      repo_full = repo_full,
      commits = NULL,
      details = list(),
      errors = .rbind_data_frames(errors, template_cols = c("repository", "sha", "stage", "error"))
    ))
  }

  if (!is.null(known_shas) && length(known_shas) > 0L) {
    known <- unique(as.character(known_shas))
    before_n <- length(commits)
    keep <- vapply(commits, function(cmt) {
      sha <- .osv_to_scalar(cmt$sha)
      !(sha %in% known)
    }, logical(1))
    commits <- commits[keep]
    .debug_log(debug, "prefetch", repo_full, ": filtered known sha ", before_n - length(commits), "/", before_n)
  }

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
    cluster = prefetch_cluster
  )
  .debug_log(debug, "prefetch", repo_full, ": commit detail payloads=", length(details_raw))

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
  errors_df <- .rbind_data_frames(errors, template_cols = c("repository", "sha", "stage", "error"))
  .debug_log(debug, "prefetch", repo_full, ": details ready=", length(details), ", errors=", nrow(errors_df))

  list(
    repo_full = repo_full,
    commits = commits,
    details = details,
    errors = errors_df
  )
}

.analyze_repository_commits <- function(
    repo_full,
    token,
    max_commits_per_repo,
    since_date = NULL,
    known_shas = NULL,
    dependency_patterns,
    force_scan,
    temp_root,
    keep_snapshots,
    include_uncertain,
    syft_resolved,
    osv_db_obj,
    syft_timeout_sec = 0L,
    syft_excludes = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 1L,
    prefetched_commits = NULL,
    prefetched_details = NULL,
    prefetched_errors = NULL,
    debug = FALSE,
    debug_repo_every = 25L
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
      .gh_repo_commits(repo_full, token = token, max_commits = max_commits_per_repo, since = since_date),
      error = function(e) {
        append_error(NA_character_, "list_commits", conditionMessage(e))
        NULL
      }
    )
  }

  if (!is.null(commits) && length(commits) > 0L && !is.null(known_shas) && length(known_shas) > 0L) {
    known <- unique(as.character(known_shas))
    before_n <- length(commits)
    keep <- vapply(commits, function(cmt) {
      sha <- .osv_to_scalar(cmt$sha)
      !(sha %in% known)
    }, logical(1))
    commits <- commits[keep]
    .debug_log(debug, "repo", repo_full, ": filtered known sha ", before_n - length(commits), "/", before_n)
  }

  if (is.null(commits) || length(commits) == 0L) {
    .debug_log(debug, "repo", repo_full, ": no commits to process")
    return(list(
      summary = data.frame(),
      vulnerabilities = data.frame(),
      vulnerability_lifecycle = data.frame(),
      errors = .rbind_data_frames(error_rows)
    ))
  }

  total_commits <- length(commits)
  .debug_log(debug, "repo", repo_full, ": start commits=", total_commits, ", since=", .osv_null(since_date, "none"))
  debug_repo_every <- max(1L, as.integer(debug_repo_every))
  processed_n <- 0L
  scanned_n <- 0L
  skipped_n <- 0L
  failed_n <- 0L
  emit_progress <- function(force = FALSE) {
    if (!isTRUE(debug)) return(invisible(NULL))
    if (!isTRUE(force) && (processed_n %% debug_repo_every != 0L) && processed_n != total_commits) {
      return(invisible(NULL))
    }
    .debug_log(
      TRUE, "repo",
      repo_full, ": progress ", processed_n, "/", total_commits,
      " | scanned=", scanned_n, " skipped=", skipped_n, " failed=", failed_n
    )
    invisible(NULL)
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
      workers = as.integer(commit_info_workers)
    )
  }
  .debug_log(debug, "repo", repo_full, ": commit details loaded=", length(commit_details))

  for (commit_idx in seq_along(commits)) {
    cmt <- commits[[commit_idx]]
    sha <- .osv_to_scalar(cmt$sha)
    commit_date <- .osv_to_scalar(cmt$commit$author$date)
    commit_message <- .osv_to_scalar(cmt$commit$message)
    commit_author <- .osv_to_scalar(cmt$author$login)
    if (!.osv_non_empty(commit_author)) {
      commit_author <- .osv_to_scalar(cmt$commit$author$name)
    }

    detail_info <- if (commit_idx <= length(commit_details)) commit_details[[commit_idx]] else NULL
    if (!is.null(detail_info) && !is.null(detail_info$error)) {
      append_error(sha, "commit_detail", as.character(detail_info$error))
    }
    filenames <- NULL
    if (!is.null(detail_info) && !is.null(detail_info$filenames)) {
      filenames <- as.character(detail_info$filenames)
    } else {
      detail <- if (!is.null(detail_info)) detail_info$detail else NULL
      if (is.null(detail)) {
        failed_n <- failed_n + 1L
        processed_n <- processed_n + 1L
        emit_progress()
        next
      }

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
      skipped_n <- skipped_n + 1L
      summary_rows[[length(summary_rows) + 1L]] <- data.frame(
        repository = repo_full,
        sha = sha,
        date = commit_date,
        message = commit_message,
        commit_author = commit_author,
        scanned = FALSE,
        dependency_files = "",
        vulnerability_count = 0L,
          status = "skipped_no_dependency_changes",
          stringsAsFactors = FALSE
        )
      processed_n <- processed_n + 1L
      emit_progress()
      next
    }

    snapshot <- tryCatch(
      .download_repo_snapshot(repo_full, sha = sha, token = token, temp_root = temp_root),
      error = function(e) {
        append_error(sha, "download_snapshot", conditionMessage(e))
        NULL
      }
    )

    if (is.null(snapshot)) {
      failed_n <- failed_n + 1L
      summary_rows[[length(summary_rows) + 1L]] <- data.frame(
        repository = repo_full,
        sha = sha,
        date = commit_date,
        message = commit_message,
        commit_author = commit_author,
        scanned = FALSE,
        dependency_files = paste(dep_files, collapse = ";"),
        vulnerability_count = NA_integer_,
          status = "failed_snapshot_download",
          stringsAsFactors = FALSE
        )
      processed_n <- processed_n + 1L
      emit_progress()
      next
    }

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
    }
    if (!is.null(scan)) {
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
        author = commit_author,
        dependency_files = paste(dep_files, collapse = ";"),
        keys = unique(keys),
        key_rows = key_rows
      )
    }

    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      repository = repo_full,
      sha = sha,
      date = commit_date,
      message = commit_message,
      commit_author = commit_author,
      scanned = !is.null(scan),
      dependency_files = paste(dep_files, collapse = ";"),
      vulnerability_count = as.integer(vuln_count),
      status = if (is.null(scan)) "scan_failed" else "scanned",
      stringsAsFactors = FALSE
    )
    if (!is.null(scan)) {
      scanned_n <- scanned_n + 1L
    } else {
      failed_n <- failed_n + 1L
    }
    processed_n <- processed_n + 1L
    emit_progress()
  }
  emit_progress(force = TRUE)

  lifecycle_df <- .build_vulnerability_lifecycle(
    repo_full = repo_full,
    scan_snapshots = scan_snapshots
  )
  .debug_log(
    debug, "repo",
    repo_full, ": done summary=", length(summary_rows),
    ", vulnerabilities=", sum(vapply(vulnerability_rows, nrow, integer(1))),
    ", errors=", length(error_rows)
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
#' @param conn Optional ClickHouse connection object for ETL `get_github_repos()`.
#' @param clickhouse_incremental If `TRUE` (default), use ClickHouse cache for incremental analysis.
#' @param incremental_compare_commits_by_sha If `TRUE`, incremental mode refreshes commit list
#' and compares commit SHAs with cache (ignores `since` optimization) to scan only truly new commits.
#' @param clickhouse_cache_prefix Prefix for ClickHouse cache tables.
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
#' @param syft_timeout_sec Timeout for each syft run in seconds (`0` means no timeout).
#' @param syft_excludes Character vector of syft `--exclude` patterns.
#' @param commit_info_parallel If `TRUE`, fetch commit detail payloads in parallel per repository.
#' @param commit_info_workers Number of workers for commit-details prefetch.
#' @param debug If `TRUE`, print stage-by-stage debug messages.
#' @param debug_repo_every Progress print step (in commits) per repository when `debug = TRUE`.
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
    syft_timeout_sec = 0L,
    syft_excludes = NULL,
    commit_info_parallel = FALSE,
    commit_info_workers = 4L,
    debug = FALSE,
    debug_repo_every = 25L,
    conn = NULL,
    clickhouse_incremental = TRUE,
    incremental_compare_commits_by_sha = FALSE,
    clickhouse_cache_prefix = "github_commit_scan",
    ...
) {
  .osv_assert_scalar_character(profile, "profile")
  .osv_assert_scalar_character(syft_path, "syft_path")
  .osv_assert_scalar_logical(include_uncertain, "include_uncertain")
  .osv_assert_scalar_logical(keep_snapshots, "keep_snapshots")
  .osv_assert_scalar_logical(force_scan, "force_scan")
  parallel_strategy <- match.arg(parallel_strategy)
  .osv_assert_scalar_logical(parallel_repos, "parallel_repos")
  .osv_assert_scalar_numeric_ge(repo_workers, "repo_workers", min_value = 1)
  if (!is.null(auto_workers)) {
    .osv_assert_scalar_numeric_ge(auto_workers, "auto_workers", min_value = 1)
  }
  .osv_assert_scalar_numeric_ge(auto_min_repos_for_repo_parallel, "auto_min_repos_for_repo_parallel", min_value = 1)
  .osv_assert_scalar_numeric_ge(auto_min_commits_for_commit_parallel, "auto_min_commits_for_commit_parallel", min_value = 1)
  .osv_assert_scalar_numeric_ge(syft_timeout_sec, "syft_timeout_sec", min_value = 0)
  .osv_assert_scalar_logical(commit_info_parallel, "commit_info_parallel")
  .osv_assert_scalar_numeric_ge(commit_info_workers, "commit_info_workers", min_value = 1)
  .osv_assert_scalar_logical(debug, "debug")
  .osv_assert_scalar_numeric_ge(debug_repo_every, "debug_repo_every", min_value = 1)
  .osv_assert_scalar_logical(clickhouse_incremental, "clickhouse_incremental")
  .osv_assert_scalar_logical(incremental_compare_commits_by_sha, "incremental_compare_commits_by_sha")
  .osv_assert_scalar_character(clickhouse_cache_prefix, "clickhouse_cache_prefix")

  profile_trim <- trimws(profile)
  if (grepl("username_or_url|github_username", profile_trim, ignore.case = TRUE)) {
    stop(
      "`profile` looks like a placeholder value ('", profile_trim, "'). ",
      "Pass a real GitHub username (e.g. 'torvalds') or profile URL (e.g. 'https://github.com/torvalds').",
      call. = FALSE
    )
  }

  db_obj <- .osv_resolve_db(osv_db)
  if (!inherits(db_obj, "osv_mongo_database")) {
    stop("Only MongoDB OSV backend is supported. Use load_osv_mongo_database()/prepare_osv_database().", call. = FALSE)
  }
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
  .debug_log(debug, "init", "username=", username, ", parallel_strategy=", parallel_strategy)

  repos_df <- .resolve_repos_df(username = username, token = token, conn = conn)
  if (!is.data.frame(repos_df)) {
    stop("Repository listing returned non-tabular result.", call. = FALSE)
  }
  if (!("full_name" %in% names(repos_df))) {
    if ("name" %in% names(repos_df)) {
      repos_df$full_name <- paste0(username, "/", as.character(repos_df$name))
    } else {
      stop("Repository list must contain `full_name` (or `name`) column.", call. = FALSE)
    }
  }

  repos_df <- repos_df[!is.na(repos_df$full_name) & nzchar(repos_df$full_name), , drop = FALSE]
  if (!is.null(max_repos)) {
    repos_df <- head(repos_df, as.integer(max_repos))
  }
  .debug_log(debug, "repos", "selected repos=", nrow(repos_df))
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
  cache_enabled <- isTRUE(clickhouse_incremental) && .ch_cache_is_available(conn)
  .debug_log(debug, "cache", "enabled=", cache_enabled, ", prefix=", clickhouse_cache_prefix)
  cache_state <- if (isTRUE(cache_enabled)) {
    .ch_load_commit_scan_cache(
      conn = conn,
      profile = username,
      repos = repo_list,
      table_prefix = clickhouse_cache_prefix
    )
  } else {
    list(
      summary = data.frame(),
      vulnerabilities = data.frame(),
      lifecycle = data.frame(),
      errors = data.frame(),
      since_by_repo = list(),
      known_shas_by_repo = list()
    )
  }
  .debug_log(
    debug, "cache",
    "loaded summary=", nrow(.osv_null(cache_state$summary, data.frame())),
    ", vulns=", nrow(.osv_null(cache_state$vulnerabilities, data.frame())),
    ", errors=", nrow(.osv_null(cache_state$errors, data.frame()))
  )
  since_by_repo <- .osv_null(cache_state$since_by_repo, list())
  known_shas_by_repo <- .osv_null(cache_state$known_shas_by_repo, list())
  if (isTRUE(cache_enabled) && isTRUE(incremental_compare_commits_by_sha)) {
    since_by_repo <- list()
    .debug_log(
      debug, "cache",
      "incremental_compare_commits_by_sha=TRUE: since-filter disabled; new commits detected by SHA diff against cache."
    )
  } else if (isTRUE(incremental_compare_commits_by_sha) && !isTRUE(cache_enabled)) {
    .debug_log(
      debug, "cache",
      "incremental_compare_commits_by_sha=TRUE ignored because ClickHouse incremental cache is unavailable."
    )
  }

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
      .gitariki_log("[auto] package 'parallel' is unavailable; running sequentially.")
      .debug_log(debug, "auto", "parallel package unavailable -> sequential")
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
        .gitariki_log("[auto] selected repository-level parallelism: workers=", repo_workers_effective, ", repos=", repo_count)
        .debug_log(debug, "auto", "selected repo parallel workers=", repo_workers_effective, ", repos=", repo_count)
      } else {
        expected_commits <- if (!is.null(max_commits_per_repo)) as.integer(max_commits_per_repo) else NA_integer_
        commit_parallel_ok <- worker_budget >= 2L &&
          (is.na(expected_commits) || expected_commits >= as.integer(auto_min_commits_for_commit_parallel))

        parallel_repos_effective <- FALSE
        repo_workers_effective <- 1L
        commit_info_parallel_effective <- isTRUE(commit_parallel_ok)
        commit_info_workers_effective <- if (isTRUE(commit_parallel_ok)) min(worker_budget, 8L) else 1L

        if (isTRUE(commit_info_parallel_effective)) {
          .gitariki_log("[auto] selected commit-details parallel prefetch: workers=", commit_info_workers_effective, ".")
          .debug_log(debug, "auto", "selected commit-detail parallel workers=", commit_info_workers_effective)
        } else {
          .gitariki_log("[auto] selected sequential mode (insufficient workload/workers for safe speedup).")
          .debug_log(debug, "auto", "selected sequential path")
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

    .gitariki_log(
      "[staged_all] stage 1/2 prefetch: commit_parallel=", commit_info_parallel_effective,
      " (workers=", commit_info_workers_effective, "); ",
      "stage 2/2 scan: repo_parallel=", parallel_repos_effective,
      " (workers=", repo_workers_effective, ")."
    )
    .debug_log(
      debug, "staged_all",
      "prefetch_parallel=", commit_info_parallel_effective, " workers=", commit_info_workers_effective,
      "; repo_parallel=", parallel_repos_effective, " workers=", repo_workers_effective
    )

    repo_prefetch_plans <- vector("list", length(repo_list))
    names(repo_prefetch_plans) <- repo_list
    prefetch_cl <- NULL

    if (isTRUE(commit_info_parallel_effective) && as.integer(commit_info_workers_effective) > 1L) {
      prefetch_cl <- parallel::makeCluster(as.integer(commit_info_workers_effective))
      on.exit(parallel::stopCluster(prefetch_cl), add = TRUE)
      .gitariki_log("[staged_all] prefetch worker pool started: workers=", as.integer(commit_info_workers_effective))
    }

    for (repo_full in repo_list) {
      .gitariki_log("[staged_all][prefetch] ", repo_full)
      repo_prefetch_plans[[repo_full]] <- .build_repo_prefetch_plan(
        repo_full = repo_full,
        token = token,
        max_commits_per_repo = max_commits_per_repo,
        since_date = .osv_null(since_by_repo[[repo_full]], NULL),
        known_shas = .osv_null(known_shas_by_repo[[repo_full]], character(0)),
        commit_info_parallel = commit_info_parallel_effective,
        commit_info_workers = commit_info_workers_effective,
        prefetch_cluster = prefetch_cl,
        debug = debug
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
    .debug_log(debug, "scan", "repository parallel mode workers=", workers, ", repos=", length(repo_list))
    .debug_log(debug, "scan", "note: per-repo worker messages may be buffered in parallel mode")
    worker_mongo_cfg <- list(
      mongo_url = db_obj$mongo_url,
      db_name = db_obj$db_name,
      collection = .osv_null(db_obj$collection, "vulns")
    )
    required_fns <- c(
      # repository analysis
      ".analyze_repository_commits",
      ".prefetch_commit_details",
      ".gh_require",
      ".gh_repo_commits",
      ".gh_commit_detail_safe",
      ".download_repo_snapshot",
      "commit_has_dependency_changes",
      ".debug_log",
      ".gitariki_log",
      ".osv_assert_scalar_character",
      ".osv_assert_scalar_logical",
      ".osv_assert_scalar_numeric_ge",
      ".osv_to_scalar",
      ".osv_null",
      ".osv_non_empty",
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
      "load_osv_mongo_database",
      "search_osv_vulnerabilities_mongo",
      ".osv_get_mongo_connection",
      ".osv_collapse_values",
      ".osv_reference_urls",
      ".osv_match_to_df",
      # osv search helpers
      ".osv_resolve_db",
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

    fn_export_env <- new.env(parent = emptyenv())
    for (nm in required_fns) {
      assign(nm, get(nm, mode = "function", inherits = TRUE), envir = fn_export_env)
    }

    required_objects <- character(0)
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

    obj_export_env <- new.env(parent = emptyenv())
    for (nm in required_objects) {
      assign(nm, get(nm, inherits = TRUE), envir = obj_export_env)
    }

    cl <- parallel::makeCluster(workers)
    on.exit(parallel::stopCluster(cl), add = TRUE)

    parallel::clusterExport(cl, varlist = required_fns, envir = fn_export_env)
    if (length(required_objects) > 0L) {
      parallel::clusterExport(cl, varlist = required_objects, envir = obj_export_env)
    }
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

    repo_tasks <- lapply(repo_list, function(repo_full) {
      list(
        repo_full = repo_full,
        plan = if (isTRUE(staged_all_mode)) repo_prefetch_plans[[repo_full]] else NULL,
        since_date = .osv_null(since_by_repo[[repo_full]], NULL),
        known_shas = .osv_null(known_shas_by_repo[[repo_full]], character(0))
      )
    })

    repo_results <- parallel::parLapply(
      cl = cl,
      X = repo_tasks,
      fun = function(task, token, max_commits_per_repo, dependency_patterns, force_scan, temp_root,
                     keep_snapshots, include_uncertain, syft_resolved, syft_timeout_sec, syft_excludes,
                     staged_all_mode, debug, debug_repo_every) {
        repo_full <- task$repo_full
        plan <- task$plan
        since_date <- task$since_date
        known_shas <- task$known_shas
        tryCatch(
          .analyze_repository_commits(
            repo_full = repo_full,
            token = token,
            max_commits_per_repo = max_commits_per_repo,
            since_date = since_date,
            known_shas = known_shas,
            dependency_patterns = dependency_patterns,
            force_scan = force_scan,
            temp_root = temp_root,
            keep_snapshots = keep_snapshots,
            include_uncertain = include_uncertain,
            syft_resolved = syft_resolved,
            osv_db_obj = .gitariki_worker_osv_db,
            syft_timeout_sec = syft_timeout_sec,
            syft_excludes = syft_excludes,
            commit_info_parallel = FALSE,
            commit_info_workers = 1L,
            prefetched_commits = if (!is.null(plan)) plan$commits else NULL,
            prefetched_details = if (!is.null(plan)) plan$details else NULL,
            prefetched_errors = if (!is.null(plan)) plan$errors else NULL,
            debug = debug,
            debug_repo_every = debug_repo_every
          ),
          error = function(e) {
            list(
              summary = data.frame(
                repository = repo_full,
                sha = NA_character_,
                date = NA_character_,
                message = NA_character_,
                commit_author = NA_character_,
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
      syft_timeout_sec = as.integer(syft_timeout_sec),
      syft_excludes = syft_excludes,
      staged_all_mode = staged_all_mode,
      debug = debug,
      debug_repo_every = as.integer(debug_repo_every)
    )
    .debug_log(debug, "scan", "parallel repository stage finished")
  } else {
    if (isTRUE(parallel_repos_effective) && as.integer(repo_workers_effective) > 1L && !requireNamespace("parallel", quietly = TRUE)) {
      .gitariki_log("Package 'parallel' is unavailable; falling back to sequential repository processing.")
      .debug_log(debug, "scan", "fallback to sequential repository processing")
    }
    for (repo_full in repo_list) {
      .debug_log(debug, "scan", "processing repo=", repo_full)
      repo_results[[length(repo_results) + 1L]] <- .analyze_repository_commits(
        repo_full = repo_full,
        token = token,
        max_commits_per_repo = max_commits_per_repo,
        since_date = .osv_null(since_by_repo[[repo_full]], NULL),
        known_shas = .osv_null(known_shas_by_repo[[repo_full]], character(0)),
        dependency_patterns = dependency_patterns,
        force_scan = force_scan,
        temp_root = temp_root,
        keep_snapshots = keep_snapshots,
        include_uncertain = include_uncertain,
        syft_resolved = syft_resolved,
        osv_db_obj = db_obj,
        syft_timeout_sec = syft_timeout_sec,
        syft_excludes = syft_excludes,
        commit_info_parallel = if (isTRUE(staged_all_mode)) FALSE else commit_info_parallel_effective,
        commit_info_workers = if (isTRUE(staged_all_mode)) 1L else as.integer(commit_info_workers_effective),
        prefetched_commits = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$commits, NULL) else NULL,
        prefetched_details = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$details, NULL) else NULL,
        prefetched_errors = if (isTRUE(staged_all_mode)) .osv_null(repo_prefetch_plans[[repo_full]]$errors, NULL) else NULL,
        debug = debug,
        debug_repo_every = debug_repo_every
      )
    }
  }

  summary_template <- c("repository", "sha", "date", "message", "commit_author", "scanned", "dependency_files", "vulnerability_count", "status")
  vulnerabilities_template <- c(
    "osv_id", "summary", "details", "published", "modified", "aliases", "references",
    "query_purl", "query_version", "matched_purl", "matched_package", "matched_ecosystem",
    "affected", "match_reason", "source_json",
    "component_bom_ref", "component_type", "component_name", "component_version", "component_purl",
    "repository", "sha", "commit_date", "commit_message", "dependency_files"
  )
  lifecycle_template <- c(
    "repository", "vulnerability_key", "osv_id",
    "query_purl", "matched_purl", "matched_package", "matched_ecosystem",
    "component_name", "component_purl",
    "introduced_sha", "introduced_date", "introduced_message", "introduced_author", "introduced_dependency_files",
    "fixed_sha", "fixed_date", "fixed_message", "fixed_author", "fixed_dependency_files",
    "status"
  )
  errors_template <- c("repository", "sha", "stage", "error")

  summary_new <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$summary, data.frame())),
    template_cols = summary_template
  )
  vulnerabilities_new <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$vulnerabilities, data.frame())),
    template_cols = vulnerabilities_template
  )
  errors_new <- .rbind_data_frames(
    lapply(repo_results, function(x) .osv_null(x$errors, data.frame())),
    template_cols = errors_template
  )

  summary_df <- .merge_commit_scan_frames(
    cached_df = cache_state$summary,
    new_df = summary_new,
    template_cols = summary_template,
    key_cols = c("repository", "sha")
  )
  vulnerabilities_df <- .merge_commit_scan_frames(
    cached_df = cache_state$vulnerabilities,
    new_df = vulnerabilities_new,
    template_cols = vulnerabilities_template,
    key_cols = c("repository", "sha", "osv_id", "component_purl", "query_purl", "matched_purl")
  )
  errors_df <- .merge_commit_scan_frames(
    cached_df = cache_state$errors,
    new_df = errors_new,
    template_cols = errors_template,
    key_cols = c("repository", "sha", "stage", "error")
  )
  lifecycle_df <- .rebuild_lifecycle_from_summary_and_vulnerabilities(
    summary_df = summary_df,
    vulnerabilities_df = vulnerabilities_df
  )
  lifecycle_df <- .rbind_data_frames(list(lifecycle_df), template_cols = lifecycle_template)
  enriched <- .enrich_lifecycle_authors(
    summary_df = summary_df,
    lifecycle_df = lifecycle_df,
    token = token,
    debug = debug
  )
  summary_df <- .osv_null(enriched$summary, summary_df)
  lifecycle_df <- .osv_null(enriched$lifecycle, lifecycle_df)
  .debug_log(
    debug, "merge",
    "summary_new=", nrow(summary_new), ", summary_total=", nrow(summary_df),
    "; vulns_new=", nrow(vulnerabilities_new), ", vulns_total=", nrow(vulnerabilities_df),
    "; lifecycle_total=", nrow(lifecycle_df)
  )

  if (isTRUE(cache_enabled)) {
    .ch_save_commit_scan_cache(
      conn = conn,
      profile = username,
      summary_df = summary_df,
      vulnerabilities_df = vulnerabilities_df,
      lifecycle_df = lifecycle_df,
      errors_df = errors_df,
      table_prefix = clickhouse_cache_prefix
    )
    .debug_log(debug, "cache", "cache updated in ClickHouse")
  }

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

