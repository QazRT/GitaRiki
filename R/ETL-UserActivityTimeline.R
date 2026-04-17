get_github_user_activity_timeline <- function(
    profile,
    token = NULL,
    conn = NULL,
    start_at = Sys.time() - 365 * 24 * 60 * 60,
    end_at = Sys.time(),
    step = "day",
    activity_types = c("all"),
    include_zero_points = TRUE,
    include_thread_engagement = TRUE,
    include_event_activity = TRUE,
    include_search_activity = TRUE,
    events_limit = 300,
    search_limit_commits = 1000,
    search_limit_comments = 1000,
    search_limit_items = 1000,
    table_points = "github_user_activity_timeline_points",
    table_raw = "github_user_activity_timeline_raw",
    save_to_clickhouse = TRUE,
    use_clickhouse_cache = TRUE,
    cache_max_age_hours = 12,
    cache_only = FALSE,
    profile_cache_table = "github_user_profile_native_stats",
    skip_clickhouse_write_on_cache_hit = TRUE
) {
  required_packages <- c("gh", "dplyr", "purrr", "httr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
  }
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)

  `%||%` <- function(x, y) if (is.null(x)) y else x
  .now_utc <- function() as.POSIXct(Sys.time(), tz = "UTC")
  run_t0 <- .now_utc()

  source_if_missing <- function(fn_name, candidates) {
    if (exists(fn_name, mode = "function")) {
      return(invisible(TRUE))
    }

    resolved <- NULL
    for (candidate in candidates) {
      if (file.exists(candidate)) {
        resolved <- candidate
        break
      }
      candidate_from_wd <- file.path(getwd(), candidate)
      if (file.exists(candidate_from_wd)) {
        resolved <- candidate_from_wd
        break
      }
      candidate_from_r <- file.path("R", candidate)
      if (file.exists(candidate_from_r)) {
        resolved <- candidate_from_r
        break
      }
    }

    if (is.null(resolved)) {
      return(invisible(FALSE))
    }

    source(resolved, local = FALSE)
    exists(fn_name, mode = "function")
  }

  normalize_step <- function(x) {
    x <- tolower(trimws(as.character(x)))
    if (x %in% c("hour", "day", "week", "month")) {
      return(list(kind = x, seconds = NA_real_, seq_by = x))
    }

    m <- regexec("^([0-9]+)\\s*(min|mins|minute|minutes|hour|hours|day|days)$", x)
    reg <- regmatches(x, m)[[1]]
    if (length(reg) == 3) {
      n <- as.integer(reg[2])
      unit <- reg[3]
      mult <- switch(
        unit,
        "min" = 60,
        "mins" = 60,
        "minute" = 60,
        "minutes" = 60,
        "hour" = 3600,
        "hours" = 3600,
        "day" = 86400,
        "days" = 86400,
        NA_real_
      )
      if (!is.na(mult)) {
        return(list(kind = "fixed", seconds = n * mult, seq_by = paste(n, if (mult == 60) "mins" else if (mult == 3600) "hours" else "days")))
      }
    }

    stop("Unsupported 'step'. Use 'hour', 'day', 'week', 'month' or interval like '6 hours', '30 min'.")
  }

  parse_time <- function(x, label) {
    if (inherits(x, "POSIXt")) {
      out <- as.POSIXct(x, tz = "UTC")
    } else {
      out <- as.POSIXct(x, tz = "UTC")
    }
    if (is.na(out)) {
      stop("Could not parse ", label, ". Use POSIXct or 'YYYY-MM-DD HH:MM:SS'.")
    }
    out
  }

  floor_time_custom <- function(x, step_cfg) {
    x <- as.POSIXct(x, tz = "UTC")

    if (step_cfg$kind == "fixed") {
      return(as.POSIXct(floor(as.numeric(x) / step_cfg$seconds) * step_cfg$seconds, origin = "1970-01-01", tz = "UTC"))
    }

    if (step_cfg$kind == "hour") {
      return(as.POSIXct(format(x, "%Y-%m-%d %H:00:00"), tz = "UTC"))
    }

    if (step_cfg$kind == "day") {
      return(as.POSIXct(paste0(as.Date(x, tz = "UTC"), " 00:00:00"), tz = "UTC"))
    }

    if (step_cfg$kind == "week") {
      d <- as.Date(x, tz = "UTC")
      wday <- as.POSIXlt(d, tz = "UTC")$wday
      monday <- d - ((wday + 6) %% 7)
      return(as.POSIXct(paste0(monday, " 00:00:00"), tz = "UTC"))
    }

    if (step_cfg$kind == "month") {
      return(as.POSIXct(format(x, "%Y-%m-01 00:00:00"), tz = "UTC"))
    }

    stop("Unsupported step kind.")
  }

  build_time_sequence <- function(from_time, to_time, step_cfg) {
    from_bucket <- floor_time_custom(from_time, step_cfg)
    to_bucket <- floor_time_custom(to_time, step_cfg)

    if (step_cfg$kind == "fixed") {
      by_sec <- step_cfg$seconds
      return(seq(from_bucket, to_bucket, by = by_sec))
    }

    by_value <- switch(
      step_cfg$kind,
      "hour" = "hour",
      "day" = "day",
      "week" = "week",
      "month" = "month"
    )

    seq(from_bucket, to_bucket, by = by_value)
  }

  extract_username <- function(input) {
    if (exists("extract_github_username", mode = "function")) {
      return(extract_github_username(input))
    }
    if (grepl("^[a-zA-Z0-9_-]+$", input)) {
      return(input)
    }
    parsed <- httr::parse_url(input)
    if (!is.null(parsed$path)) {
      parts <- strsplit(gsub("^/|/$", "", parsed$path), "/")[[1]]
      if (length(parts) >= 1) {
        return(parts[1])
      }
    }
    NULL
  }

  format_github_time <- function(x) {
    format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  }

  normalize_event_activity_type <- function(event_type, action = NULL) {
    action <- tolower(action %||% "")
    out <- switch(
      event_type %||% "",
      "PushEvent" = "push_event",
      "IssueCommentEvent" = "issue_comment_event",
      "PullRequestReviewCommentEvent" = "pr_review_comment_event",
      "CommitCommentEvent" = "commit_comment_event",
      "PullRequestReviewEvent" = "pr_review_event",
      "WatchEvent" = "watch_event",
      "ForkEvent" = "fork_event",
      "CreateEvent" = "create_event",
      "DeleteEvent" = "delete_event",
      "ReleaseEvent" = "release_event",
      "PublicEvent" = "public_event",
      "MemberEvent" = "member_event",
      "GollumEvent" = "wiki_event",
      "IssuesEvent" = if (action == "opened") "issue_opened_event" else "issues_event",
      "PullRequestEvent" = if (action == "opened") "pr_opened_event" else "pull_request_event",
      tolower(gsub("([a-z])([A-Z])", "\\1_\\2", gsub("Event$", "", event_type %||% "")))
    )
    tolower(out)
  }

  fetch_search_items <- function(endpoint, query, limit, token, sort_by = NULL, order = "desc", accept = NULL) {
    args <- list(
      endpoint,
      q = query,
      .limit = as.integer(limit),
      .token = token,
      .progress = FALSE
    )
    if (!is.null(sort_by)) {
      args$sort <- sort_by
    }
    if (!is.null(order)) {
      args$order <- order
    }
    if (!is.null(accept)) {
      args$.accept <- accept
    }

    result <- tryCatch({
      do.call(gh::gh, args)
    }, error = function(e) {
      warning("Failed to execute search query '", query, "': ", e$message)
      NULL
    })

    if (is.null(result) || is.null(result$items) || length(result$items) == 0) {
      list(total_count = 0L, items = list())
    } else {
      list(
        total_count = as.integer(result$total_count %||% length(result$items)),
        items = result$items
      )
    }
  }

  ensure_clickhouse_delete <- function(conn, table_name, username, period_start, period_end, step_value) {
    if (!exists("table_exists_custom", mode = "function") ||
        !exists("clickhouse_request", mode = "function")) {
      return(invisible(FALSE))
    }

    if (!isTRUE(table_exists_custom(conn, table_name))) {
      return(invisible(FALSE))
    }

    esc <- function(x) gsub("'", "''", x)

    sql <- paste0(
      "ALTER TABLE ", table_name,
      " DELETE WHERE profile = '", esc(username), "'",
      " AND period_start = '", esc(format(period_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND period_end = '", esc(format(period_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND step = '", esc(step_value), "'"
    )

    tryCatch(
      clickhouse_request(conn, sql, parse_json = FALSE),
      error = function(e) warning("Failed to delete previous rows from ", table_name, ": ", e$message)
    )
    invisible(TRUE)
  }

  read_cached_raw_activity <- function(conn, table_name, username, period_start, period_end, step_value) {
    if (is.null(conn) ||
        !exists("query_clickhouse", mode = "function") ||
        !exists("table_exists_custom", mode = "function")) {
      return(data.frame())
    }

    if (!isTRUE(table_exists_custom(conn, table_name))) {
      return(data.frame())
    }

    esc <- function(x) gsub("'", "''", x)
    sql_main <- paste0(
      "SELECT activity_time, activity_type, activity_value, repository, source, source_id, ",
      "generated_at, period_start, period_end, step FROM ", table_name,
      " WHERE profile = '", esc(username), "'",
      " AND step = '", esc(step_value), "'",
      " AND activity_time >= '", esc(format(period_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND activity_time <= '", esc(format(period_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'"
    )
    sql_fallback <- paste0(
      "SELECT activity_time, activity_type, activity_value, repository, source, source_id, generated_at ",
      "FROM ", table_name,
      " WHERE profile = '", esc(username), "'",
      " AND activity_time >= '", esc(format(period_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND activity_time <= '", esc(format(period_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'"
    )

    out <- tryCatch(
      query_clickhouse(conn, sql_main),
      error = function(e_main) {
        tryCatch(
          query_clickhouse(conn, sql_fallback),
          error = function(e_fallback) {
            warning("Failed to load cached raw activity from ClickHouse: ", e_fallback$message)
            data.frame()
          }
        )
      }
    )

    if (!is.data.frame(out) || nrow(out) == 0) {
      return(data.frame())
    }

    if (!("generated_at" %in% names(out))) {
      out$generated_at <- as.POSIXct(NA, tz = "UTC")
    }
    if (!("period_start" %in% names(out))) {
      out$period_start <- as.POSIXct(NA, tz = "UTC")
    }
    if (!("period_end" %in% names(out))) {
      out$period_end <- as.POSIXct(NA, tz = "UTC")
    }
    if (!("step" %in% names(out))) {
      out$step <- NA_character_
    }

    out
  }

  read_cached_profile_native <- function(conn, table_name, username) {
    if (is.null(conn) ||
        !exists("query_clickhouse", mode = "function") ||
        !exists("table_exists_custom", mode = "function")) {
      return(data.frame())
    }

    if (!isTRUE(table_exists_custom(conn, table_name))) {
      return(data.frame())
    }

    esc <- function(x) gsub("'", "''", x)
    sql <- paste0(
      "SELECT * FROM ", table_name,
      " WHERE login = '", esc(username), "'",
      " ORDER BY fetched_at DESC LIMIT 1"
    )

    out <- tryCatch(
      query_clickhouse(conn, sql),
      error = function(e) {
        warning("Failed to load cached profile from ClickHouse: ", e$message)
        data.frame()
      }
    )

    if (!is.data.frame(out)) {
      return(data.frame())
    }

    out
  }

  read_cached_points <- function(conn, table_name, username, period_start, period_end, step_value, requested_types_input) {
    if (is.null(conn) ||
        !exists("query_clickhouse", mode = "function") ||
        !exists("table_exists_custom", mode = "function")) {
      return(data.frame())
    }

    if (!isTRUE(table_exists_custom(conn, table_name))) {
      return(data.frame())
    }

    esc <- function(x) gsub("'", "''", x)
    has_explicit_types <- length(requested_types_input) > 0 && !any(requested_types_input == "all")
    type_filter <- ""
    if (has_explicit_types) {
      type_values <- paste0("'", vapply(requested_types_input, esc, character(1)), "'", collapse = ",")
      type_filter <- paste0(" AND lower(activity_type) IN (", type_values, ")")
    }

    sql_main <- paste0(
      "SELECT * FROM ", table_name,
      " WHERE profile = '", esc(username), "'",
      " AND step = '", esc(step_value), "'",
      " AND period_start = '", esc(format(period_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND period_end = '", esc(format(period_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      type_filter
    )

    sql_fallback <- paste0(
      "SELECT * FROM ", table_name,
      " WHERE profile = '", esc(username), "'",
      " AND step = '", esc(step_value), "'",
      " AND bucket_time >= '", esc(format(period_start, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      " AND bucket_time <= '", esc(format(period_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'",
      type_filter
    )

    out <- tryCatch(
      query_clickhouse(conn, sql_main),
      error = function(e_main) {
        tryCatch(
          query_clickhouse(conn, sql_fallback),
          error = function(e_fallback) {
            warning("Failed to load cached points from ClickHouse: ", e_fallback$message)
            data.frame()
          }
        )
      }
    )

    if (!is.data.frame(out) || nrow(out) == 0) {
      return(data.frame())
    }

    out
  }

  # Try to load reusable project modules if they are not already in environment.
  source_if_missing("load_df_to_clickhouse", c("R/CC.r", "CC.r"))
  source_if_missing("query_clickhouse", c("R/CC.r", "CC.r"))
  source_if_missing("table_exists_custom", c("R/CC.r", "CC.r"))
  source_if_missing("get_github_repos", c("R/ETL-Repos.R", "ETL-Repos.R"))
  source_if_missing("get_github_user_stats", c("R/ETL-UserStats.R", "ETL-UserStats.R"))
  source_if_missing("get_github_user_comments", c("R/ETL-UserComments.R", "ETL-UserComments.R"))
  source_if_missing("get_github_user_profile_native_stats", c("R/ETL-UserProfileNativeStats.R", "ETL-UserProfileNativeStats.R"))

  if (isTRUE(save_to_clickhouse) && is.null(conn)) {
    stop("Parameter 'conn' is required when save_to_clickhouse = TRUE.")
  }
  if (isTRUE(cache_only) && is.null(conn)) {
    stop("Parameter 'conn' is required when cache_only = TRUE.")
  }

  start_time <- parse_time(start_at, "start_at")
  end_time <- parse_time(end_at, "end_at")
  start_time <- as.POSIXct(format(start_time, "%Y-%m-%d %H:%M:%S", tz = "UTC"), tz = "UTC")
  end_time <- as.POSIXct(format(end_time, "%Y-%m-%d %H:%M:%S", tz = "UTC"), tz = "UTC")
  if (start_time > end_time) {
    stop("'start_at' must be earlier than or equal to 'end_at'.")
  }

  step_cfg <- normalize_step(step)

  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Could not extract GitHub username from: ", profile)
  }

  period_qualifier <- paste0(
    format_github_time(start_time),
    "..",
    format_github_time(end_time)
  )
  requested_types_input <- unique(tolower(as.character(activity_types)))
  if (length(requested_types_input) == 0) {
    requested_types_input <- c("all")
  }

  raw_parts <- list()
  diagnostics <- list()
  oldest_public_event_time <- as.POSIXct(NA, tz = "UTC")
  comments_result <- list(thread_comments = data.frame(), event_comments = data.frame(), summary = data.frame())
  user_stats_result <- list(events = data.frame(), events_by_type = data.frame())
  live_api_used <- FALSE

  # ClickHouse cache bootstrap -------------------------------------------------
  cache_enabled <- isTRUE(use_clickhouse_cache) && !is.null(conn)
  cache_read_t0 <- .now_utc()
  cached_points <- data.frame()
  cached_raw <- data.frame()
  cache_age_hours <- Inf
  cache_is_fresh <- FALSE
  cached_sources <- character(0)

  if (cache_enabled) {
    cached_points <- read_cached_points(
      conn = conn,
      table_name = table_points,
      username = username,
      period_start = start_time,
      period_end = end_time,
      step_value = as.character(step),
      requested_types_input = requested_types_input
    )
    if (nrow(cached_points) > 0 && "generated_at" %in% names(cached_points)) {
      cached_points$generated_at <- as.POSIXct(cached_points$generated_at, tz = "UTC")
      if (any(!is.na(cached_points$generated_at))) {
        cache_age_hours <- as.numeric(difftime(Sys.time(), max(cached_points$generated_at, na.rm = TRUE), units = "hours"))
        cache_is_fresh <- is.finite(cache_age_hours) && !is.na(cache_age_hours) && cache_age_hours <= cache_max_age_hours
      }
    }

    points_types_ok <- TRUE
    if (!(length(requested_types_input) == 0 || any(requested_types_input == "all"))) {
      points_types_ok <- all(requested_types_input %in% unique(tolower(cached_points$activity_type)))
    }
    cache_points_hit <- nrow(cached_points) > 0 && (isTRUE(cache_only) || (cache_is_fresh && points_types_ok))
    diagnostics$cache_points_rows_loaded <- nrow(cached_points)
    diagnostics$cache_points_hit <- cache_points_hit

    if (cache_points_hit) {
      cached_points <- cached_points %>%
        mutate(bucket_time = as.POSIXct(bucket_time, tz = "UTC")) %>%
        arrange(bucket_time, activity_type)

      diagnostics$cache_enabled <- cache_enabled
      diagnostics$cache_rows_loaded <- 0L
      diagnostics$cache_sources <- character(0)
      diagnostics$cache_age_hours <- if (is.finite(cache_age_hours)) cache_age_hours else NA_real_
      diagnostics$cache_is_fresh <- cache_is_fresh
      diagnostics$cache_max_age_hours <- cache_max_age_hours
      diagnostics$cache_used_for_points <- TRUE
      diagnostics$live_api_used <- FALSE
      diagnostics$cache_hit_only <- TRUE
      diagnostics$clickhouse_write_skipped <- TRUE
      diagnostics$timings_sec <- list(
        cache_read = as.numeric(difftime(.now_utc(), cache_read_t0, units = "secs")),
        total = as.numeric(difftime(.now_utc(), run_t0, units = "secs"))
      )

      return(list(
        points = cached_points,
        raw_activity = data.frame(),
        diagnostics = diagnostics
      ))
    }

    cached_raw <- read_cached_raw_activity(conn, table_raw, username, start_time, end_time, as.character(step))
    if (nrow(cached_raw) > 0) {
      cached_raw <- cached_raw %>%
        mutate(
          activity_time = as.POSIXct(activity_time, tz = "UTC"),
          generated_at = as.POSIXct(generated_at, tz = "UTC"),
          period_start = as.POSIXct(period_start, tz = "UTC"),
          period_end = as.POSIXct(period_end, tz = "UTC")
        ) %>%
        filter(!is.na(activity_time))

      cached_sources <- unique(cached_raw$source[!is.na(cached_raw$source) & nzchar(cached_raw$source)])

      if (any(!is.na(cached_raw$generated_at)) && !is.finite(cache_age_hours)) {
        cache_age_hours <- as.numeric(difftime(Sys.time(), max(cached_raw$generated_at, na.rm = TRUE), units = "hours"))
        cache_is_fresh <- is.finite(cache_age_hours) && !is.na(cache_age_hours) && cache_age_hours <= cache_max_age_hours
      }

      if (isTRUE(cache_only) || cache_is_fresh) {
        cached_raw_selected <- cached_raw %>%
          select(activity_time, activity_type, activity_value, repository, source, source_id)
        raw_parts <- append(raw_parts, list(cached_raw_selected))
      }
    }
  }

  diagnostics$cache_enabled <- cache_enabled
  diagnostics$cache_rows_loaded <- nrow(cached_raw)
  diagnostics$cache_sources <- sort(unique(cached_sources))
  diagnostics$cache_age_hours <- if (is.finite(cache_age_hours)) cache_age_hours else NA_real_
  diagnostics$cache_is_fresh <- cache_is_fresh
  diagnostics$cache_max_age_hours <- cache_max_age_hours
  diagnostics$cache_used_for_points <- FALSE
  cache_read_sec <- as.numeric(difftime(.now_utc(), cache_read_t0, units = "secs"))
  collection_sec <- NA_real_
  aggregation_sec <- NA_real_
  write_sec <- 0

  if (isTRUE(cache_only) && nrow(cached_raw) == 0) {
    stop("cache_only = TRUE, but no cached rows were found in ClickHouse for the selected period.")
  }

  source_is_covered <- function(src_values) {
    if (nrow(cached_raw) == 0) {
      return(FALSE)
    }
    rows <- cached_raw %>% filter(source %in% src_values)
    if (nrow(rows) == 0) {
      return(FALSE)
    }
    if (!("period_start" %in% names(rows)) || !("period_end" %in% names(rows))) {
      return(FALSE)
    }
    if (all(is.na(rows$period_start)) || all(is.na(rows$period_end))) {
      min_time <- suppressWarnings(min(rows$activity_time, na.rm = TRUE))
      max_time <- suppressWarnings(max(rows$activity_time, na.rm = TRUE))
      if (!is.finite(as.numeric(min_time)) || !is.finite(as.numeric(max_time))) {
        return(FALSE)
      }
      return(min_time <= start_time && max_time >= end_time)
    }
    any(rows$period_start <= start_time & rows$period_end >= end_time, na.rm = TRUE)
  }

  skip_live_calls <- isTRUE(cache_only)
  skip_public_events_api <- skip_live_calls || (cache_is_fresh && source_is_covered("public_events"))
  skip_commit_search_api <- skip_live_calls || (cache_is_fresh && source_is_covered("search_commits"))
  skip_issues_search_api <- skip_live_calls || (cache_is_fresh && source_is_covered("search_issues_opened"))
  skip_prs_search_api <- skip_live_calls || (cache_is_fresh && source_is_covered("search_prs_opened"))
  skip_comments_events_module <- skip_live_calls || (cache_is_fresh && source_is_covered("module_user_comments_events"))
  skip_comments_threads_module <- skip_live_calls || (cache_is_fresh && source_is_covered("module_user_comments_threads"))

  if (skip_public_events_api && nrow(cached_raw) > 0) {
    cached_public_times <- as.POSIXct(
      cached_raw$activity_time[cached_raw$source %in% c("public_events", "public_events_payload")],
      tz = "UTC"
    )
    if (length(cached_public_times) > 0 && any(!is.na(cached_public_times))) {
      oldest_public_event_time <- min(cached_public_times, na.rm = TRUE)
    }
  }

  # Reuse profile statistics from ClickHouse first, then fallback to API module.
  profile_native <- data.frame(login = username, stringsAsFactors = FALSE)
  if (cache_enabled) {
    profile_cached <- read_cached_profile_native(conn, profile_cache_table, username)
    if (nrow(profile_cached) > 0) {
      profile_native <- profile_cached
      diagnostics$profile_enrichment_source <- "clickhouse_cache"
    }
  }
  if (!isTRUE(diagnostics$profile_enrichment_source == "clickhouse_cache") && !isTRUE(cache_only)) {
    profile_native <- tryCatch({
      if (exists("get_github_user_profile_native_stats", mode = "function")) {
        get_github_user_profile_native_stats(profile = username, token = token, conn = NULL)$profile
      } else {
        data.frame(login = username, stringsAsFactors = FALSE)
      }
    }, error = function(e) {
      warning("Failed to load native profile stats: ", e$message)
      data.frame(login = username, stringsAsFactors = FALSE)
    })
    diagnostics$profile_enrichment_source <- "github_api"
  } else if (!isTRUE(diagnostics$profile_enrichment_source == "clickhouse_cache") && isTRUE(cache_only)) {
    diagnostics$profile_enrichment_source <- "cache_only_no_profile_cache"
  }

  collection_t0 <- .now_utc()
  # 1) Public events (broad activity types, plus push payload size).
  if (isTRUE(include_event_activity) && !skip_public_events_api) {
    live_api_used <- TRUE
    diagnostics$public_events_source <- "github_api"
    public_events <- tryCatch({
      gh::gh(
        "GET /users/{username}/events/public",
        username = username,
        .limit = min(as.integer(events_limit), 300L),
        .token = token,
        .progress = TRUE
      )
    }, error = function(e) {
      warning("Failed to load public events: ", e$message)
      list()
    })

    diagnostics$public_events_loaded <- length(public_events)
    diagnostics$public_events_window_limited <- length(public_events) >= min(as.integer(events_limit), 300L)

    if (length(public_events) > 0) {
      public_event_times <- as.POSIXct(
        vapply(public_events, function(ev) ev$created_at %||% NA_character_, character(1)),
        tz = "UTC"
      )
      if (any(!is.na(public_event_times))) {
        oldest_public_event_time <- min(public_event_times, na.rm = TRUE)
      }

      events_df <- bind_rows(lapply(public_events, function(ev) {
        payload <- ev$payload %||% list()
        action <- payload$action %||% NA_character_
        created <- ev$created_at %||% NA_character_
        data.frame(
          activity_time = created,
          activity_type = normalize_event_activity_type(ev$type %||% NA_character_, action),
          activity_value = 1,
          repository = ev$repo$name %||% NA_character_,
          source = "public_events",
          source_id = ev$id %||% NA_character_,
          stringsAsFactors = FALSE
        )
      }))

      push_commit_df <- bind_rows(lapply(public_events, function(ev) {
        if (!identical(ev$type %||% "", "PushEvent")) {
          return(data.frame())
        }
        payload <- ev$payload %||% list()
        push_size <- suppressWarnings(as.numeric(payload$size %||% 0))
        if (is.na(push_size)) {
          push_size <- 0
        }
        data.frame(
          activity_time = ev$created_at %||% NA_character_,
          activity_type = "commit_from_push",
          activity_value = push_size,
          repository = ev$repo$name %||% NA_character_,
          source = "public_events_payload",
          source_id = ev$id %||% NA_character_,
          stringsAsFactors = FALSE
        )
      }))

      raw_parts <- append(raw_parts, list(events_df))
      if (nrow(push_commit_df) > 0) {
        raw_parts <- append(raw_parts, list(push_commit_df))
      }
    }
  } else if (isTRUE(include_event_activity) && skip_public_events_api) {
    diagnostics$public_events_loaded <- 0L
    diagnostics$public_events_window_limited <- NA
    diagnostics$public_events_source <- "clickhouse_cache"
  }

  # 2) Search commits by author in selected period.
  if (isTRUE(include_search_activity)) {
    if (!skip_commit_search_api) {
      live_api_used <- TRUE
      commits_query <- paste0("author:", username, " author-date:", period_qualifier)
      commit_search <- fetch_search_items(
        endpoint = "GET /search/commits",
        query = commits_query,
        limit = search_limit_commits,
        token = token,
        sort_by = "author-date",
        order = "desc",
        accept = "application/vnd.github.cloak-preview+json"
      )
      diagnostics$commit_search_total_count <- commit_search$total_count
      diagnostics$commit_search_limit_hits <- commit_search$total_count > length(commit_search$items)
      diagnostics$commit_search_source <- "github_api"

      if (length(commit_search$items) > 0) {
        commits_df <- bind_rows(lapply(commit_search$items, function(item) {
          data.frame(
            activity_time = item$commit$author$date %||% NA_character_,
            activity_type = "commit",
            activity_value = 1,
            repository = item$repository$full_name %||% NA_character_,
            source = "search_commits",
            source_id = item$sha %||% NA_character_,
            stringsAsFactors = FALSE
          )
        }))
        raw_parts <- append(raw_parts, list(commits_df))
      }
    } else {
      diagnostics$commit_search_source <- "clickhouse_cache"
    }

    # Issues opened.
    if (!skip_issues_search_api) {
      live_api_used <- TRUE
      issues_query <- paste0("author:", username, " is:issue created:", period_qualifier)
      issues_search <- fetch_search_items(
        endpoint = "GET /search/issues",
        query = issues_query,
        limit = search_limit_items,
        token = token,
        sort_by = "created",
        order = "desc"
      )
      diagnostics$issues_search_total_count <- issues_search$total_count
      diagnostics$issues_search_limit_hits <- issues_search$total_count > length(issues_search$items)
      diagnostics$issues_search_source <- "github_api"

      if (length(issues_search$items) > 0) {
        issues_df <- bind_rows(lapply(issues_search$items, function(item) {
          data.frame(
            activity_time = item$created_at %||% NA_character_,
            activity_type = "issue_opened",
            activity_value = 1,
            repository = sub("^https://api\\.github\\.com/repos/([^/]+/[^/]+).*$", "\\1", item$repository_url %||% ""),
            source = "search_issues_opened",
            source_id = item$node_id %||% NA_character_,
            stringsAsFactors = FALSE
          )
        }))
        raw_parts <- append(raw_parts, list(issues_df))
      }
    } else {
      diagnostics$issues_search_source <- "clickhouse_cache"
    }

    # Pull requests opened.
    if (!skip_prs_search_api) {
      live_api_used <- TRUE
      prs_query <- paste0("author:", username, " is:pr created:", period_qualifier)
      prs_search <- fetch_search_items(
        endpoint = "GET /search/issues",
        query = prs_query,
        limit = search_limit_items,
        token = token,
        sort_by = "created",
        order = "desc"
      )
      diagnostics$prs_search_total_count <- prs_search$total_count
      diagnostics$prs_search_limit_hits <- prs_search$total_count > length(prs_search$items)
      diagnostics$prs_search_source <- "github_api"

      if (length(prs_search$items) > 0) {
        prs_df <- bind_rows(lapply(prs_search$items, function(item) {
          data.frame(
            activity_time = item$created_at %||% NA_character_,
            activity_type = "pr_opened",
            activity_value = 1,
            repository = sub("^https://api\\.github\\.com/repos/([^/]+/[^/]+).*$", "\\1", item$repository_url %||% ""),
            source = "search_prs_opened",
            source_id = item$node_id %||% NA_character_,
            stringsAsFactors = FALSE
          )
        }))
        raw_parts <- append(raw_parts, list(prs_df))
      }
    } else {
      diagnostics$prs_search_source <- "clickhouse_cache"
    }
  }

  # 3) Reused data from comments module.
  need_comments_module <- (!skip_comments_events_module && isTRUE(include_event_activity)) ||
    (!skip_comments_threads_module && isTRUE(include_thread_engagement))
  if (need_comments_module) {
    live_api_used <- TRUE
    comments_result <- tryCatch({
      if (exists("get_github_user_comments", mode = "function")) {
        get_github_user_comments(
          profile = username,
          token = token,
          max_items = search_limit_comments,
          include_event_comments = include_event_activity
        )
      } else {
        list(thread_comments = data.frame(), event_comments = data.frame(), summary = data.frame())
      }
    }, error = function(e) {
      warning("Failed to load comments module data: ", e$message)
      list(thread_comments = data.frame(), event_comments = data.frame(), summary = data.frame())
    })
    diagnostics$comments_source <- "github_api"
  } else if ((isTRUE(include_event_activity) || isTRUE(include_thread_engagement)) &&
             (skip_comments_events_module || skip_comments_threads_module)) {
    diagnostics$comments_source <- "clickhouse_cache"
  }

  if (!is.null(comments_result$event_comments) && nrow(comments_result$event_comments) > 0) {
    comment_events_df <- comments_result$event_comments %>%
      mutate(
        activity_time = created_at,
        activity_type = case_when(
          event_type == "IssueCommentEvent" ~ "issue_comment",
          event_type == "PullRequestReviewCommentEvent" ~ "pr_review_comment",
          event_type == "CommitCommentEvent" ~ "commit_comment",
          TRUE ~ "comment_event"
        ),
        activity_value = 1,
        repository = repository,
        source = "module_user_comments_events",
        source_id = event_id
      ) %>%
      select(activity_time, activity_type, activity_value, repository, source, source_id)
    raw_parts <- append(raw_parts, list(comment_events_df))
  }

  if (isTRUE(include_thread_engagement) &&
      !is.null(comments_result$thread_comments) &&
      nrow(comments_result$thread_comments) > 0) {
    thread_df <- comments_result$thread_comments %>%
      mutate(
        activity_time = updated_at,
        activity_type = case_when(
          item_type == "issue" ~ "issue_thread_engagement",
          item_type == "pull_request" ~ "pr_thread_engagement",
          TRUE ~ "thread_engagement"
        ),
        activity_value = 1,
        repository = repository,
        source = "module_user_comments_threads",
        source_id = html_url
      ) %>%
      select(activity_time, activity_type, activity_value, repository, source, source_id)
    raw_parts <- append(raw_parts, list(thread_df))
  }

  # 4) Reused lightweight events from user stats module (additional signal).
  need_user_stats_events <- (is.null(diagnostics$public_events_loaded) || diagnostics$public_events_loaded == 0) &&
    !skip_live_calls &&
    !skip_public_events_api
  if (need_user_stats_events) {
    live_api_used <- TRUE
    user_stats_result <- tryCatch({
      if (exists("get_github_user_stats", mode = "function") && !is.null(conn)) {
        get_github_user_stats(
          profile = username,
          token = token,
          conn = conn,
          include_events = include_event_activity,
          events_limit = events_limit
        )
      } else {
        list(events = data.frame(), events_by_type = data.frame())
      }
    }, error = function(e) {
      warning("Failed to load stats module data: ", e$message)
      list(events = data.frame(), events_by_type = data.frame())
    })
    diagnostics$user_stats_source <- "github_api"
  } else if (skip_public_events_api) {
    diagnostics$user_stats_source <- "clickhouse_cache_or_skipped"
  }

  if ((is.null(diagnostics$public_events_loaded) || diagnostics$public_events_loaded == 0) &&
      !is.null(user_stats_result$events) && nrow(user_stats_result$events) > 0) {
    stats_events_df <- user_stats_result$events %>%
      mutate(
        activity_time = created_at,
        activity_type = paste0("stats_", tolower(gsub("Event$", "", type))),
        activity_value = 1,
        repository = repo,
        source = "module_user_stats_events",
        source_id = event_id
      ) %>%
      select(activity_time, activity_type, activity_value, repository, source, source_id)
    raw_parts <- append(raw_parts, list(stats_events_df))
  }

  normalize_raw_part <- function(df) {
    required_cols <- c("activity_time", "activity_type", "activity_value", "repository", "source", "source_id")
    if (!is.data.frame(df) || nrow(df) == 0) {
      return(data.frame(
        activity_time = character(),
        activity_type = character(),
        activity_value = numeric(),
        repository = character(),
        source = character(),
        source_id = character(),
        stringsAsFactors = FALSE
      ))
    }

    for (col in required_cols) {
      if (!(col %in% names(df))) {
        df[[col]] <- NA
      }
    }

    df %>%
      mutate(
        activity_time = as.character(activity_time),
        activity_type = as.character(activity_type),
        activity_value = suppressWarnings(as.numeric(activity_value)),
        repository = as.character(repository),
        source = as.character(source),
        source_id = as.character(source_id)
      ) %>%
      select(activity_time, activity_type, activity_value, repository, source, source_id)
  }

  raw_parts <- lapply(raw_parts, normalize_raw_part)
  raw_activity <- bind_rows(raw_parts)
  if (nrow(raw_activity) == 0) {
    raw_activity <- data.frame(
      activity_time = as.POSIXct(character(), tz = "UTC"),
      activity_type = character(),
      activity_value = numeric(),
      repository = character(),
      source = character(),
      source_id = character(),
      stringsAsFactors = FALSE
    )
  } else {
    raw_activity <- raw_activity %>%
      mutate(
        activity_time = as.POSIXct(activity_time, tz = "UTC"),
        activity_value = suppressWarnings(as.numeric(activity_value))
      ) %>%
      filter(!is.na(activity_time), !is.na(activity_value)) %>%
      distinct(
        activity_time,
        activity_type,
        activity_value,
        repository,
        source,
        source_id,
        .keep_all = TRUE
      )
  }
  collection_sec <- as.numeric(difftime(.now_utc(), collection_t0, units = "secs"))

  aggregation_t0 <- .now_utc()
  # Filter by requested period.
  raw_activity <- raw_activity %>%
    filter(activity_time >= start_time, activity_time <= end_time)

  all_detected_types <- sort(unique(raw_activity$activity_type))
  requested_types <- activity_types
  if (length(requested_types) == 0 || any(tolower(requested_types) == "all")) {
    requested_types <- all_detected_types
  }
  requested_types <- unique(tolower(requested_types))

  if (length(requested_types) > 0 && nrow(raw_activity) > 0) {
    raw_activity <- raw_activity %>%
      filter(tolower(activity_type) %in% requested_types)
  }

  if (nrow(raw_activity) > 0) {
    raw_activity <- raw_activity %>%
      mutate(bucket_time = floor_time_custom(activity_time, step_cfg))
  } else {
    raw_activity$bucket_time <- as.POSIXct(character(), tz = "UTC")
  }

  points <- if (nrow(raw_activity) > 0) {
    raw_activity %>%
      group_by(bucket_time, activity_type) %>%
      summarise(
        activity_value = sum(activity_value, na.rm = TRUE),
        events_count = n(),
        repositories_count = n_distinct(repository[!is.na(repository) & nzchar(repository)]),
        .groups = "drop"
      ) %>%
      arrange(bucket_time, activity_type)
  } else {
    data.frame(
      bucket_time = as.POSIXct(character(), tz = "UTC"),
      activity_type = character(),
      activity_value = numeric(),
      events_count = integer(),
      repositories_count = integer(),
      stringsAsFactors = FALSE
    )
  }

  if (isTRUE(include_zero_points) && length(requested_types) > 0) {
    time_grid <- build_time_sequence(start_time, end_time, step_cfg)
    full_grid <- expand.grid(
      bucket_time = time_grid,
      activity_type = requested_types,
      KEEP.OUT.ATTRS = FALSE,
      stringsAsFactors = FALSE
    )

    points <- full_grid %>%
      left_join(points, by = c("bucket_time", "activity_type")) %>%
      mutate(
        activity_value = ifelse(is.na(activity_value), 0, activity_value),
        events_count = ifelse(is.na(events_count), 0L, events_count),
        repositories_count = ifelse(is.na(repositories_count), 0L, repositories_count)
      ) %>%
      arrange(bucket_time, activity_type)

    # Public events API is capped (latest 300 events). Zero-filling older buckets for
    # event-derived activity types would be misleading, so we drop those artificial zeros.
    if (!is.na(oldest_public_event_time)) {
      earliest_public_bucket <- floor_time_custom(oldest_public_event_time, step_cfg)
      observed_event_derived_types <- unique(raw_activity$activity_type[
        raw_activity$source %in% c(
          "public_events",
          "public_events_payload",
          "module_user_comments_events",
          "module_user_stats_events"
        )
      ])
      known_event_derived_types <- c(
        "push_event", "watch_event", "fork_event", "create_event", "delete_event",
        "release_event", "public_event", "member_event", "wiki_event",
        "issues_event", "issue_opened_event", "pull_request_event", "pr_opened_event",
        "pr_review_event", "issue_comment_event", "pr_review_comment_event",
        "commit_comment_event", "issue_comment", "pr_review_comment", "commit_comment",
        "comment_event", "commit_from_push"
      )
      event_derived_types <- unique(c(
        observed_event_derived_types,
        requested_types[requested_types %in% known_event_derived_types]
      ))
      if (length(event_derived_types) > 0) {
        points <- points %>%
          filter(!(activity_type %in% event_derived_types & bucket_time < earliest_public_bucket))
      }
    }
  }

  points <- points %>%
    mutate(
      profile = username,
      period_start = start_time,
      period_end = end_time,
      step = as.character(step),
      generated_at = Sys.time(),
      weekday = weekdays(as.Date(bucket_time, tz = "UTC")),
      hour = as.integer(format(bucket_time, "%H", tz = "UTC")),
      is_weekend = as.POSIXlt(as.Date(bucket_time, tz = "UTC"), tz = "UTC")$wday %in% c(0, 6)
    )

  # Enrich rows with native profile counters.
  if (nrow(profile_native) > 0) {
    profile_enrichment <- profile_native[1, , drop = FALSE]
    for (nm in names(profile_enrichment)) {
      if (!(nm %in% names(points))) {
        points[[nm]] <- profile_enrichment[[nm]][1]
      }
    }
  }
  aggregation_sec <- as.numeric(difftime(.now_utc(), aggregation_t0, units = "secs"))

  diagnostics$available_activity_types <- all_detected_types
  diagnostics$selected_activity_types <- requested_types
  diagnostics$coverage_notes <- c(
    "Public events endpoint returns at most 300 recent events.",
    "Search endpoints can be capped by search_limit_* and API constraints."
  )
  diagnostics$oldest_public_event_time <- oldest_public_event_time
  diagnostics$live_api_used <- live_api_used

  # Save results to ClickHouse (points + raw timeline data).
  if (isTRUE(save_to_clickhouse)) {
    write_t0 <- .now_utc()
    if (!exists("load_df_to_clickhouse", mode = "function")) {
      stop("Function 'load_df_to_clickhouse' is not available for ClickHouse export.")
    }

    cache_hit_only <- cache_enabled &&
      nrow(cached_raw) > 0 &&
      !isTRUE(live_api_used)
    diagnostics$cache_hit_only <- cache_hit_only

    if (isTRUE(skip_clickhouse_write_on_cache_hit) && isTRUE(cache_hit_only)) {
      diagnostics$clickhouse_write_skipped <- TRUE
      write_sec <- as.numeric(difftime(.now_utc(), write_t0, units = "secs"))
      diagnostics$timings_sec <- list(
        cache_read = cache_read_sec,
        collection = collection_sec,
        aggregation = aggregation_sec,
        clickhouse_write = write_sec,
        total = as.numeric(difftime(.now_utc(), run_t0, units = "secs"))
      )
      return(list(
        points = points,
        raw_activity = raw_activity,
        diagnostics = diagnostics
      ))
    }

    ensure_clickhouse_delete(conn, table_points, username, start_time, end_time, as.character(step))
    ensure_clickhouse_delete(conn, table_raw, username, start_time, end_time, as.character(step))

    points_store <- points
    raw_store <- raw_activity %>%
      mutate(
        profile = username,
        period_start = start_time,
        period_end = end_time,
        step = as.character(step),
        generated_at = Sys.time()
      )

    load_df_to_clickhouse(
      df = points_store,
      table_name = table_points,
      conn = conn,
      overwrite = FALSE,
      append = TRUE
    )

    load_df_to_clickhouse(
      df = raw_store,
      table_name = table_raw,
      conn = conn,
      overwrite = FALSE,
      append = TRUE
    )
    write_sec <- as.numeric(difftime(.now_utc(), write_t0, units = "secs"))
    diagnostics$clickhouse_write_skipped <- FALSE
  }
  if (!isTRUE(save_to_clickhouse)) {
    diagnostics$clickhouse_write_skipped <- TRUE
  }
  diagnostics$timings_sec <- list(
    cache_read = cache_read_sec,
    collection = collection_sec,
    aggregation = aggregation_sec,
    clickhouse_write = write_sec,
    total = as.numeric(difftime(.now_utc(), run_t0, units = "secs"))
  )

  list(
    points = points,
    raw_activity = raw_activity,
    diagnostics = diagnostics
  )
}
