args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop("Usage: Rscript protocol_task_worker.R <task.rds>", call. = FALSE)
}

task_path <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
task <- readRDS(task_path)

write_result <- function(result) {
  dir.create(dirname(task$result_path), recursive = TRUE, showWarnings = FALSE)
  tmp <- paste0(task$result_path, ".", Sys.getpid(), ".tmp")
  saveRDS(result, tmp)
  file.rename(tmp, task$result_path)
}

tryCatch({
  if (!is.null(task$project_root) && nzchar(task$project_root)) {
    setwd(task$project_root)
  }

  renviron_candidates <- c(
    file.path(getwd(), ".Renviron"),
    file.path(dirname(normalizePath(getwd(), winslash = "/", mustWork = FALSE)), ".Renviron")
  )
  for (renviron_path in renviron_candidates[file.exists(renviron_candidates)]) {
    readRenviron(renviron_path)
  }

  local_lib_candidates <- c(
    file.path(getwd(), ".Rlibs"),
    file.path(dirname(normalizePath(getwd(), winslash = "/", mustWork = FALSE)), ".Rlibs")
  )
  local_libs <- local_lib_candidates[dir.exists(local_lib_candidates)]
  if (length(local_libs) > 0L) {
    .libPaths(unique(c(normalizePath(local_libs, winslash = "/", mustWork = FALSE), .libPaths())))
  }

  source(task$account_storage_path, encoding = "UTF-8")
  source(task$set_protocol_path, encoding = "UTF-8")
  load_set_protocol_dependencies()
  conn <- connect_githound_clickhouse()

  result <- switch(
    task$task,
    commits = get_user_commits(
      profile = task$profile,
      token = task$token,
      include_stats = TRUE,
      conn = conn
    ),
    links = get_github_social_links(
      profile = task$profile,
      token = task$token,
      conn = conn
    ),
    user_info = get_github_user_info(
      profile = task$profile,
      token = task$token,
      include_commit_stats = TRUE,
      conn = conn
    ),
    activity_timeline = get_github_user_activity_timeline(
      profile = task$profile,
      token = task$token,
      conn = conn,
      start_at = Sys.time() - 365 * 24 * 60 * 60 * 8,
      end_at = Sys.time(),
      step = "month",
      activity_types = c(
        "commit",
        "issue_opened",
        "pr_opened",
        "issue_comment",
        "pr_review_comment",
        "commit_comment",
        "push_event",
        "watch_event",
        "fork_event"
      ),
      include_zero_points = TRUE,
      use_clickhouse_cache = TRUE,
      cache_max_age_hours = 12,
      cache_only = FALSE,
      profile_cache_table = "github_user_profile_native_stats",
      save_to_clickhouse = TRUE
    ),
    stop("Unknown protocol task: ", task$task, call. = FALSE)
  )

  write_result(list(ok = TRUE, result = result))
}, error = function(e) {
  write_result(list(ok = FALSE, error = conditionMessage(e)))
})
