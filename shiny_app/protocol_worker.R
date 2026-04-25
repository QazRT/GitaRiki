args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop("Usage: Rscript protocol_worker.R <job.rds>", call. = FALSE)
}

job_path <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
job <- readRDS(job_path)

write_progress <- function(value = 0, label = NULL) {
  dir.create(dirname(job$progress_path), recursive = TRUE, showWarnings = FALSE)
  tmp <- paste0(job$progress_path, ".", Sys.getpid(), ".tmp")
  saveRDS(
    list(value = value, label = label, updated_at = Sys.time()),
    tmp
  )
  file.rename(tmp, job$progress_path)
}

write_result <- function(result) {
  dir.create(dirname(job$result_path), recursive = TRUE, showWarnings = FALSE)
  tmp <- paste0(job$result_path, ".", Sys.getpid(), ".tmp")
  saveRDS(result, tmp)
  file.rename(tmp, job$result_path)
}

tryCatch({
  if (!is.null(job$project_root) && nzchar(job$project_root)) {
    setwd(job$project_root)
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

  source(job$account_storage_path, encoding = "UTF-8")
  source(job$set_protocol_path, encoding = "UTF-8")

  progress_label <- job$label
  progress <- function(value, label = NULL) {
    if (!is.null(label) && nzchar(label)) {
      progress_label <<- label
    }
    write_progress(value, progress_label)
  }

  result <- if (identical(job$protocol_type, "isis")) {
    run_isis_protocol(
      profile = job$target,
      conn = job$conn,
      progress = progress
    )
  } else {
    run_set_protocol(
      profile = job$target,
      token = job$token,
      conn = job$conn,
      progress = progress
    )
  }

  write_result(list(ok = TRUE, result = result))
}, error = function(e) {
  write_result(list(ok = FALSE, error = conditionMessage(e)))
})
