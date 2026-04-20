# Вспомогательные функции для сохранения аналитических таблиц и графиков.

resolve_analysis_output_dir <- function(output_dir = NULL, profile_name = NULL) {
  safe_profile_name <- sanitize_file_part(profile_name)
  
  if (is.null(output_dir) || !nzchar(output_dir)) {
    base_dir <- if (dir.exists("R")) {
      file.path("R", "analysis_output")
    } else {
      "analysis_output"
    }
    output_dir <- file.path(base_dir, safe_profile_name)
  }
  
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  normalizePath(output_dir, winslash = "/", mustWork = FALSE)
}

sanitize_file_part <- function(x) {
  x <- if (is.null(x) || !nzchar(x)) "unknown_profile" else x
  gsub("[^A-Za-z0-9_-]", "_", x)
}

derive_profile_name <- function(...) {
  candidates <- list(...)
  
  for (candidate in candidates) {
    if (is.null(candidate)) next
    
    if (is.character(candidate) && length(candidate) == 1 && nzchar(candidate)) {
      return(candidate)
    }
    
    if (is.data.frame(candidate) && "profile" %in% names(candidate) && nrow(candidate) > 0) {
      values <- candidate$profile[!is.na(candidate$profile) & nzchar(candidate$profile)]
      if (length(values) > 0) {
        return(values[[1]])
      }
    }
  }
  
  "unknown_profile"
}

write_analysis_table <- function(df, output_dir, file_name) {
  if (!is.data.frame(df)) {
    stop("Ожидался data.frame для сохранения в ", file_name)
  }
  
  target_path <- file.path(output_dir, file_name)
  utils::write.csv(df, target_path, row.names = FALSE, fileEncoding = "UTF-8")
  target_path
}

save_base_plot <- function(output_dir,
                           file_name,
                           plot_fun,
                           width = 1400,
                           height = 900,
                           res = 140) {
  target_path <- file.path(output_dir, file_name)
  grDevices::png(target_path, width = width, height = height, res = res)
  on.exit(grDevices::dev.off(), add = TRUE)
  plot_fun()
  target_path
}

prepare_commit_dates <- function(commits_df) {
  if (!is.data.frame(commits_df) || !"date" %in% names(commits_df) || nrow(commits_df) == 0) {
    return(commits_df)
  }
  
  parsed <- suppressWarnings(as.POSIXct(commits_df$date, tz = "UTC"))
  commits_df$commit_datetime <- parsed
  commits_df$commit_day <- as.Date(parsed)
  commits_df
}

empty_plot_result <- function(message_text) {
  list(saved_files = character(), message = message_text)
}
