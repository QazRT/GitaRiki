# Анализ коммитной активности пользователя GitHub.

analyze_commit_activity <- function(commits_df,
                                    output_dir = NULL,
                                    profile_name = NULL) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Пакет 'dplyr' не установлен.")
  }
  
  if (!is.data.frame(commits_df) || nrow(commits_df) == 0) {
    warning("Коммиты отсутствуют. Анализ активности пропущен.")
    return(NULL)
  }
  
  profile_name <- derive_profile_name(profile_name, commits_df)
  output_dir <- resolve_analysis_output_dir(output_dir, profile_name = profile_name)
  file_prefix <- paste0(sanitize_file_part(profile_name), "_activity")
  
  commits_df <- prepare_commit_dates(commits_df)
  
  activity_overall <- data.frame(
    profile = profile_name,
    total_commits = nrow(commits_df),
    total_repositories = dplyr::n_distinct(commits_df$repository),
    active_days = dplyr::n_distinct(commits_df$commit_day[!is.na(commits_df$commit_day)]),
    first_commit = suppressWarnings(as.character(min(commits_df$commit_day, na.rm = TRUE))),
    last_commit = suppressWarnings(as.character(max(commits_df$commit_day, na.rm = TRUE))),
    avg_commits_per_repo = round(nrow(commits_df) / max(dplyr::n_distinct(commits_df$repository), 1), 3),
    avg_commits_per_active_day = round(
      nrow(commits_df) / max(dplyr::n_distinct(commits_df$commit_day[!is.na(commits_df$commit_day)]), 1),
      3
    ),
    median_commits_per_day = 0,
    stringsAsFactors = FALSE
  )
  
  by_day <- commits_df %>%
    dplyr::filter(!is.na(commit_day)) %>%
    dplyr::group_by(commit_day) %>%
    dplyr::summarise(
      commits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(commit_day)
  
  if (nrow(by_day) > 0) {
    activity_overall$median_commits_per_day <- stats::median(by_day$commits)
  }
  
  if (all(c("additions", "deletions", "changes") %in% names(commits_df))) {
    activity_overall$total_additions <- sum(commits_df$additions, na.rm = TRUE)
    activity_overall$total_deletions <- sum(commits_df$deletions, na.rm = TRUE)
    activity_overall$total_changes <- sum(commits_df$changes, na.rm = TRUE)
    activity_overall$avg_changes_per_commit <- round(mean(commits_df$changes, na.rm = TRUE), 3)
  }
  
  by_repo <- commits_df %>%
    dplyr::group_by(repository) %>%
    dplyr::summarise(
      total_commits = dplyr::n(),
      active_days = dplyr::n_distinct(commit_day[!is.na(commit_day)]),
      first_commit = suppressWarnings(as.character(min(commit_day, na.rm = TRUE))),
      last_commit = suppressWarnings(as.character(max(commit_day, na.rm = TRUE))),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(total_commits), repository)
  
  if (all(c("additions", "deletions", "changes") %in% names(commits_df))) {
    repo_changes <- commits_df %>%
      dplyr::group_by(repository) %>%
      dplyr::summarise(
        total_additions = sum(additions, na.rm = TRUE),
        total_deletions = sum(deletions, na.rm = TRUE),
        total_changes = sum(changes, na.rm = TRUE),
        avg_changes_per_commit = round(mean(changes, na.rm = TRUE), 3),
        .groups = "drop"
      )
    by_repo <- dplyr::left_join(by_repo, repo_changes, by = "repository")
  }
  
  by_month <- commits_df %>%
    dplyr::filter(!is.na(commit_day)) %>%
    dplyr::mutate(commit_month = format(commit_day, "%Y-%m")) %>%
    dplyr::group_by(commit_month) %>%
    dplyr::summarise(
      commits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(commit_month)
  
  saved_tables <- c(
    write_analysis_table(activity_overall, output_dir, paste0(file_prefix, "_overall.csv")),
    write_analysis_table(by_repo, output_dir, paste0(file_prefix, "_by_repo.csv")),
    write_analysis_table(by_day, output_dir, paste0(file_prefix, "_by_day.csv")),
    write_analysis_table(by_month, output_dir, paste0(file_prefix, "_by_month.csv"))
  )
  
  saved_plots <- character()
  
  if (nrow(by_repo) > 0) {
    top_repo <- utils::head(by_repo, 12)
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_top_repositories.png"),
      plot_fun = function() {
        graphics::par(mar = c(10, 5, 4, 2) + 0.1)
        graphics::barplot(
          height = top_repo$total_commits,
          names.arg = top_repo$repository,
          las = 2,
          col = "#2E86AB",
          main = paste("Топ репозиториев по коммитам:", profile_name),
          ylab = "Количество коммитов"
        )
      }
    ))
  }
  
  if (nrow(by_day) > 1) {
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_timeline.png"),
      plot_fun = function() {
        graphics::plot(
          x = by_day$commit_day,
          y = by_day$commits,
          type = "o",
          pch = 16,
          col = "#D1495B",
          main = paste("Динамика коммитов по дням:", profile_name),
          xlab = "Дата",
          ylab = "Коммиты"
        )
      }
    ))
  }
  
  list(
    overall = activity_overall,
    by_repo = by_repo,
    by_day = by_day,
    by_month = by_month,
    saved_tables = saved_tables,
    saved_plots = saved_plots
  )
}
