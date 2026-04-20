# Анализ популярности профиля и репозиториев.

analyze_profile_popularity <- function(owned_repos_df,
                                       starred_df = NULL,
                                       subscriptions_df = NULL,
                                       output_dir = NULL,
                                       profile_name = NULL) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Пакет 'dplyr' не установлен.")
  }
  
  if (!is.data.frame(owned_repos_df) || nrow(owned_repos_df) == 0) {
    warning("Собственные репозитории отсутствуют. Анализ популярности пропущен.")
    return(NULL)
  }
  
  profile_name <- derive_profile_name(profile_name, owned_repos_df)
  output_dir <- resolve_analysis_output_dir(output_dir, profile_name = profile_name)
  file_prefix <- paste0(sanitize_file_part(profile_name), "_popularity")
  
  popularity_overall <- data.frame(
    profile = profile_name,
    owned_repositories = nrow(owned_repos_df),
    total_stars_received = sum(owned_repos_df$stargazers_count, na.rm = TRUE),
    avg_stars_per_repo = round(mean(owned_repos_df$stargazers_count, na.rm = TRUE), 3),
    median_stars_per_repo = stats::median(owned_repos_df$stargazers_count, na.rm = TRUE),
    max_stars_on_repo = max(owned_repos_df$stargazers_count, na.rm = TRUE),
    total_forks_received = sum(owned_repos_df$forks_count, na.rm = TRUE),
    avg_forks_per_repo = round(mean(owned_repos_df$forks_count, na.rm = TRUE), 3),
    starred_external_repos = if (is.data.frame(starred_df)) nrow(starred_df) else 0,
    watched_external_repos = if (is.data.frame(subscriptions_df)) nrow(subscriptions_df) else 0,
    popularity_index = 0,
    stringsAsFactors = FALSE
  )
  
  popularity_overall$popularity_index <- round(
    popularity_overall$total_stars_received +
      0.7 * popularity_overall$total_forks_received +
      0.15 * popularity_overall$starred_external_repos +
      0.15 * popularity_overall$watched_external_repos,
    3
  )
  
  repo_popularity <- owned_repos_df %>%
    dplyr::mutate(
      popularity_score = stargazers_count + 0.7 * forks_count,
      popularity_share = round(
        stargazers_count / max(sum(stargazers_count, na.rm = TRUE), 1),
        4
      )
    ) %>%
    dplyr::arrange(dplyr::desc(popularity_score), full_name)
  
  social_interest <- data.frame(
    metric = c("starred_repositories", "watched_repositories"),
    value = c(
      if (is.data.frame(starred_df)) nrow(starred_df) else 0,
      if (is.data.frame(subscriptions_df)) nrow(subscriptions_df) else 0
    ),
    stringsAsFactors = FALSE
  )
  
  saved_tables <- c(
    write_analysis_table(popularity_overall, output_dir, paste0(file_prefix, "_overall.csv")),
    write_analysis_table(repo_popularity, output_dir, paste0(file_prefix, "_by_repo.csv")),
    write_analysis_table(social_interest, output_dir, paste0(file_prefix, "_social_interest.csv"))
  )
  
  saved_plots <- character()
  
  if (nrow(repo_popularity) > 0) {
    top_repo <- utils::head(repo_popularity, 12)
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_top_by_stars.png"),
      plot_fun = function() {
        graphics::par(mar = c(10, 5, 4, 2) + 0.1)
        graphics::barplot(
          height = top_repo$stargazers_count,
          names.arg = top_repo$full_name,
          las = 2,
          col = "#4F772D",
          main = paste("Топ репозиториев по звёздам:", profile_name),
          ylab = "Количество звёзд"
        )
      }
    ))
    
    if (nrow(repo_popularity) > 1) {
      saved_plots <- c(saved_plots, save_base_plot(
        output_dir = output_dir,
        file_name = paste0(file_prefix, "_stars_vs_forks.png"),
        plot_fun = function() {
          graphics::plot(
            x = repo_popularity$stargazers_count,
            y = repo_popularity$forks_count,
            pch = 19,
            col = "#1B4965",
            main = paste("Звёзды и форки:", profile_name),
            xlab = "Звёзды",
            ylab = "Форки"
          )
          graphics::text(
            x = repo_popularity$stargazers_count,
            y = repo_popularity$forks_count,
            labels = repo_popularity$full_name,
            pos = 4,
            cex = 0.7
          )
        }
      ))
    }
  }
  
  list(
    overall = popularity_overall,
    by_repo = repo_popularity,
    social_interest = social_interest,
    saved_tables = saved_tables,
    saved_plots = saved_plots
  )
}

# Сравнение активности в собственных и чужих репозиториях.
analyze_repository_ownership_activity <- function(commits_df,
                                                  owned_repos_df,
                                                  output_dir = NULL,
                                                  profile_name = NULL) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Пакет 'dplyr' не установлен.")
  }
  
  if (!is.data.frame(commits_df) || nrow(commits_df) == 0) {
    warning("Коммиты отсутствуют. Анализ соотношения активности пропущен.")
    return(NULL)
  }
  
  if (!is.data.frame(owned_repos_df) || nrow(owned_repos_df) == 0) {
    warning("Список собственных репозиториев отсутствует. Анализ соотношения активности пропущен.")
    return(NULL)
  }
  
  profile_name <- derive_profile_name(profile_name, commits_df, owned_repos_df)
  output_dir <- resolve_analysis_output_dir(output_dir, profile_name = profile_name)
  file_prefix <- paste0(sanitize_file_part(profile_name), "_ownership")
  
  own_repo_names <- unique(owned_repos_df$full_name)
  commits_df <- prepare_commit_dates(commits_df)
  
  ownership_commits <- commits_df %>%
    dplyr::mutate(
      ownership_type = ifelse(repository %in% own_repo_names, "own_repo", "external_repo")
    )
  
  ownership_by_repo <- ownership_commits %>%
    dplyr::group_by(ownership_type, repository) %>%
    dplyr::summarise(
      total_commits = dplyr::n(),
      active_days = dplyr::n_distinct(commit_day[!is.na(commit_day)]),
      first_commit = suppressWarnings(as.character(min(commit_day, na.rm = TRUE))),
      last_commit = suppressWarnings(as.character(max(commit_day, na.rm = TRUE))),
      .groups = "drop"
    ) %>%
    dplyr::arrange(ownership_type, dplyr::desc(total_commits), repository)
  
  if (all(c("additions", "deletions", "changes") %in% names(ownership_commits))) {
    ownership_by_repo_stats <- ownership_commits %>%
      dplyr::group_by(ownership_type, repository) %>%
      dplyr::summarise(
        total_additions = sum(additions, na.rm = TRUE),
        total_deletions = sum(deletions, na.rm = TRUE),
        total_changes = sum(changes, na.rm = TRUE),
        avg_changes_per_commit = round(mean(changes, na.rm = TRUE), 3),
        .groups = "drop"
      )
    
    ownership_by_repo <- ownership_by_repo %>%
      dplyr::left_join(ownership_by_repo_stats, by = c("ownership_type", "repository"))
  }
  
  ownership_summary <- ownership_commits %>%
    dplyr::group_by(ownership_type) %>%
    dplyr::summarise(
      repositories = dplyr::n_distinct(repository),
      commits = dplyr::n(),
      active_days = dplyr::n_distinct(commit_day[!is.na(commit_day)]),
      avg_commits_per_repo = round(dplyr::n() / max(dplyr::n_distinct(repository), 1), 3),
      avg_commits_per_active_day = round(dplyr::n() / max(dplyr::n_distinct(commit_day[!is.na(commit_day)]), 1), 3),
      first_commit = suppressWarnings(as.character(min(commit_day, na.rm = TRUE))),
      last_commit = suppressWarnings(as.character(max(commit_day, na.rm = TRUE))),
      .groups = "drop"
    )
  
  if (all(c("additions", "deletions", "changes") %in% names(ownership_commits))) {
    ownership_summary_stats <- ownership_commits %>%
      dplyr::group_by(ownership_type) %>%
      dplyr::summarise(
        total_additions = sum(additions, na.rm = TRUE),
        total_deletions = sum(deletions, na.rm = TRUE),
        total_changes = sum(changes, na.rm = TRUE),
        avg_changes_per_commit = round(mean(changes, na.rm = TRUE), 3),
        .groups = "drop"
      )
    
    ownership_summary <- ownership_summary %>%
      dplyr::left_join(ownership_summary_stats, by = "ownership_type")
  }
  
  total_commits <- sum(ownership_summary$commits)
  ownership_summary$commit_share <- round(ownership_summary$commits / max(total_commits, 1), 4)
  ownership_summary$profile <- profile_name
  
  ownership_by_month <- ownership_commits %>%
    dplyr::filter(!is.na(commit_day)) %>%
    dplyr::mutate(commit_month = format(commit_day, "%Y-%m")) %>%
    dplyr::group_by(ownership_type, commit_month) %>%
    dplyr::summarise(
      commits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(ownership_type, commit_month)
  
  ownership_by_day <- ownership_commits %>%
    dplyr::filter(!is.na(commit_day)) %>%
    dplyr::group_by(ownership_type, commit_day) %>%
    dplyr::summarise(
      commits = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(ownership_type, commit_day)
  
  saved_tables <- c(
    write_analysis_table(ownership_summary, output_dir, paste0(file_prefix, "_summary.csv")),
    write_analysis_table(ownership_by_repo, output_dir, paste0(file_prefix, "_by_repo.csv")),
    write_analysis_table(ownership_by_month, output_dir, paste0(file_prefix, "_by_month.csv")),
    write_analysis_table(ownership_by_day, output_dir, paste0(file_prefix, "_by_day.csv"))
  )
  
  saved_plots <- character()
  
  if (nrow(ownership_summary) > 0) {
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_commit_split.png"),
      plot_fun = function() {
        graphics::barplot(
          height = ownership_summary$commits,
          names.arg = ownership_summary$ownership_type,
          col = c("#2A9D8F", "#E76F51"),
          main = paste("Соотношение коммитов: свои и чужие репозитории:", profile_name),
          ylab = "Количество коммитов"
        )
      }
    ))
  }
  
  if (nrow(ownership_by_month) > 0) {
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_monthly_split.png"),
      plot_fun = function() {
        own_month <- ownership_by_month %>%
          dplyr::filter(ownership_type == "own_repo")
        ext_month <- ownership_by_month %>%
          dplyr::filter(ownership_type == "external_repo")
        
        all_months <- sort(unique(ownership_by_month$commit_month))
        own_values <- stats::setNames(rep(0, length(all_months)), all_months)
        ext_values <- stats::setNames(rep(0, length(all_months)), all_months)
        
        if (nrow(own_month) > 0) own_values[own_month$commit_month] <- own_month$commits
        if (nrow(ext_month) > 0) ext_values[ext_month$commit_month] <- ext_month$commits
        
        mat <- rbind(own_values, ext_values)
        graphics::barplot(
          mat,
          beside = FALSE,
          col = c("#2A9D8F", "#E76F51"),
          legend.text = c("own_repo", "external_repo"),
          args.legend = list(x = "topright", bty = "n"),
          names.arg = all_months,
          las = 2,
          main = paste("Помесячная активность: свои и чужие репозитории:", profile_name),
          ylab = "Количество коммитов"
        )
      }
    ))
  }
  
  external_top <- ownership_by_repo %>%
    dplyr::filter(ownership_type == "external_repo") %>%
    utils::head(10)
  
  if (nrow(external_top) > 0) {
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_top_external_repos.png"),
      plot_fun = function() {
        graphics::par(mar = c(10, 5, 4, 2) + 0.1)
        graphics::barplot(
          height = external_top$total_commits,
          names.arg = external_top$repository,
          las = 2,
          col = "#F4A261",
          main = paste("Топ чужих репозиториев по активности:", profile_name),
          ylab = "Количество коммитов"
        )
      }
    ))
  }
  
  list(
    summary = ownership_summary,
    by_repo = ownership_by_repo,
    by_month = ownership_by_month,
    by_day = ownership_by_day,
    saved_tables = saved_tables,
    saved_plots = saved_plots
  )
}

# Дополнительный анализ социальных ссылок профиля.
analyze_social_presence <- function(links_df,
                                    output_dir = NULL,
                                    profile_name = NULL) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Пакет 'dplyr' не установлен.")
  }
  
  if (!is.data.frame(links_df) || nrow(links_df) == 0) {
    warning("Социальные ссылки отсутствуют. Анализ социальных профилей пропущен.")
    return(NULL)
  }
  
  profile_name <- derive_profile_name(profile_name, links_df)
  output_dir <- resolve_analysis_output_dir(output_dir, profile_name = profile_name)
  file_prefix <- paste0(sanitize_file_part(profile_name), "_social")
  
  by_type <- links_df %>%
    dplyr::group_by(type) %>%
    dplyr::summarise(
      links = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(links), type)
  
  by_source <- links_df %>%
    dplyr::group_by(source) %>%
    dplyr::summarise(
      links = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(links), source)
  
  summary_df <- data.frame(
    profile = profile_name,
    total_links = nrow(links_df),
    unique_link_types = dplyr::n_distinct(links_df$type),
    unique_sources = dplyr::n_distinct(links_df$source),
    stringsAsFactors = FALSE
  )
  
  saved_tables <- c(
    write_analysis_table(summary_df, output_dir, paste0(file_prefix, "_overall.csv")),
    write_analysis_table(by_type, output_dir, paste0(file_prefix, "_by_type.csv")),
    write_analysis_table(by_source, output_dir, paste0(file_prefix, "_by_source.csv"))
  )
  
  saved_plots <- character()
  
  if (nrow(by_type) > 0) {
    saved_plots <- c(saved_plots, save_base_plot(
      output_dir = output_dir,
      file_name = paste0(file_prefix, "_type_distribution.png"),
      plot_fun = function() {
        graphics::barplot(
          height = by_type$links,
          names.arg = by_type$type,
          col = "#6A4C93",
          main = paste("Распределение социальных ссылок:", profile_name),
          ylab = "Количество ссылок"
        )
      }
    ))
  }
  
  list(
    overall = summary_df,
    by_type = by_type,
    by_source = by_source,
    saved_tables = saved_tables,
    saved_plots = saved_plots
  )
}
