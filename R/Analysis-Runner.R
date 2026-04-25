# Общий запуск аналитики по данным, полученным из ETL-функций.
#
# Пример:
# source("R/Analysis-Helpers.R")
# source("R/Analysis-Activity.R")
# source("R/Analysis-Popularity.R")
# source("R/Analysis-Runner.R")
#
# analysis <- run_github_profile_analysis(
#   commits_df = commits_df,
#   links_df = links_df,
#   user_info = user_info
# )

run_github_profile_analysis <- function(commits_df = NULL,
                                        links_df = NULL,
                                        user_info = NULL,
                                        owned_repos_df = NULL,
                                        starred_df = NULL,
                                        subscriptions_df = NULL,
                                        output_dir = NULL,
                                        profile_name = NULL) {
  combine_commit_sources <- function(primary_df, secondary_df) {
    if (!is.data.frame(primary_df) || nrow(primary_df) == 0) {
      return(secondary_df)
    }
    if (!is.data.frame(secondary_df) || nrow(secondary_df) == 0) {
      return(primary_df)
    }
    
    combined <- dplyr::bind_rows(primary_df, secondary_df)
    key_cols <- intersect(c("repository", "sha"), names(combined))
    
    if (length(key_cols) == 2) {
      combined <- combined %>%
        dplyr::distinct(.data$repository, .data$sha, .keep_all = TRUE)
    } else {
      combined <- dplyr::distinct(combined)
    }
    
    combined
  }
  
  if (is.list(user_info)) {
    if (is.null(owned_repos_df) && "owned_repos" %in% names(user_info)) {
      owned_repos_df <- user_info$owned_repos
    }
    if (is.null(starred_df) && "starred" %in% names(user_info)) {
      starred_df <- user_info$starred
    }
    if (is.null(subscriptions_df) && "subscriptions" %in% names(user_info)) {
      subscriptions_df <- user_info$subscriptions
    }
    if ("raw_commits" %in% names(user_info)) {
      commits_df <- combine_commit_sources(commits_df, user_info$raw_commits)
    }
  }
  
  profile_name <- derive_profile_name(
    profile_name,
    commits_df,
    links_df,
    owned_repos_df,
    starred_df,
    subscriptions_df
  )
  
  output_dir <- resolve_analysis_output_dir(output_dir, profile_name = profile_name)
  
  results <- list(
    profile_name = profile_name,
    output_dir = output_dir
  )
  
  if (is.data.frame(commits_df) && nrow(commits_df) > 0) {
    results$activity <- analyze_commit_activity(
      commits_df = commits_df,
      output_dir = output_dir,
      profile_name = profile_name
    )
  }
  
  if (is.data.frame(owned_repos_df) && nrow(owned_repos_df) > 0) {
    results$popularity <- analyze_profile_popularity(
      owned_repos_df = owned_repos_df,
      starred_df = starred_df,
      subscriptions_df = subscriptions_df,
      output_dir = output_dir,
      profile_name = profile_name
    )
  }
  
  if (is.data.frame(commits_df) && nrow(commits_df) > 0 &&
      is.data.frame(owned_repos_df) && nrow(owned_repos_df) > 0) {
    results$ownership <- analyze_repository_ownership_activity(
      commits_df = commits_df,
      owned_repos_df = owned_repos_df,
      output_dir = output_dir,
      profile_name = profile_name
    )
  }
  
  if (is.data.frame(links_df) && nrow(links_df) > 0) {
    results$social <- analyze_social_presence(
      links_df = links_df,
      output_dir = output_dir,
      profile_name = profile_name
    )
  }
  
  summary_rows <- list()
  
  if (!is.null(results$activity)) {
    summary_rows$activity <- data.frame(
      section = "activity",
      metric = c("total_commits", "total_repositories", "active_days", "avg_commits_per_active_day"),
      value = c(
        results$activity$overall$total_commits,
        results$activity$overall$total_repositories,
        results$activity$overall$active_days,
        results$activity$overall$avg_commits_per_active_day
      ),
      stringsAsFactors = FALSE
    )
  }
  
  if (!is.null(results$popularity)) {
    summary_rows$popularity <- data.frame(
      section = "popularity",
      metric = c("owned_repositories", "total_stars_received", "total_forks_received", "popularity_index"),
      value = c(
        results$popularity$overall$owned_repositories,
        results$popularity$overall$total_stars_received,
        results$popularity$overall$total_forks_received,
        results$popularity$overall$popularity_index
      ),
      stringsAsFactors = FALSE
    )
  }
  
  if (!is.null(results$ownership)) {
    ownership_summary <- results$ownership$summary
    own_row <- ownership_summary[ownership_summary$ownership_type == "own_repo", , drop = FALSE]
    external_row <- ownership_summary[ownership_summary$ownership_type == "external_repo", , drop = FALSE]
    
    summary_rows$ownership <- data.frame(
      section = "ownership",
      metric = c(
        "own_repositories",
        "external_repositories",
        "own_commit_share",
        "external_commit_share",
        "own_avg_commits_per_repo",
        "external_avg_commits_per_repo"
      ),
      value = c(
        if (nrow(own_row) == 0) 0 else own_row$repositories[[1]],
        if (nrow(external_row) == 0) 0 else external_row$repositories[[1]],
        if (nrow(own_row) == 0) 0 else own_row$commit_share[[1]],
        if (nrow(external_row) == 0) 0 else external_row$commit_share[[1]],
        if (nrow(own_row) == 0) 0 else own_row$avg_commits_per_repo[[1]],
        if (nrow(external_row) == 0) 0 else external_row$avg_commits_per_repo[[1]]
      ),
      stringsAsFactors = FALSE
    )
  }
  
  if (!is.null(results$social)) {
    summary_rows$social <- data.frame(
      section = "social",
      metric = c("total_links", "unique_link_types"),
      value = c(
        results$social$overall$total_links,
        results$social$overall$unique_link_types
      ),
      stringsAsFactors = FALSE
    )
  }
  
  if (length(summary_rows) > 0) {
    summary_df <- dplyr::bind_rows(summary_rows)
    summary_path <- write_analysis_table(
      summary_df,
      output_dir,
      paste0(sanitize_file_part(profile_name), "_analysis_summary.csv")
    )
    results$summary <- summary_df
    results$summary_path <- summary_path
  }
  
  results
}
