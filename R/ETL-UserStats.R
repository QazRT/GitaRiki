source("ETL-Repos.R")

get_github_user_stats <- function(profile,
                                  token = NULL,
                                  include_events = TRUE,
                                  events_limit = 300) {
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

  repo_to_df <- function(repo_list) {
    if (length(repo_list) == 0) {
      return(data.frame())
    }

    bind_rows(lapply(repo_list, function(r) {
      data.frame(
        id = r$id %||% NA,
        name = r$name %||% NA_character_,
        full_name = r$full_name %||% NA_character_,
        html_url = r$html_url %||% NA_character_,
        description = r$description %||% NA_character_,
        private = r$private %||% NA,
        fork = r$fork %||% NA,
        created_at = r$created_at %||% NA_character_,
        updated_at = r$updated_at %||% NA_character_,
        pushed_at = r$pushed_at %||% NA_character_,
        size = r$size %||% 0,
        language = r$language %||% NA_character_,
        stargazers_count = r$stargazers_count %||% 0,
        watchers_count = r$watchers_count %||% 0,
        forks_count = r$forks_count %||% 0,
        open_issues_count = r$open_issues_count %||% 0,
        default_branch = r$default_branch %||% NA_character_,
        stringsAsFactors = FALSE
      )
    }))
  }

  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Could not extract GitHub username from: ", profile)
  }

  user_data <- tryCatch({
    gh::gh(
      "GET /users/{username}",
      username = username,
      .token = token
    )
  }, error = function(e) {
    stop("Failed to load GitHub user profile: ", e$message)
  })


  owned_repos <- get_github_repos(username, token)

  language_stats <- if (nrow(owned_repos) > 0) {
    owned_repos %>%
      filter(!is.na(language), nzchar(language)) %>%
      group_by(language) %>%
      summarise(
        repositories = n(),
        total_repo_size_kb = sum(size, na.rm = TRUE),
        total_repo_stars = sum(stargazers_count, na.rm = TRUE),
        total_repo_forks = sum(forks_count, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(desc(repositories), desc(total_repo_stars))
  } else {
    data.frame()
  }

  profile_df <- data.frame(
    login = user_data$login %||% username,
    name = user_data$name %||% NA_character_,
    company = user_data$company %||% NA_character_,
    blog = user_data$blog %||% NA_character_,
    location = user_data$location %||% NA_character_,
    email = user_data$email %||% NA_character_,
    bio = user_data$bio %||% NA_character_,
    twitter_username = user_data$twitter_username %||% NA_character_,
    public_repos = user_data$public_repos %||% 0,
    public_gists = user_data$public_gists %||% 0,
    followers = user_data$followers %||% 0,
    following = user_data$following %||% 0,
    account_created_at = user_data$created_at %||% NA_character_,
    account_updated_at = user_data$updated_at %||% NA_character_,
    owned_repositories = nrow(owned_repos),
    total_stars_received = if (nrow(owned_repos) > 0) sum(owned_repos$stargazers_count, na.rm = TRUE) else 0,
    total_forks_received = if (nrow(owned_repos) > 0) sum(owned_repos$forks_count, na.rm = TRUE) else 0,
    total_open_issues = if (nrow(owned_repos) > 0) sum(owned_repos$open_issues_count, na.rm = TRUE) else 0,
    stringsAsFactors = FALSE
  )

  events <- data.frame()
  events_by_type <- data.frame()

  if (isTRUE(include_events)) {
    event_limit <- suppressWarnings(as.integer(events_limit))
    if (is.na(event_limit) || event_limit < 1) {
      event_limit <- 1L
    }
    event_limit <- min(event_limit, 300L)
    events <- tryCatch({
      raw_events <- gh::gh(
        "GET /users/{username}/events/public",
        username = username,
        .limit = event_limit,
        .token = token,
        .progress = TRUE
      )

      if (length(raw_events) == 0) {
        data.frame()
      } else {
        bind_rows(lapply(raw_events, function(ev) {
          data.frame(
            event_id = ev$id %||% NA_character_,
            type = ev$type %||% NA_character_,
            repo = ev$repo$name %||% NA_character_,
            created_at = ev$created_at %||% NA_character_,
            public = ev$public %||% NA,
            stringsAsFactors = FALSE
          )
        }))
      }
    }, error = function(e) {
      warning("Failed to load public events: ", e$message)
      data.frame()
    })

    if (nrow(events) > 0) {
      events_by_type <- events %>%
        group_by(type) %>%
        summarise(
          total_events = n(),
          first_seen = min(created_at, na.rm = TRUE),
          last_seen = max(created_at, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        arrange(desc(total_events))
    }
  }

  list(
    profile = profile_df,
    owned_repos = owned_repos,
    language_stats = language_stats,
    events = events,
    events_by_type = events_by_type
  )
}
