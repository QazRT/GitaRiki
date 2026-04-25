get_github_user_comments <- function(profile,
                                     token = NULL,
                                     max_items = 1000,
                                     include_event_comments = TRUE) {
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

  parse_repository_from_api_url <- function(url) {
    if (is.null(url) || !nzchar(url)) {
      return(NA_character_)
    }
    repo <- sub("^https://api\\.github\\.com/repos/([^/]+/[^/]+).*$", "\\1", url)
    if (identical(repo, url)) {
      return(NA_character_)
    }
    repo
  }

  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Could not extract GitHub username from: ", profile)
  }

  query_limit <- suppressWarnings(as.integer(max_items))
  if (is.na(query_limit) || query_limit < 1) {
    query_limit <- 1L
  }

  thread_comments <- tryCatch({
    per_query_limit <- max(1L, as.integer(ceiling(query_limit / 2)))
    qualifiers <- c("is:issue", "is:pr")

    collected <- lapply(qualifiers, function(qualifier) {
      query <- paste0("commenter:", username, " ", qualifier)
      result <- tryCatch({
        search_result <- gh::gh(
          "GET /search/issues",
          q = query,
          sort = "updated",
          order = "desc",
          .limit = per_query_limit,
          .token = token,
          .progress = FALSE
        )
        search_result$items
      }, error = function(e) {
        warning("Failed to load query '", query, "': ", e$message)
        list()
      })
      items <- result

      if (length(items) == 0) {
        return(data.frame())
      }

      bind_rows(lapply(items, function(item) {
        data.frame(
          repository = parse_repository_from_api_url(item$repository_url %||% ""),
          item_type = if (!is.null(item$pull_request)) "pull_request" else "issue",
          state = item$state %||% NA_character_,
          title = item$title %||% NA_character_,
          html_url = item$html_url %||% NA_character_,
          created_at = item$created_at %||% NA_character_,
          updated_at = item$updated_at %||% NA_character_,
          comments_count = item$comments %||% 0,
          author_association = item$author_association %||% NA_character_,
          stringsAsFactors = FALSE
        )
      }))
    })

    collected <- Filter(function(x) is.data.frame(x) && nrow(x) > 0, collected)
    if (length(collected) == 0) {
      data.frame()
    } else {
      bind_rows(collected) %>%
        arrange(desc(updated_at)) %>%
        distinct(html_url, .keep_all = TRUE)
    }
  }, error = function(e) {
    warning("Failed to load comment threads: ", e$message)
    data.frame()
  })

  event_comments <- data.frame()
  if (isTRUE(include_event_comments)) {
    event_limit <- min(query_limit, 300L)
    event_comments <- tryCatch({
      events <- gh::gh(
        "GET /users/{username}/events/public",
        username = username,
        .limit = event_limit,
        .token = token,
        .progress = TRUE
      )

      if (length(events) == 0) {
        return(data.frame())
      }

      comment_events <- Filter(function(ev) {
        (ev$type %||% "") %in% c(
          "IssueCommentEvent",
          "PullRequestReviewCommentEvent",
          "CommitCommentEvent"
        )
      }, events)

      if (length(comment_events) == 0) {
        return(data.frame())
      }

      bind_rows(lapply(comment_events, function(ev) {
        payload <- ev$payload %||% list()
        data.frame(
          event_id = ev$id %||% NA_character_,
          event_type = ev$type %||% NA_character_,
          repository = ev$repo$name %||% NA_character_,
          action = payload$action %||% NA_character_,
          created_at = ev$created_at %||% NA_character_,
          html_url = payload$comment$html_url %||% NA_character_,
          stringsAsFactors = FALSE
        )
      }))
    }, error = function(e) {
      warning("Failed to load comment events: ", e$message)
      data.frame()
    })
  }

  by_repo_threads <- if (nrow(thread_comments) > 0) {
    thread_comments %>%
      group_by(repository) %>%
      summarise(
        commented_threads = n(),
        commented_issues = sum(item_type == "issue", na.rm = TRUE),
        commented_pull_requests = sum(item_type == "pull_request", na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    data.frame()
  }

  by_repo_events <- if (nrow(event_comments) > 0) {
    event_comments %>%
      group_by(repository) %>%
      summarise(
        comment_events = n(),
        .groups = "drop"
      )
  } else {
    data.frame()
  }

  by_repository <- if (nrow(by_repo_threads) > 0 && nrow(by_repo_events) > 0) {
    full_join(by_repo_threads, by_repo_events, by = "repository")
  } else if (nrow(by_repo_threads) > 0) {
    by_repo_threads
  } else {
    by_repo_events
  }

  if (nrow(by_repository) > 0) {
    by_repository$comment_events[is.na(by_repository$comment_events)] <- 0
    by_repository$commented_threads[is.na(by_repository$commented_threads)] <- 0
    by_repository$commented_issues[is.na(by_repository$commented_issues)] <- 0
    by_repository$commented_pull_requests[is.na(by_repository$commented_pull_requests)] <- 0
    by_repository <- by_repository %>%
      arrange(desc(commented_threads), desc(comment_events))
  }

  unique_repos <- unique(c(
    if (nrow(thread_comments) > 0) thread_comments$repository else character(0),
    if (nrow(event_comments) > 0) event_comments$repository else character(0)
  ))
  unique_repos <- unique_repos[!is.na(unique_repos) & nzchar(unique_repos)]

  summary <- data.frame(
    username = username,
    total_commented_threads = nrow(thread_comments),
    issue_threads = if (nrow(thread_comments) > 0) sum(thread_comments$item_type == "issue", na.rm = TRUE) else 0,
    pull_request_threads = if (nrow(thread_comments) > 0) sum(thread_comments$item_type == "pull_request", na.rm = TRUE) else 0,
    observed_comment_events = nrow(event_comments),
    unique_repositories_commented = length(unique_repos),
    stringsAsFactors = FALSE
  )

  list(
    thread_comments = thread_comments,
    event_comments = event_comments,
    by_repository = by_repository,
    summary = summary
  )
}
