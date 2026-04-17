# Функция для получения расширенной информации о пользователе GitHub
# (репозитории, подписки, звёзды, активность по коммитам)
# с использованием ClickHouse как кэша и дозагрузкой новых данных.

get_github_user_info <- function(profile,
                                 token = NULL,
                                 include_commit_stats = FALSE,
                                 max_commits = 1000,
                                 conn = NULL) {
  
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  required_packages <- c("gh", "dplyr", "purrr", "httr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите вручную: install.packages('", pkg, "')")
    }
  }
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  extract_github_username <- function(input) {
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
    return(NULL)
  }
  
  repo_to_df <- function(repo_list) {
    if (length(repo_list) == 0) return(data.frame())
    bind_rows(lapply(repo_list, function(r) {
      data.frame(
        full_name        = r$full_name %||% NA,
        html_url         = r$html_url %||% NA,
        description      = r$description %||% NA,
        created_at       = r$created_at %||% NA,
        updated_at       = r$updated_at %||% NA,
        language         = r$language %||% NA,
        stargazers_count = r$stargazers_count %||% 0,
        forks_count      = r$forks_count %||% 0,
        stringsAsFactors = FALSE
      )
    }))
  }
  
  read_cached_rows <- function(table_name, where_sql) {
    table_exists <- exists("table_exists_custom", mode = "function") &&
      table_exists_custom(conn, table_name)
    
    if (!table_exists) {
      return(data.frame())
    }
    
    sql <- paste0("SELECT * FROM ", table_name, " WHERE ", where_sql)
    tryCatch(
      query_clickhouse(conn, sql),
      error = function(e) {
        warning("Не удалось загрузить данные из ClickHouse для таблицы ", table_name, ": ", e$message)
        data.frame()
      }
    )
  }
  
  save_incremental <- function(df,
                               tbl_name,
                               key_cols,
                               latest_key_cols = key_cols,
                               compare_cols = key_cols) {
    make_typed_na <- function(template, n) {
      if (inherits(template, "POSIXct") || inherits(template, "POSIXt")) {
        return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC"))
      }
      if (inherits(template, "Date")) {
        return(as.Date(rep(NA_real_, n), origin = "1970-01-01"))
      }
      if (is.character(template)) return(rep(NA_character_, n))
      if (is.integer(template)) return(rep(NA_integer_, n))
      if (is.numeric(template)) return(rep(NA_real_, n))
      if (is.logical(template)) return(rep(NA, n))
      rep(NA_character_, n)
    }
    
    existing_all <- read_cached_rows(tbl_name, paste0("profile = '", escaped_username, "'"))
    
    if ("fetched_at" %in% names(existing_all)) {
      existing_all$fetched_at <- as.character(existing_all$fetched_at)
    }
    
    if (nrow(existing_all) > 0) {
      message("В ClickHouse уже есть данные в ", tbl_name, ": ", nrow(existing_all), " записей.")
    }
    
    if (nrow(df) == 0) {
      if (length(latest_key_cols) > 0 && nrow(existing_all) > 0) {
        latest_existing <- existing_all %>%
          arrange(desc(fetched_at)) %>%
          distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
        return(latest_existing)
      }
      return(existing_all)
    }
    
    df <- df %>%
      mutate(
        profile = username,
        fetched_at = format(Sys.Date(), "%Y-%m-%d")
      )
    
    if ("fetched_at" %in% names(df)) {
      df$fetched_at <- as.character(df$fetched_at)
    }
    
    for (col in unique(c(compare_cols, latest_key_cols))) {
      if (!col %in% names(existing_all)) {
        existing_all[[col]] <- make_typed_na(df[[col]], nrow(existing_all))
      }
      if (!col %in% names(df)) {
        df[[col]] <- make_typed_na(existing_all[[col]], nrow(df))
      }
    }
    
    latest_existing <- if (nrow(existing_all) > 0) {
      existing_all %>%
        arrange(desc(fetched_at)) %>%
        distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
    } else {
      existing_all
    }
    
    df_for_compare <- df %>%
      distinct(across(all_of(compare_cols)), .keep_all = TRUE)
    
    if (nrow(latest_existing) > 0) {
      new_rows <- anti_join(df_for_compare, latest_existing, by = compare_cols)
    } else {
      new_rows <- df_for_compare
    }
    
    if (nrow(new_rows) > 0) {
      message("Сохраняю новые записи в ", tbl_name, ": ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = tbl_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      existing_all <- bind_rows(existing_all, new_rows)
    } else {
      message("Новых записей для таблицы ", tbl_name, " не найдено.")
    }
    
    existing_all %>%
      arrange(desc(fetched_at)) %>%
      distinct(across(all_of(latest_key_cols)), .keep_all = TRUE)
  }
  
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  escaped_username <- gsub("'", "''", username)
  
  message("Запрашиваю репозитории, принадлежащие пользователю ", username, "...")
  owned_repos <- tryCatch({
    repos <- gh::gh(
      "GET /users/{username}/repos",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(repos)
  }, error = function(e) {
    warning("Не удалось получить репозитории: ", e$message)
    data.frame()
  })
  
  message("Запрашиваю подписки пользователя ", username, "...")
  subscriptions <- tryCatch({
    subs <- gh::gh(
      "GET /users/{username}/subscriptions",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(subs)
  }, error = function(e) {
    warning("Не удалось получить подписки: ", e$message)
    data.frame()
  })
  
  message("Запрашиваю звёзды пользователя ", username, "...")
  starred <- tryCatch({
    stars <- gh::gh(
      "GET /users/{username}/starred",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    )
    repo_to_df(stars)
  }, error = function(e) {
    warning("Не удалось получить звёзды: ", e$message)
    data.frame()
  })
  
  message("Выполняю поиск коммитов автора ", username, " (макс. ", max_commits, ")...")
  commits_df <- data.frame()
  activity_summary <- data.frame()
  
  if (is.null(token)) {
    warning("Поиск коммитов требует аутентификации. Без токена эта часть будет пропущена.")
  } else {
    tryCatch({
      search_result <- gh::gh(
        "GET /search/commits",
        q = paste0("author:", username),
        .token = token,
        .limit = max_commits,
        .progress = TRUE
      )
      items <- search_result$items
      
      if (length(items) > 0) {
        commits_df <- map_df(items, function(cmt) {
          repo_full <- cmt$repository$full_name %||% NA
          data.frame(
            repository = repo_full,
            sha        = cmt$sha %||% NA,
            date       = cmt$commit$author$date %||% NA,
            message    = cmt$commit$message %||% NA,
            url        = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        }) %>%
          distinct(repository, sha, .keep_all = TRUE)
        
        if (include_commit_stats && nrow(commits_df) > 0) {
          message("Запрашиваю статистику изменений для ", nrow(commits_df), " коммитов...")
          stats_list <- vector("list", nrow(commits_df))
          for (i in seq_len(nrow(commits_df))) {
            if (i %% 10 == 0) message("  Обработано ", i, " из ", nrow(commits_df))
            sha <- commits_df$sha[i]
            repo <- commits_df$repository[i]
            if (is.na(repo) || is.na(sha)) next
            stats_list[[i]] <- tryCatch({
              cmt_detail <- gh::gh(
                "GET /repos/{repo}/commits/{sha}",
                repo = repo,
                sha = sha,
                .token = token
              )
              s <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
              data.frame(
                repository = repo,
                sha = sha,
                additions = s$additions %||% 0,
                deletions = s$deletions %||% 0,
                changes   = s$total %||% 0,
                stringsAsFactors = FALSE
              )
            }, error = function(e) {
              warning("Не удалось получить статистику для коммита ", sha, ": ", e$message)
              data.frame(
                repository = repo,
                sha = sha,
                additions = NA,
                deletions = NA,
                changes = NA,
                stringsAsFactors = FALSE
              )
            })
          }
          stats_df <- bind_rows(stats_list) %>%
            distinct(repository, sha, .keep_all = TRUE)
          commits_df <- commits_df %>%
            left_join(stats_df, by = c("repository", "sha"))
        }
        
        if (nrow(commits_df) > 0) {
          if (include_commit_stats && "additions" %in% names(commits_df)) {
            activity_summary <- commits_df %>%
              group_by(repository) %>%
              summarise(
                total_commits = n(),
                first_commit = min(date, na.rm = TRUE),
                last_commit = max(date, na.rm = TRUE),
                total_additions = sum(additions, na.rm = TRUE),
                total_deletions = sum(deletions, na.rm = TRUE),
                total_changes = sum(changes, na.rm = TRUE),
                .groups = "drop"
              ) %>%
              arrange(desc(total_commits))
          } else {
            activity_summary <- commits_df %>%
              group_by(repository) %>%
              summarise(
                total_commits = n(),
                first_commit = min(date, na.rm = TRUE),
                last_commit = max(date, na.rm = TRUE),
                .groups = "drop"
              ) %>%
              arrange(desc(total_commits))
          }
        }
      } else {
        message("Коммитов не найдено.")
      }
    }, error = function(e) {
      warning("Ошибка при поиске коммитов: ", e$message)
    })
  }
  
  message("Сохраняю данные в ClickHouse...")
  
  owned_repos_result <- save_incremental(
    df = owned_repos,
    tbl_name = "github_owned_repos",
    key_cols = c("profile", "full_name")
  )
  
  subscriptions_result <- save_incremental(
    df = subscriptions,
    tbl_name = "github_subscriptions",
    key_cols = c("profile", "full_name")
  )
  
  starred_result <- save_incremental(
    df = starred,
    tbl_name = "github_starred",
    key_cols = c("profile", "full_name")
  )
  
  raw_commits_result <- save_incremental(
    df = commits_df,
    tbl_name = "github_raw_commits",
    key_cols = c("profile", "repository", "sha")
  )
  
  activity_compare_cols <- c("profile", "repository", "total_commits", "first_commit", "last_commit")
  if (include_commit_stats && nrow(activity_summary) > 0) {
    activity_compare_cols <- c(
      activity_compare_cols,
      "total_additions",
      "total_deletions",
      "total_changes"
    )
  }
  
  commits_activity_result <- save_incremental(
    df = activity_summary,
    tbl_name = "github_commits_activity",
    key_cols = activity_compare_cols,
    latest_key_cols = c("profile", "repository"),
    compare_cols = activity_compare_cols
  )
  
  result <- list(
    owned_repos      = owned_repos_result,
    subscriptions    = subscriptions_result,
    starred          = starred_result,
    commits_activity = commits_activity_result,
    raw_commits      = raw_commits_result
  )
  
  message("Сбор данных завершён.")
  result
}
