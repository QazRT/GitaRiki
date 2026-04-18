# Function to fetch GitHub user commits with ClickHouse caching.

get_user_commits <- function(profile,
                             token = NULL,
                             repos = NULL,
                             max_repos = NULL,
                             include_stats = FALSE,
                             conn = NULL) {
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  required_packages <- c("gh", "dplyr", "purrr", "httr", "jsonlite")
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
        warning("Не удалось загрузить данные из ClickHouse: ", e$message)
        data.frame()
      }
    )
  }
  
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
  
  append_new_rows <- function(existing_df, fresh_df, key_cols, table_name) {
    if (nrow(fresh_df) == 0) {
      return(existing_df)
    }
    
    if ("fetched_at" %in% names(existing_df)) {
      existing_df$fetched_at <- as.character(existing_df$fetched_at)
    }
    if ("fetched_at" %in% names(fresh_df)) {
      fresh_df$fetched_at <- as.character(fresh_df$fetched_at)
    }
    
    for (col in key_cols) {
      if (!col %in% names(existing_df)) {
        existing_df[[col]] <- make_typed_na(fresh_df[[col]], nrow(existing_df))
      }
      if (!col %in% names(fresh_df)) {
        fresh_df[[col]] <- make_typed_na(existing_df[[col]], nrow(fresh_df))
      }
    }
    
    fresh_df <- fresh_df %>%
      distinct(across(all_of(key_cols)), .keep_all = TRUE)
    
    new_rows <- if (nrow(existing_df) > 0) {
      anti_join(fresh_df, existing_df, by = key_cols)
    } else {
      fresh_df
    }
    
    if (nrow(new_rows) > 0) {
      message("Новых коммитов для загрузки в ClickHouse: ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = table_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      bind_rows(existing_df, new_rows)
    } else {
      message("Новых коммитов для загрузки не найдено.")
      existing_df
    }
  }
  
  resolve_repos_script <- function() {
    candidate_paths <- c(
      "ETL-Repos.R",
      file.path("R", "ETL-Repos.R"),
      "C:/Users/miron/Documents/GitaRiki/R/ETL-Repos.R"
    )
    existing_path <- candidate_paths[file.exists(candidate_paths)][1]
    if (is.na(existing_path)) {
      stop("Файл ETL-Repos.R не найден.")
    }
    existing_path
  }
  
  repo_owner <- function(repo_full_name) {
    vapply(
      repo_full_name,
      function(x) {
        if (is.na(x) || !nzchar(x)) {
          return(NA_character_)
        }
        strsplit(x, "/", fixed = TRUE)[[1]][1]
      },
      character(1)
    )
  }
  
  collect_event_repos <- function(username, token = NULL, max_pages = 3) {
    repo_names <- character()
    
    for (page in seq_len(max_pages)) {
      events <- tryCatch({
        gh::gh(
          "GET /users/{username}/events/public",
          username = username,
          per_page = 100,
          page = page,
          .token = token,
          .progress = FALSE
        )
      }, error = function(e) {
        warning("Не удалось получить публичные события пользователя ", username, ": ", e$message)
        NULL
      })
      
      if (is.null(events) || length(events) == 0) {
        break
      }
      
      page_repos <- vapply(
        events,
        function(evt) evt$repo$name %||% NA_character_,
        character(1)
      )
      
      repo_names <- c(repo_names, page_repos)
    }
    
    unique(repo_names[!is.na(repo_names) & nzchar(repo_names)])
  }
  
  collect_contributed_repos <- function(username, token = NULL) {
    if (is.null(token) || !nzchar(token)) {
      return(character())
    }
    
    query <- paste(
      "query($login: String!, $cursor: String) {",
      "  user(login: $login) {",
      "    repositoriesContributedTo(",
      "      first: 100,",
      "      after: $cursor,",
      "      includeUserRepositories: true,",
      "      contributionTypes: [COMMIT]",
      "    ) {",
      "      nodes {",
      "        nameWithOwner",
      "      }",
      "      pageInfo {",
      "        hasNextPage",
      "        endCursor",
      "      }",
      "    }",
      "  }",
      "}",
      sep = "\n"
    )
    
    repo_names <- character()
    cursor <- NULL
    
    repeat {
      response <- tryCatch({
        httr::POST(
          url = "https://api.github.com/graphql",
          httr::add_headers(
            Authorization = paste("bearer", token),
            Accept = "application/vnd.github+json"
          ),
          encode = "json",
          body = list(
            query = query,
            variables = list(
              login = username,
              cursor = cursor
            )
          )
        )
      }, error = function(e) {
        warning("Не удалось получить список репозиториев с contributions для пользователя ", username, ": ", e$message)
        NULL
      })
      
      if (is.null(response)) {
        break
      }
      
      payload <- tryCatch(
        httr::content(response, as = "parsed", type = "application/json", encoding = "UTF-8"),
        error = function(e) NULL
      )
      
      if (is.null(payload) || !is.null(payload$errors) || is.null(payload$data$user)) {
        break
      }
      
      repo_block <- payload$data$user$repositoriesContributedTo
      nodes <- repo_block$nodes %||% list()
      if (length(nodes) > 0) {
        repo_names <- c(
          repo_names,
          vapply(nodes, function(node) node$nameWithOwner %||% NA_character_, character(1))
        )
      }
      
      page_info <- repo_block$pageInfo
      if (is.null(page_info) || !isTRUE(page_info$hasNextPage)) {
        break
      }
      
      cursor <- page_info$endCursor %||% NULL
      if (is.null(cursor) || !nzchar(cursor)) {
        break
      }
    }
    
    unique(repo_names[!is.na(repo_names) & nzchar(repo_names)])
  }
  
  collect_commits_from_repo <- function(repo_full, username, token = NULL, include_stats = FALSE) {
    repo_commits <- data.frame()
    
    search_items <- tryCatch({
      search_result <- gh::gh(
        "GET /search/commits",
        q = paste0("author:", username, " repo:", repo_full),
        per_page = 100,
        .limit = Inf,
        .token = token,
        .progress = FALSE
      )
      search_result$items %||% list()
    }, error = function(e) {
      NULL
    })
    
    if (length(search_items) > 0) {
      repo_commits <- map_df(search_items, function(cmt) {
        data.frame(
          repository = repo_full,
          sha = cmt$sha %||% NA,
          date = cmt$commit$author$date %||% NA,
          message = cmt$commit$message %||% NA,
          url = cmt$html_url %||% NA,
          stringsAsFactors = FALSE
        )
      })
    }
    
    if (nrow(repo_commits) == 0) {
      api_commits <- tryCatch({
        gh::gh(
          "GET /repos/{repo_full}/commits",
          repo_full = repo_full,
          author = username,
          .limit = Inf,
          .token = token,
          .progress = FALSE
        )
      }, error = function(e) {
        NULL
      })
      
      if (length(api_commits) > 0) {
        repo_commits <- map_df(api_commits, function(cmt) {
          data.frame(
            repository = repo_full,
            sha = cmt$sha %||% NA,
            date = cmt$commit$author$date %||% NA,
            message = cmt$commit$message %||% NA,
            url = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        })
      }
    }
    
    repo_commits <- repo_commits %>%
      distinct(repository, sha, .keep_all = TRUE)
    
    if (include_stats && nrow(repo_commits) > 0) {
      stats_df <- map_df(repo_commits$sha, function(sha) {
        tryCatch({
          cmt_detail <- gh::gh(
            "GET /repos/{repo_full}/commits/{sha}",
            repo_full = repo_full,
            sha = sha,
            .token = token
          )
          stats <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
          data.frame(
            repository = repo_full,
            sha = sha,
            additions = stats$additions %||% 0,
            deletions = stats$deletions %||% 0,
            changes = stats$total %||% 0,
            stringsAsFactors = FALSE
          )
        }, error = function(e) {
          warning("Не удалось получить статистику для коммита ", sha, " в репозитории ", repo_full, ": ", e$message)
          data.frame(
            repository = repo_full,
            sha = sha,
            additions = NA,
            deletions = NA,
            changes = NA,
            stringsAsFactors = FALSE
          )
        })
      })
      
      repo_commits <- repo_commits %>%
        left_join(stats_df, by = c("repository", "sha"))
    }
    
    repo_commits
  }
  
  source(resolve_repos_script(), local = TRUE)
  
  username <- extract_github_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  table_name <- "github_user_commits"
  escaped_username <- gsub("'", "''", username)
  existing_df <- read_cached_rows(table_name, paste0("profile = '", escaped_username, "'"))
  
  if (nrow(existing_df) > 0) {
    message("В ClickHouse уже есть коммиты для профиля ", username, ": ", nrow(existing_df), " записей.")
  } else {
    message("В ClickHouse пока нет коммитов для профиля ", username, ".")
  }
  
  if (is.null(repos)) {
    repos_df <- get_github_repos(username, token = token, conn = conn)
    candidate_repos <- unique(c(
      repos_df$full_name,
      existing_df$repository,
      collect_event_repos(username, token = token),
      collect_contributed_repos(username, token = token)
    ))
    candidate_repos <- candidate_repos[!is.na(candidate_repos) & nzchar(candidate_repos)]
    
    if (length(candidate_repos) == 0) {
      message("У пользователя не найдено репозиториев-кандидатов для поиска коммитов.")
      return(existing_df)
    }
    
    own_repo_list <- candidate_repos[repo_owner(candidate_repos) == username]
    foreign_repo_list <- candidate_repos[repo_owner(candidate_repos) != username]
    repo_list <- c(own_repo_list, foreign_repo_list)
    
    if (!is.null(max_repos) && max_repos > 0 && max_repos < length(repo_list)) {
      repo_list <- head(repo_list, max_repos)
      message("Ограничено первыми ", max_repos, " репозиториями-кандидатами.")
    }
    
    message(
      "Найдено репозиториев-кандидатов: ", length(repo_list),
      " (свои: ", sum(repo_owner(repo_list) == username, na.rm = TRUE),
      ", чужие: ", sum(repo_owner(repo_list) != username, na.rm = TRUE), ")."
    )
  } else {
    repo_list <- repos
    message("Будут обработаны указанные репозитории (", length(repo_list), " шт.)")
  }
  
  if (include_stats) {
    message("Запрошена статистика изменений. Это может занять дополнительное время.")
  }
  
  all_commits <- list()
  
  for (repo_full in repo_list) {
    message("Обрабатываю репозиторий: ", repo_full)
    
    tryCatch({
      repo_commits <- collect_commits_from_repo(
        repo_full = repo_full,
        username = username,
        token = token,
        include_stats = include_stats
      )
      
      if (nrow(repo_commits) > 0) {
        all_commits[[repo_full]] <- repo_commits
        message("  Найдено коммитов: ", nrow(repo_commits))
      } else {
        message("  Коммитов не найдено.")
      }
    }, error = function(e) {
      if (grepl("409", e$message)) {
        message("  Репозиторий пуст. Пропускаю.")
      } else {
        warning("Ошибка при обработке репозитория ", repo_full, ": ", e$message)
      }
    })
  }
  
  if (length(all_commits) == 0) {
    message("Коммиты не найдены ни в одном репозитории.")
    return(existing_df)
  }
  
  commits_df <- bind_rows(all_commits) %>%
    distinct(repository, sha, .keep_all = TRUE) %>%
    mutate(
      profile = username,
      fetched_at = format(Sys.Date(), "%Y-%m-%d")
    )
  
  message("Всего собрано коммитов из GitHub: ", nrow(commits_df))
  
  result <- append_new_rows(
    existing_df = existing_df,
    fresh_df = commits_df,
    key_cols = c("profile", "repository", "sha"),
    table_name = table_name
  )
  
  result %>%
    arrange(repository, date, sha)
}
