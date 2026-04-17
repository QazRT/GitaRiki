# Функция для получения коммитов пользователя GitHub
# с использованием ClickHouse как кэша и дозагрузкой новых коммитов.

get_user_commits <- function(profile,
                             token = NULL,
                             repos = NULL,
                             max_repos = NULL,
                             include_stats = FALSE,
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
  
  append_new_rows <- function(existing_df, fresh_df, key_cols, table_name) {
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
    
    if (nrow(existing_df) > 0) {
      new_rows <- anti_join(fresh_df, existing_df, by = key_cols)
    } else {
      new_rows <- fresh_df
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
    if (nrow(repos_df) == 0) {
      message("У пользователя нет публичных репозиториев.")
      return(existing_df)
    }
    if (!is.null(max_repos) && max_repos > 0 && max_repos < nrow(repos_df)) {
      repos_df <- head(repos_df, max_repos)
      message("Ограничено первыми ", max_repos, " репозиториями.")
    }
    repo_list <- repos_df$full_name
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
      commits <- gh::gh(
        "GET /repos/{repo_full}/commits",
        repo_full = repo_full,
        author = username,
        .limit = Inf,
        .token = token,
        .progress = FALSE
      )
      
      if (length(commits) > 0) {
        repo_commits <- map_df(commits, function(cmt) {
          data.frame(
            repository = repo_full,
            sha        = cmt$sha %||% NA,
            date       = cmt$commit$author$date %||% NA,
            message    = cmt$commit$message %||% NA,
            url        = cmt$html_url %||% NA,
            stringsAsFactors = FALSE
          )
        })
        
        if (include_stats && nrow(repo_commits) > 0) {
          message("  Запрашиваю статистику для ", nrow(repo_commits), " коммитов...")
          stats <- map_df(repo_commits$sha, function(sha) {
            tryCatch({
              cmt_detail <- gh::gh(
                "GET /repos/{repo_full}/commits/{sha}",
                repo_full = repo_full,
                sha = sha,
                .token = token
              )
              stats <- cmt_detail$stats %||% list(additions = 0, deletions = 0, total = 0)
              data.frame(
                sha = sha,
                additions = stats$additions %||% 0,
                deletions = stats$deletions %||% 0,
                changes   = stats$total %||% 0,
                stringsAsFactors = FALSE
              )
            }, error = function(e) {
              warning("Не удалось получить статистику для коммита ", sha, ": ", e$message)
              data.frame(
                sha = sha,
                additions = NA,
                deletions = NA,
                changes = NA,
                stringsAsFactors = FALSE
              )
            })
          })
          repo_commits <- repo_commits %>%
            left_join(stats, by = "sha")
        }
        
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
