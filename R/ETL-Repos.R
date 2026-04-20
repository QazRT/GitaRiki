# Функция для получения списка репозиториев пользователя GitHub
# с использованием ClickHouse как кэша и дозагрузкой новых данных.

get_github_repos <- function(input, token = NULL, conn = NULL) {
  
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  required_packages <- c("gh", "httr", "dplyr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите вручную: install.packages('", pkg, "')")
    }
  }
  library(gh)
  library(httr)
  library(dplyr)
  
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
      message("Найдено новых записей для загрузки в ClickHouse: ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = table_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      bind_rows(existing_df, new_rows)
    } else {
      message("Новых записей для загрузки в ClickHouse не найдено.")
      existing_df
    }
  }
  
  username <- extract_github_username(input)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", input)
  }
  
  table_name <- "github_repos"
  escaped_username <- gsub("'", "''", username)
  existing_df <- read_cached_rows(table_name, paste0("profile = '", escaped_username, "'"))
  
  if (nrow(existing_df) > 0) {
    message("В ClickHouse уже есть данные по репозиториям для профиля ", username, ": ", nrow(existing_df), " записей.")
  } else {
    message("В ClickHouse пока нет репозиториев для профиля ", username, ".")
  }
  
  message("Запрашиваю репозитории пользователя ", username, " через GitHub API...")
  repos <- tryCatch(
    gh::gh(
      "GET /users/{username}/repos",
      username = username,
      .limit = Inf,
      .token = token,
      .progress = TRUE
    ),
    error = function(e) {
      if (nrow(existing_df) > 0) {
        warning("Не удалось обновить репозитории через GitHub API. Возвращаю данные из ClickHouse: ", e$message)
        return(NULL)
      }
      stop("Ошибка при запросе к GitHub API: ", e$message)
    }
  )
  
  if (is.null(repos)) {
    return(existing_df)
  }
  
  if (length(repos) == 0) {
    message("Репозитории не найдены.")
    return(existing_df)
  }
  
  repos_df <- bind_rows(lapply(repos, function(repo) {
    data.frame(
      name             = repo$name %||% NA,
      full_name        = repo$full_name %||% NA,
      description      = repo$description %||% NA,
      html_url         = repo$html_url %||% NA,
      ssh_url          = repo$ssh_url %||% NA,
      language         = repo$language %||% NA,
      stargazers_count = repo$stargazers_count %||% 0,
      forks_count      = repo$forks_count %||% 0,
      open_issues      = repo$open_issues_count %||% 0,
      created_at       = repo$created_at %||% NA,
      updated_at       = repo$updated_at %||% NA,
      pushed_at        = repo$pushed_at %||% NA,
      size             = repo$size %||% 0,
      stringsAsFactors = FALSE
    )
  })) %>%
    mutate(
      profile = username,
      fetched_at = format(Sys.Date(), "%Y-%m-%d")
    )
  
  message("Получено репозиториев из GitHub: ", nrow(repos_df))
  
  result <- append_new_rows(
    existing_df = existing_df,
    fresh_df = repos_df,
    key_cols = c("profile", "full_name"),
    table_name = table_name
  )
  
  result %>%
    arrange(full_name)
}

# Вспомогательная функция для извлечения имени пользователя из URL или строки
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

# Оператор %||% для удобства
`%||%` <- function(x, y) if (is.null(x)) y else x
