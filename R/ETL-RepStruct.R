# Функция для получения структуры файлов и папок репозитория GitHub
# с использованием ClickHouse как кэша и дозагрузкой новых элементов.

# Предполагается, что файл ClickHouseConnector.R уже загружен через source()
# и доступны функции: query_clickhouse, load_df_to_clickhouse, table_exists_custom.

get_repo_structure <- function(repo,
                               token = NULL,
                               recursive = FALSE,
                               get_content = FALSE,
                               ref = NULL,
                               conn = NULL) {
  
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  if (!requireNamespace("gh", quietly = TRUE)) stop("Пакет 'gh' не установлен.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Пакет 'dplyr' не установлен.")
  if (!requireNamespace("purrr", quietly = TRUE)) stop("Пакет 'purrr' не установлен.")
  if (!requireNamespace("httr", quietly = TRUE)) stop("Пакет 'httr' не установлен.")
  if (!requireNamespace("base64enc", quietly = TRUE)) stop("Пакет 'base64enc' не установлен.")
  
  library(gh)
  library(dplyr)
  library(purrr)
  library(httr)
  library(base64enc)
  
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
        warning("Не удалось загрузить структуру из ClickHouse: ", e$message)
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
      message("Найдено новых элементов структуры для загрузки: ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = table_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      bind_rows(existing_df, new_rows)
    } else {
      message("Новых элементов структуры не найдено.")
      existing_df
    }
  }
  
  parse_repo <- function(input) {
    if (grepl("^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$", input)) {
      parts <- strsplit(input, "/")[[1]]
      return(list(owner = parts[1], repo = parts[2]))
    }
    parsed <- httr::parse_url(input)
    if (!is.null(parsed$path)) {
      path_parts <- strsplit(gsub("^/|/$", "", parsed$path), "/")[[1]]
      if (length(path_parts) >= 2) {
        return(list(owner = path_parts[1], repo = path_parts[2]))
      }
    }
    stop("Не удалось распарсить репозиторий: ", input, ". Используйте формат 'owner/repo' или ссылку.")
  }
  
  repo_info <- parse_repo(repo)
  owner <- repo_info$owner
  repo_name <- repo_info$repo
  table_name <- "github_repo_structure"
  
  escaped_owner <- gsub("'", "''", owner)
  escaped_repo <- gsub("'", "''", repo_name)
  where_sql <- paste0(
    "owner = '", escaped_owner, "'",
    " AND repo = '", escaped_repo, "'",
    if (is.null(ref)) {
      " AND isNull(ref)"
    } else {
      paste0(" AND ref = '", gsub("'", "''", ref), "'")
    }
  )
  
  existing_df <- read_cached_rows(table_name, where_sql)
  if (nrow(existing_df) > 0) {
    message("В ClickHouse уже есть структура для ", owner, "/", repo_name, ": ", nrow(existing_df), " записей.")
  } else {
    message("В ClickHouse пока нет структуры для ", owner, "/", repo_name, ".")
  }
  
  message("Получаю структуру репозитория ", owner, "/", repo_name, " через GitHub API...")
  
  fetch_contents <- function(path = "") {
    endpoint <- if (path == "") {
      "GET /repos/{owner}/{repo}/contents"
    } else {
      "GET /repos/{owner}/{repo}/contents/{path}"
    }
    
    contents <- tryCatch({
      gh::gh(
        endpoint,
        owner = owner,
        repo = repo_name,
        path = path,
        ref = ref,
        .token = token,
        .limit = Inf
      )
    }, error = function(e) {
      if (nrow(existing_df) > 0) {
        warning("Не удалось обновить структуру через GitHub API. Возвращаю данные из ClickHouse: ", e$message)
        return(NULL)
      }
      stop("Ошибка при запросе ", path, ": ", e$message)
    })
    
    if (is.null(contents)) {
      return(NULL)
    }
    
    if (!is.list(contents) || is.null(names(contents))) {
      items <- contents
    } else {
      items <- list(contents)
    }
    
    items_df <- map_df(items, function(item) {
      base <- data.frame(
        path = item$path %||% NA,
        type = item$type %||% NA,
        size = ifelse(item$type == "file", item$size %||% 0, NA),
        sha = item$sha %||% NA,
        download_url = ifelse(item$type == "file", item$download_url %||% NA, NA),
        stringsAsFactors = FALSE
      )
      
      if (get_content && item$type == "file") {
        if (!is.null(item$content)) {
          encoded <- gsub("\n", "", item$content)
          decoded <- tryCatch({
            rawToChar(base64decode(encoded))
          }, error = function(e) {
            warning("Не удалось декодировать содержимое файла ", item$path, ": ", e$message)
            NA_character_
          })
          base$content <- decoded
        } else {
          base$content <- NA_character_
        }
      } else {
        base$content <- NA_character_
      }
      
      base
    })
    
    if (recursive) {
      dirs <- items_df %>% filter(type == "dir")
      if (nrow(dirs) > 0) {
        sub_dfs <- map(dirs$path, fetch_contents)
        sub_dfs <- compact(sub_dfs)
        if (length(sub_dfs) > 0) {
          items_df <- bind_rows(items_df, bind_rows(sub_dfs))
        }
      }
    }
    
    items_df
  }
  
  fresh_df <- fetch_contents("")
  if (is.null(fresh_df)) {
    return(existing_df)
  }
  
  fresh_df <- fresh_df %>%
    mutate(
      owner = owner,
      repo = repo_name,
      ref = if (is.null(ref)) NA_character_ else ref,
      fetched_at = format(Sys.Date(), "%Y-%m-%d")
    )
  
  result <- append_new_rows(
    existing_df = existing_df,
    fresh_df = fresh_df,
    key_cols = c("owner", "repo", "ref", "path", "sha", "type"),
    table_name = table_name
  )
  
  result %>%
    arrange(path, sha)
}
