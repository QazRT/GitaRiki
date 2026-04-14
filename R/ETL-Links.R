# ETL-Links.R
# Функция для извлечения социальных ссылок из профиля GitHub
# с кэшированием результатов в ClickHouse.
#
# Требуется предварительная загрузка ClickHouseConnector.R:
#   source("R/ClickHouseConnector.R")

get_github_social_links <- function(profile, token = NULL, conn = NULL) {
  
  # Проверяем, что передан объект подключения ClickHouse
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
  # Проверяем наличие необходимых пакетов
  required_packages <- c("gh", "httr", "dplyr", "stringr", "purrr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      stop("Пакет '", pkg, "' не установлен. Установите его вручную: install.packages('", pkg, "')")
    }
  }
  
  library(gh)
  library(httr)
  library(dplyr)
  library(stringr)
  library(purrr)
  
  # Вспомогательная функция для извлечения имени пользователя из профиля (строка или URL)
  extract_username <- function(input) {
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
  
  # Извлекает username из URL социальной сети (последний сегмент пути)
  extract_username_from_url <- function(url, type) {
    if (is.na(url)) return(NA_character_)
    if (type %in% c("twitter", "instagram", "telegram", "vk", "github", "linkedin")) {
      parts <- strsplit(gsub("^https?://[^/]+/", "", url), "/")[[1]]
      if (length(parts) > 0 && parts[1] != "") return(parts[1])
    }
    return(NA_character_)
  }
  
  # Классификация URL по домену
  classify_url <- function(url) {
    if (is.na(url)) return("other")
    if (grepl("twitter\\.com|x\\.com", url)) {
      return("twitter")
    } else if (grepl("t\\.me|telegram\\.me", url)) {
      return("telegram")
    } else if (grepl("vk\\.com", url)) {
      return("vk")
    } else if (grepl("wa\\.me|whatsapp\\.com", url)) {
      return("whatsapp")
    } else if (grepl("linkedin\\.com", url)) {
      return("linkedin")
    } else if (grepl("instagram\\.com", url)) {
      return("instagram")
    } else if (grepl("facebook\\.com", url)) {
      return("facebook")
    } else if (grepl("youtube\\.com|youtu\\.be", url)) {
      return("youtube")
    } else if (grepl("github\\.com", url)) {
      return("github")
    } else {
      return("other")
    }
  }
  
  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  # Имя таблицы в ClickHouse
  table_name <- "github_social_links"
  
  # 1. Проверяем наличие данных в ClickHouse
  escaped_username <- gsub("'", "''", username)
  sql_check <- paste0(
    "SELECT COUNT(*) AS cnt FROM ", table_name,
    " WHERE profile = '", escaped_username, "'"
  )
  
  table_exists <- exists("table_exists_custom", mode = "function") &&
    table_exists_custom(conn, table_name)
  
  if (table_exists) {
    cnt_df <- tryCatch(
      query_clickhouse(conn, sql_check),
      error = function(e) {
        warning("Не удалось проверить наличие данных в ClickHouse: ", e$message)
        data.frame(cnt = 0)
      }
    )
    if (nrow(cnt_df) > 0 && cnt_df$cnt[1] > 0) {
      message("Данные для профиля ", username, " уже есть в ClickHouse. Загружаю из базы...")
      sql_read <- paste0(
        "SELECT * FROM ", table_name,
        " WHERE profile = '", escaped_username, "'"
      )
      result <- query_clickhouse(conn, sql_read)
      return(result)
    }
  }
  
  # 2. Данных нет – запрашиваем GitHub API
  message("Получаю информацию о пользователе ", username, " через GitHub API...")
  user_data <- tryCatch({
    gh::gh("GET /users/{username}", username = username, .token = token)
  }, error = function(e) {
    stop("Ошибка при запросе к GitHub API: ", e$message)
  })
  
  links <- list()
  
  # --- Обработка поля blog ---
  if (!is.null(user_data$blog) && user_data$blog != "") {
    blog <- user_data$blog
    if (grepl("^https?://", blog)) {
      blog_url <- blog
    } else {
      # Пытаемся интерпретировать как осмысленную ссылку
      if (grepl("\\.", blog) || grepl("^@?[a-zA-Z0-9_.]+$", blog)) {
        # Если выглядит как Instagram username (без домена и точек)
        if (grepl("^@?[a-zA-Z0-9_.]+$", blog) && !grepl("\\.", blog)) {
          blog_url <- paste0("https://instagram.com/", gsub("^@", "", blog))
        } else {
          blog_url <- paste0("https://", blog)
        }
      } else {
        blog_url <- NA_character_
      }
    }
    
    if (!is.na(blog_url)) {
      tp <- classify_url(blog_url)
      links <- append(links, list(data.frame(
        source = "blog",
        url = blog_url,
        type = tp,
        username = extract_username_from_url(blog_url, tp),
        stringsAsFactors = FALSE
      )))
    } else {
      links <- append(links, list(data.frame(
        source = "blog",
        url = NA_character_,
        type = "text",
        username = NA_character_,
        stringsAsFactors = FALSE
      )))
    }
  }
  
  # --- Поле twitter_username ---
  if (!is.null(user_data$twitter_username) && user_data$twitter_username != "") {
    tw <- user_data$twitter_username
    links <- append(links, list(data.frame(
      source = "twitter",
      url = paste0("https://twitter.com/", tw),
      type = "twitter",
      username = tw,
      stringsAsFactors = FALSE
    )))
  }
  
  # --- Обработка bio ---
  if (!is.null(user_data$bio) && user_data$bio != "") {
    bio <- user_data$bio
    
    # Полные URL (http/https)
    url_pattern <- "https?://[^\\s]+"
    urls <- str_extract_all(bio, url_pattern)[[1]]
    if (length(urls) > 0) {
      for (u in urls) {
        tp <- classify_url(u)
        links <- append(links, list(data.frame(
          source = "bio_url",
          url = u,
          type = tp,
          username = extract_username_from_url(u, tp),
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # Telegram: t.me/username или telegram.me/username
    tg_pattern <- "(?:t\\.me/|telegram\\.me/)([a-zA-Z0-9_]+)"
    tg_matches <- str_match_all(bio, tg_pattern)[[1]]
    if (nrow(tg_matches) > 0) {
      for (i in 1:nrow(tg_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_telegram",
          url = paste0("https://t.me/", tg_matches[i, 2]),
          type = "telegram",
          username = tg_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # VK: vk.com/id123 или vk.com/username
    vk_pattern <- "vk\\.com/([a-zA-Z0-9_.]+)"
    vk_matches <- str_match_all(bio, vk_pattern)[[1]]
    if (nrow(vk_matches) > 0) {
      for (i in 1:nrow(vk_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_vk",
          url = paste0("https://vk.com/", vk_matches[i, 2]),
          type = "vk",
          username = vk_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # WhatsApp: wa.me/... или whatsapp.com/...
    wa_pattern <- "(?:wa\\.me/|whatsapp\\.com/)([a-zA-Z0-9_/]+)"
    wa_matches <- str_match_all(bio, wa_pattern)[[1]]
    if (nrow(wa_matches) > 0) {
      for (i in 1:nrow(wa_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_whatsapp",
          url = paste0("https://wa.me/", wa_matches[i, 2]),
          type = "whatsapp",
          username = wa_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # Instagram: instagram.com/username
    inst_pattern <- "(?:instagram\\.com/)([a-zA-Z0-9_.]+)"
    inst_matches <- str_match_all(bio, inst_pattern)[[1]]
    if (nrow(inst_matches) > 0) {
      for (i in 1:nrow(inst_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_instagram",
          url = paste0("https://instagram.com/", inst_matches[i, 2]),
          type = "instagram",
          username = inst_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    # Если в bio упоминается Instagram и есть @username
    if (grepl("instagram", bio, ignore.case = TRUE)) {
      at_inst_pattern <- "@([a-zA-Z0-9_.]+)"
      at_inst_matches <- str_match_all(bio, at_inst_pattern)[[1]]
      if (nrow(at_inst_matches) > 0) {
        for (i in 1:nrow(at_inst_matches)) {
          links <- append(links, list(data.frame(
            source = "bio_instagram_mention",
            url = paste0("https://instagram.com/", at_inst_matches[i, 2]),
            type = "instagram",
            username = at_inst_matches[i, 2],
            stringsAsFactors = FALSE
          )))
        }
      }
    }
    
    # Общие упоминания @username (без привязки к конкретной соцсети)
    at_pattern <- "@([a-zA-Z0-9_]+)"
    at_matches <- str_match_all(bio, at_pattern)[[1]]
    if (nrow(at_matches) > 0) {
      for (i in 1:nrow(at_matches)) {
        links <- append(links, list(data.frame(
          source = "bio_mention",
          url = NA_character_,
          type = "mention",
          username = at_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
  }
  
  # Сборка результата
  if (length(links) == 0) {
    message("Не найдено ни одной ссылки на другие профили.")
    result <- data.frame()
  } else {
    result <- bind_rows(links) %>%
      distinct()
  }
  
  # Добавляем служебные колонки
  if (nrow(result) > 0) {
    result <- result %>%
      mutate(
        profile = username,
        fetched_at = Sys.time()
      )
  } else {
    result <- data.frame(
      source = character(),
      url = character(),
      type = character(),
      username = character(),
      profile = character(),
      fetched_at = as.POSIXct(character()),
      stringsAsFactors = FALSE
    )
  }
  
  # 3. Загрузка в ClickHouse
  if (nrow(result) > 0) {
    message("Загружаю социальные ссылки в ClickHouse...")
    load_df_to_clickhouse(
      df = result,
      table_name = table_name,
      conn = conn,
      overwrite = FALSE,
      append = TRUE
    )
  }
  
  return(result)
}