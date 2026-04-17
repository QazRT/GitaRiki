# Функция для извлечения социальных ссылок из профиля GitHub
# с использованием ClickHouse как кэша и дозагрузкой новых ссылок.
#
# Требуется предварительная загрузка ClickHouseConnector.R:
#   source("R/ClickHouseConnector.R")

get_github_social_links <- function(profile, token = NULL, conn = NULL) {
  
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.")
  }
  
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
      message("Найдено новых социальных ссылок для загрузки: ", nrow(new_rows))
      load_df_to_clickhouse(
        df = new_rows,
        table_name = table_name,
        conn = conn,
        overwrite = FALSE,
        append = TRUE
      )
      bind_rows(existing_df, new_rows)
    } else {
      message("Новых социальных ссылок не найдено.")
      existing_df
    }
  }
  
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
  
  extract_username_from_url <- function(url, type) {
    if (is.na(url)) return(NA_character_)
    if (type %in% c("twitter", "instagram", "telegram", "vk", "github", "linkedin")) {
      parts <- strsplit(gsub("^https?://[^/]+/", "", url), "/")[[1]]
      if (length(parts) > 0 && parts[1] != "") return(parts[1])
    }
    return(NA_character_)
  }
  
  classify_url <- function(url) {
    if (is.na(url)) return("other")
    if (grepl("twitter\\.com|x\\.com", url)) {
      "twitter"
    } else if (grepl("t\\.me|telegram\\.me", url)) {
      "telegram"
    } else if (grepl("vk\\.com", url)) {
      "vk"
    } else if (grepl("wa\\.me|whatsapp\\.com", url)) {
      "whatsapp"
    } else if (grepl("linkedin\\.com", url)) {
      "linkedin"
    } else if (grepl("instagram\\.com", url)) {
      "instagram"
    } else if (grepl("facebook\\.com", url)) {
      "facebook"
    } else if (grepl("youtube\\.com|youtu\\.be", url)) {
      "youtube"
    } else if (grepl("github\\.com", url)) {
      "github"
    } else {
      "other"
    }
  }
  
  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Не удалось извлечь имя пользователя из: ", profile)
  }
  
  table_name <- "github_social_links"
  escaped_username <- gsub("'", "''", username)
  existing_df <- read_cached_rows(table_name, paste0("profile = '", escaped_username, "'"))
  
  if (nrow(existing_df) > 0) {
    message("В ClickHouse уже есть социальные ссылки профиля ", username, ": ", nrow(existing_df), " записей.")
  } else {
    message("В ClickHouse пока нет социальных ссылок профиля ", username, ".")
  }
  
  message("Получаю информацию о пользователе ", username, " через GitHub API...")
  user_data <- tryCatch({
    gh::gh("GET /users/{username}", username = username, .token = token)
  }, error = function(e) {
    if (nrow(existing_df) > 0) {
      warning("Не удалось обновить социальные ссылки через GitHub API. Возвращаю данные из ClickHouse: ", e$message)
      return(NULL)
    }
    stop("Ошибка при запросе к GitHub API: ", e$message)
  })
  
  if (is.null(user_data)) {
    return(existing_df)
  }
  
  links <- list()
  
  if (!is.null(user_data$blog) && user_data$blog != "") {
    blog <- user_data$blog
    if (grepl("^https?://", blog)) {
      blog_url <- blog
    } else if (grepl("\\.", blog) || grepl("^@?[a-zA-Z0-9_.]+$", blog)) {
      if (grepl("^@?[a-zA-Z0-9_.]+$", blog) && !grepl("\\.", blog)) {
        blog_url <- paste0("https://instagram.com/", gsub("^@", "", blog))
      } else {
        blog_url <- paste0("https://", blog)
      }
    } else {
      blog_url <- NA_character_
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
  
  if (!is.null(user_data$bio) && user_data$bio != "") {
    bio <- user_data$bio
    
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
    
    tg_pattern <- "(?:t\\.me/|telegram\\.me/)([a-zA-Z0-9_]+)"
    tg_matches <- str_match_all(bio, tg_pattern)[[1]]
    if (nrow(tg_matches) > 0) {
      for (i in seq_len(nrow(tg_matches))) {
        links <- append(links, list(data.frame(
          source = "bio_telegram",
          url = paste0("https://t.me/", tg_matches[i, 2]),
          type = "telegram",
          username = tg_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    vk_pattern <- "vk\\.com/([a-zA-Z0-9_.]+)"
    vk_matches <- str_match_all(bio, vk_pattern)[[1]]
    if (nrow(vk_matches) > 0) {
      for (i in seq_len(nrow(vk_matches))) {
        links <- append(links, list(data.frame(
          source = "bio_vk",
          url = paste0("https://vk.com/", vk_matches[i, 2]),
          type = "vk",
          username = vk_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    wa_pattern <- "(?:wa\\.me/|whatsapp\\.com/)([a-zA-Z0-9_/]+)"
    wa_matches <- str_match_all(bio, wa_pattern)[[1]]
    if (nrow(wa_matches) > 0) {
      for (i in seq_len(nrow(wa_matches))) {
        links <- append(links, list(data.frame(
          source = "bio_whatsapp",
          url = paste0("https://wa.me/", wa_matches[i, 2]),
          type = "whatsapp",
          username = wa_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    inst_pattern <- "(?:instagram\\.com/)([a-zA-Z0-9_.]+)"
    inst_matches <- str_match_all(bio, inst_pattern)[[1]]
    if (nrow(inst_matches) > 0) {
      for (i in seq_len(nrow(inst_matches))) {
        links <- append(links, list(data.frame(
          source = "bio_instagram",
          url = paste0("https://instagram.com/", inst_matches[i, 2]),
          type = "instagram",
          username = inst_matches[i, 2],
          stringsAsFactors = FALSE
        )))
      }
    }
    
    if (grepl("instagram", bio, ignore.case = TRUE)) {
      at_inst_pattern <- "@([a-zA-Z0-9_.]+)"
      at_inst_matches <- str_match_all(bio, at_inst_pattern)[[1]]
      if (nrow(at_inst_matches) > 0) {
        for (i in seq_len(nrow(at_inst_matches))) {
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
    
    at_pattern <- "@([a-zA-Z0-9_]+)"
    at_matches <- str_match_all(bio, at_pattern)[[1]]
    if (nrow(at_matches) > 0) {
      for (i in seq_len(nrow(at_matches))) {
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
  
  if (length(links) == 0) {
    fresh_df <- data.frame(
      source = character(),
      url = character(),
      type = character(),
      username = character(),
      profile = character(),
      fetched_at = as.POSIXct(character()),
      stringsAsFactors = FALSE
    )
  } else {
    fresh_df <- bind_rows(links) %>%
      distinct() %>%
      mutate(
        profile = .env$username,
        fetched_at = format(Sys.Date(), "%Y-%m-%d")
      )
  }
  
  result <- append_new_rows(
    existing_df = existing_df,
    fresh_df = fresh_df,
    key_cols = c("profile", "source", "url", "type", "username"),
    table_name = table_name
  )
  
  result %>%
    arrange(source, type, username, url)
}
