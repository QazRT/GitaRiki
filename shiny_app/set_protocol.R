find_githound_project_root <- function() {
  candidates <- c(
    getwd(),
    dirname(normalizePath(getwd(), mustWork = FALSE)),
    normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
  )

  for (candidate in unique(candidates)) {
    if (file.exists(file.path(candidate, "R", "Analysis-Runner.R"))) {
      return(candidate)
    }
  }

  getwd()
}

load_set_protocol_dependencies <- function() {
  root <- find_githound_project_root()
  files <- c(
    "R/ClickHouseConnector.R",
    "R/MCP-OpenAI.R",
    "R/ETL-Repos.R",
    "R/ETL-Commit.R",
    "R/ETL-Links.R",
    "R/ETL-ForeignDevelop.R",
    "R/ETL-UserStats.R",
    "R/ETL-UserProfileNativeStats.R",
    "R/ETL-UserComments.R",
    "R/ETL-UserActivityTimeline.R",
    "R/OSV-utils.R",
    "R/OSV-search.R",
    "R/OSV-database.R",
    "R/OSV-mongo.R",
    "R/Syft-sbom.R",
    "R/Commit-dependency-analysis.R",
    "R/Analysis-Helpers.R",
    "R/Analysis-Activity.R",
    "R/Analysis-Popularity.R",
    "R/Analysis-Runner.R"
  )

  for (file in files) {
    path <- file.path(root, file)
    if (file.exists(path)) {
      source(path, encoding = "UTF-8")
    }
  }

  invisible(TRUE)
}

set_metric <- function(df, metric, default = NA) {
  if (!is.data.frame(df) || !"metric" %in% names(df) || !"value" %in% names(df)) {
    return(default)
  }
  row <- df[df$metric == metric, , drop = FALSE]
  if (nrow(row) == 0L) {
    return(default)
  }
  row$value[[1]]
}

set_count <- function(x) {
  if (is.data.frame(x)) nrow(x) else 0L
}

set_value <- function(x, default = "—") {
  if (is.null(x) || length(x) == 0L) {
    return(default)
  }
  value <- x[[1]]
  if (is.null(value) || is.na(value) || !nzchar(as.character(value))) {
    return(default)
  }
  as.character(value)
}

set_compact_cell <- function(x, max_chars = 90L) {
  value <- paste(as.character(unlist(x, use.names = FALSE)), collapse = ", ")
  value <- gsub("[\r\n\t]+", " ", value)
  value <- gsub("\\s{2,}", " ", value)
  value <- trimws(value)
  if (!nzchar(value) || is.na(value)) {
    return("—")
  }
  if (nchar(value, type = "width") > max_chars) {
    value <- paste0(substr(value, 1L, max_chars - 1L), "…")
  }
  value
}

set_prepare_table <- function(df, cols = NULL, ru_names = NULL, limit = 8L, max_chars = 90L) {
  if (!is.data.frame(df) || nrow(df) == 0L) {
    return(data.frame())
  }

  if (!is.null(cols)) {
    cols <- intersect(cols, names(df))
    if (length(cols) > 0L) {
      df <- df[, cols, drop = FALSE]
    }
  }

  df <- utils::head(df, limit)
  for (col in names(df)) {
    df[[col]] <- vapply(df[[col]], set_compact_cell, character(1), max_chars = max_chars)
  }

  if (!is.null(ru_names)) {
    ru_names <- ru_names[seq_len(min(length(ru_names), ncol(df)))]
    names(df)[seq_along(ru_names)] <- ru_names
  }

  df
}

set_metric_labels <- c(
  metric = "Показатель",
  value = "Значение",
  total_commits = "Всего коммитов",
  active_days = "Активных дней",
  first_commit = "Первый коммит",
  last_commit = "Последний коммит",
  commit_span_days = "Период активности, дней",
  commits_per_active_day = "Коммитов за активный день",
  total_repositories = "Репозитории",
  own_repositories = "Свои репозитории",
  external_repositories = "Внешние репозитории",
  total_stars_received = "Получено звезд",
  total_forks_received = "Получено форков",
  popularity_index = "Индекс популярности",
  social_links = "Социальные ссылки",
  profile_links = "Ссылки профиля",
  commit_links = "Ссылки из коммитов",
  comment_links = "Ссылки из комментариев"
)

set_metric_table <- function(df, limit = 12L) {
  if (!is.data.frame(df) || nrow(df) == 0L) {
    return(data.frame())
  }
  if (!all(c("metric", "value") %in% names(df))) {
    return(set_prepare_table(df, limit = limit))
  }

  out <- df[, c("metric", "value"), drop = FALSE]
  out$metric <- vapply(out$metric, function(metric) {
    label <- unname(set_metric_labels[as.character(metric)])
    if (length(label) == 0L || is.na(label)) as.character(metric) else label
  }, character(1))
  set_prepare_table(out, ru_names = c("Показатель", "Значение"), limit = limit, max_chars = 110L)
}

set_language_stats_from_repos <- function(owned_repos) {
  if (!is.data.frame(owned_repos) || nrow(owned_repos) == 0L || !"language" %in% names(owned_repos)) {
    return(data.frame())
  }

  repos <- owned_repos
  repos$language <- as.character(repos$language)
  repos <- repos[!is.na(repos$language) & nzchar(repos$language), , drop = FALSE]
  if (nrow(repos) == 0L) {
    return(data.frame())
  }

  value_or_zero <- function(df, col) {
    if (col %in% names(df)) suppressWarnings(as.numeric(df[[col]])) else rep(0, nrow(df))
  }

  repos$repo_size_kb_for_report <- value_or_zero(repos, "size")
  repos$repo_stars_for_report <- value_or_zero(repos, "stargazers_count")
  repos$repo_forks_for_report <- value_or_zero(repos, "forks_count")

  out <- aggregate(
    cbind(repo_size_kb_for_report, repo_stars_for_report, repo_forks_for_report) ~ language,
    data = repos,
    FUN = sum,
    na.rm = TRUE
  )
  counts <- aggregate(rep_id ~ language, data = transform(repos, rep_id = 1L), FUN = length)
  out <- merge(counts, out, by = "language", all.x = TRUE)
  names(out) <- c("language", "repositories", "total_repo_size_kb", "total_repo_stars", "total_repo_forks")
  out <- out[order(-out$repositories, out$language), , drop = FALSE]
  rownames(out) <- NULL
  out
}

set_find_osv_db <- function(root) {
  candidates <- unique(c(
    file.path(root, "osv_db"),
    file.path(root, "R", "osv_db"),
    file.path(getwd(), "osv_db")
  ))

  for (candidate in candidates) {
    if (dir.exists(file.path(candidate, "raw")) && file.exists(file.path(candidate, "osv_index.csv"))) {
      return(normalizePath(candidate, winslash = "/", mustWork = FALSE))
    }
  }
  NA_character_
}

set_find_syft <- function(root) {
  candidates <- unique(c(
    file.path(root, "syft.exe"),
    file.path(getwd(), "syft.exe"),
    unname(Sys.which("syft")),
    unname(Sys.which("syft.exe"))
  ))
  candidates <- candidates[nzchar(candidates)]

  for (candidate in candidates) {
    if (file.exists(candidate)) {
      return(normalizePath(candidate, winslash = "/", mustWork = FALSE))
    }
  }
  NA_character_
}

set_extract_named_string <- function(text, name) {
  pattern <- paste0(name, "\\s*=\\s*['\"]([^'\"]+)['\"]")
  match <- regexec(pattern, text, perl = TRUE)
  value <- regmatches(text, match)[[1]]
  if (length(value) >= 2L) value[[2]] else NA_character_
}

set_read_osv_config_file <- function() {
  candidates <- unique(c(
    Sys.getenv("OSV_CONFIG_R", ""),
    file.path(Sys.getenv("USERPROFILE", ""), "Downloads", "usecase.R"),
    file.path(Sys.getenv("HOME", ""), "Downloads", "usecase.R")
  ))
  candidates <- candidates[nzchar(candidates) & file.exists(candidates)]

  for (path in candidates) {
    text <- tryCatch(paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n"), error = function(e) "")
    if (!nzchar(text) || !grepl("load_osv_mongo_database", text, fixed = TRUE)) {
      next
    }

    mongo_url <- set_extract_named_string(text, "mongo_url")
    db_name <- set_extract_named_string(text, "db_name")
    collection <- set_extract_named_string(text, "collection")

    if (nzchar(mongo_url %||% "")) {
      return(list(
        mongo_url = mongo_url,
        db_name = if (nzchar(db_name %||% "")) db_name else "osv",
        collection = if (nzchar(collection %||% "")) collection else "vulns",
        source = normalizePath(path, winslash = "/", mustWork = FALSE)
      ))
    }
  }

  list()
}

set_load_osv_backend <- function(root) {
  config_file <- set_read_osv_config_file()
  mongo_url <- Sys.getenv("OSV_MONGO_URL", "")
  if (!nzchar(mongo_url)) mongo_url <- Sys.getenv("MONGO_URL", "")
  if (!nzchar(mongo_url)) mongo_url <- Sys.getenv("MONGODB_URI", "")
  if (!nzchar(mongo_url)) mongo_url <- Sys.getenv("MONGO_URI", "")
  if (!nzchar(mongo_url)) mongo_url <- config_file$mongo_url %||% ""
  if (!nzchar(mongo_url)) mongo_url <- "mongodb://localhost:27017"

  mongo_db <- Sys.getenv("OSV_MONGO_DB", "")
  if (!nzchar(mongo_db)) mongo_db <- config_file$db_name %||% "osv"

  mongo_collection <- Sys.getenv("OSV_MONGO_COLLECTION", "")
  if (!nzchar(mongo_collection)) mongo_collection <- config_file$collection %||% "vulns"
  mongo_error <- NULL

  if (!requireNamespace("mongolite", quietly = TRUE)) {
    mongo_error <- "Package 'mongolite' is required for MongoDB backend. Установите пакет mongolite в R."
  } else if (exists("load_osv_mongo_database", mode = "function") && nzchar(mongo_url)) {
    mongo_result <- tryCatch({
      list(
        db = load_osv_mongo_database(
          mongo_url = mongo_url,
          db_name = mongo_db,
          collection = mongo_collection,
          validate = TRUE
        ),
        message = paste0("База OSV подключена через MongoDB: ", mongo_db, ".", mongo_collection)
      )
    }, error = function(e) {
      list(error = conditionMessage(e))
    })

    if (is.list(mongo_result) && !is.null(mongo_result$db)) {
      return(mongo_result)
    }
    mongo_error <- mongo_result$error
  }

  osv_dir <- set_find_osv_db(root)
  if (nzchar(osv_dir) && !is.na(osv_dir)) {
    local_result <- tryCatch({
      list(
        db = load_osv_database(osv_dir, load_index = TRUE),
        message = "База OSV подключена из локальной папки osv_db."
      )
    }, error = function(e) {
      list(error = conditionMessage(e))
    })

    if (is.list(local_result) && !is.null(local_result$db)) {
      return(local_result)
    }
  }

  list(
    db = NULL,
    message = "База OSV не подключена. Проверьте MongoDB OSV или локальную папку osv_db.",
    error = mongo_error
  )
}

run_set_vulnerability_scan <- function(profile, token, conn = NULL, progress = function(value, label = NULL) NULL) {
  root <- find_githound_project_root()
  syft_path <- set_find_syft(root)

  if (!exists("analyze_user_commits_with_syft_osv", mode = "function")) {
    return(list(status = "skipped", message = "Функции Syft/OSV не загружены."))
  }
  if (!nzchar(syft_path) || is.na(syft_path)) {
    return(list(status = "skipped", message = "Syft не найден. Добавьте syft.exe в корень проекта или PATH."))
  }

  progress(76, "Подключение базы уязвимостей OSV")
  osv_backend <- set_load_osv_backend(root)
  if (is.null(osv_backend$db)) {
    message <- osv_backend$message
    if (nzchar(osv_backend$error %||% "")) {
      message <- paste(message, osv_backend$error)
    }
    return(list(status = "connection_error", message = message))
  }

  tryCatch({
    progress(82, "Поиск уязвимостей зависимостей")
    scan_args <- list(
      profile = profile,
      osv_db = osv_backend$db,
      token = token,
      syft_path = syft_path,
      include_uncertain = TRUE,
      keep_snapshots = FALSE,
      force_scan = FALSE,
      parallel_strategy = "auto",
      auto_workers = 16L,
      repo_progress = TRUE,
      repo_progress_every = 10L,
      repo_progress_details = FALSE,
      syft_timeout_sec = 0L,
      commit_info_parallel = TRUE,
      commit_info_workers = 4L,
      conn = conn,
      clickhouse_incremental = TRUE,
      incremental_compare_commits_by_sha = TRUE,
      debug = TRUE,
      debug_repo_every = 10L
    )
    supported_args <- names(formals(analyze_user_commits_with_syft_osv))
    scan <- do.call(
      analyze_user_commits_with_syft_osv,
      scan_args[names(scan_args) %in% supported_args]
    )

    diagnostics <- if (exists("diagnose_commit_scan_result", mode = "function")) {
      diagnose_commit_scan_result(scan)
    } else {
      list()
    }

    list(status = "ok", message = paste("Поиск уязвимостей выполнен.", osv_backend$message), scan = scan, diagnostics = diagnostics)
  }, error = function(e) {
    list(status = "error", message = conditionMessage(e))
  })
}

set_collect_plot_paths <- function(analysis) {
  if (!is.list(analysis)) {
    return(character())
  }

  candidates <- unlist(lapply(
    analysis[c("activity", "popularity", "ownership", "social")],
    function(section) {
      if (is.list(section) && !is.null(section$saved_plots)) section$saved_plots else character()
    }
  ), use.names = FALSE)

  candidates <- candidates[file.exists(candidates)]
  unique(candidates)
}

set_plot_label <- function(path) {
  name <- basename(path)
  labels <- c(
    activity_top_repositories = "Топ репозиториев по коммитам",
    activity_timeline = "Динамика коммитов",
    popularity_top_by_stars = "Топ репозиториев по звездам",
    popularity_stars_vs_forks = "Звезды и форки",
    ownership_commit_split = "Свои и внешние репозитории",
    ownership_monthly_split = "Помесячная активность",
    ownership_top_external_repos = "Топ внешних репозиториев",
    social_type_distribution = "Типы социальных ссылок"
  )

  for (key in names(labels)) {
    if (grepl(key, name, fixed = TRUE)) {
      return(unname(labels[[key]]))
    }
  }
  tools::file_path_sans_ext(name)
}

set_vulnerability_sections <- function(vulnerability_scan) {
  if (!is.list(vulnerability_scan) || !identical(vulnerability_scan$status, "ok")) {
    status_label <- switch(
      vulnerability_scan$status %||% "skipped",
      connection_error = "ошибка подключения",
      skipped = "пропущено",
      error = "ошибка анализа",
      vulnerability_scan$status %||% "пропущено"
    )
    return(list(list(
      title = "Уязвимости зависимостей",
      text = "Сет попытался запустить локальный поиск уязвимостей через Syft и OSV.",
      table = data.frame(
        "Показатель" = c("Статус", "Комментарий"),
        "Значение" = c(status_label, vulnerability_scan$message %||% "Проверка не выполнена."),
        check.names = FALSE,
        stringsAsFactors = FALSE
      )
    )))
  }

  scan <- vulnerability_scan$scan
  diagnostics <- vulnerability_scan$diagnostics %||% list()

  summary <- data.frame(
    "Показатель" = c(
      "Проверено коммитов",
      "Просканировано коммитов",
      "Найдено строк уязвимостей",
      "Открытых уязвимостей",
      "Закрытых уязвимостей"
    ),
    "Значение" = c(
      diagnostics$total_commits %||% set_count(scan$summary),
      diagnostics$scanned_commits %||% 0,
      diagnostics$vulnerability_rows %||% set_count(scan$vulnerabilities),
      diagnostics$open_vulnerabilities %||% 0,
      diagnostics$fixed_vulnerabilities %||% 0
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  sections <- list(list(
    title = "Уязвимости зависимостей",
    text = "Локальная проверка зависимостей через Syft и базу OSV без обращения к ИИ.",
    table = summary
  ))

  if (is.data.frame(scan$vulnerabilities) && nrow(scan$vulnerabilities) > 0L) {
    sections[[length(sections) + 1L]] <- list(
      title = "Найденные уязвимости",
      text = "Первые найденные совпадения по пакетам и версиям. Подробные описания укорочены для читаемости.",
      table = set_prepare_table(
        scan$vulnerabilities,
        c("repository", "sha", "component_name", "component_version", "matched_ecosystem", "osv_id", "summary", "affected"),
        c("Репозиторий", "Коммит", "Пакет", "Экосистема", "Версия", "OSV ID", "Описание", "Критичность"),
        limit = 100L,
        max_chars = 75L
      )
    )
  }

  if (is.data.frame(scan$vulnerability_lifecycle) && nrow(scan$vulnerability_lifecycle) > 0L) {
    sections[[length(sections) + 1L]] <- list(
      title = "Жизненный цикл уязвимостей",
      text = "Состояние найденных уязвимостей по истории коммитов.",
      table = set_prepare_table(
        scan$vulnerability_lifecycle,
        c("repository", "osv_id", "status", "introduced_author", "introduced_sha", "introduced_date", "fixed_author", "fixed_sha", "fixed_date"),
        c("Репозиторий", "OSV ID", "Статус", "Появилась в коммите", "Исправлена в коммите"),
        limit = 100L,
        max_chars = 70L
      )
    )

    lifecycle_table <- sections[[length(sections)]]$table
    if (is.data.frame(lifecycle_table) && ncol(lifecycle_table) >= 9L) {
      names(lifecycle_table)[seq_len(9L)] <- c(
        "Репозиторий", "OSV ID", "Статус",
        "Никнейм пользователя", "Появилась в коммите", "Дата появления",
        "Кто исправил", "Исправлена в коммите", "Дата исправления"
      )
      sections[[length(sections)]]$table <- lifecycle_table
    }
  }

  if (is.data.frame(diagnostics$status_counts) && nrow(diagnostics$status_counts) > 0L) {
    sections[[length(sections) + 1L]] <- list(
      title = "Статусы сканирования",
      text = "Краткая диагностика того, какие коммиты были просканированы, пропущены или завершились ошибкой.",
      table = set_prepare_table(
        diagnostics$status_counts,
        c("status", "n"),
        c("Статус", "Количество"),
        limit = 8L
      )
    )
  }

  sections
}

build_set_protocol_report <- function(profile,
                                      commits_df,
                                      links_df,
                                      user_info,
                                      activity_timeline,
                                      analysis,
                                      vulnerability_scan = list(status = "skipped", message = "Проверка не запускалась.")) {
  summary_df <- if (is.list(analysis) && is.data.frame(analysis$summary)) {
    analysis$summary
  } else {
    data.frame()
  }

  profile_df <- if (is.list(user_info) && is.data.frame(user_info$profile)) {
    user_info$profile
  } else {
    data.frame()
  }

  owned_repos <- if (is.list(user_info) && is.data.frame(user_info$owned_repos)) {
    user_info$owned_repos
  } else {
    data.frame()
  }

  language_stats <- if (is.list(user_info) && is.data.frame(user_info$language_stats)) {
    user_info$language_stats
  } else {
    data.frame()
  }

  if (!is.data.frame(language_stats) || nrow(language_stats) == 0L) {
    language_stats <- set_language_stats_from_repos(owned_repos)
  }

  activity_points <- if (is.list(activity_timeline) && is.data.frame(activity_timeline$points)) {
    activity_timeline$points
  } else if (is.data.frame(activity_timeline)) {
    activity_timeline
  } else {
    data.frame()
  }

  activity_overall <- if (is.list(analysis) && is.list(analysis$activity)) {
    analysis$activity$overall
  } else {
    data.frame()
  }

  popularity_overall <- if (is.list(analysis) && is.list(analysis$popularity)) {
    analysis$popularity$overall
  } else {
    data.frame()
  }

  ownership_summary <- if (is.list(analysis) && is.list(analysis$ownership)) {
    analysis$ownership$summary
  } else {
    data.frame()
  }

  social_overall <- if (is.list(analysis) && is.list(analysis$social)) {
    analysis$social$overall
  } else {
    data.frame()
  }

  overview <- data.frame(
    "Показатель" = c(
      "Цель",
      "Коммиты",
      "Репозитории",
      "Социальные ссылки",
      "Точек активности",
      "Звезды",
      "Форки",
      "Индекс популярности",
      "Проверка уязвимостей"
    ),
    "Значение" = c(
      profile,
      set_count(commits_df),
      set_count(owned_repos),
      set_count(links_df),
      set_count(activity_points),
      set_metric(summary_df, "total_stars_received", 0),
      set_metric(summary_df, "total_forks_received", 0),
      set_metric(summary_df, "popularity_index", 0),
      vulnerability_scan$message %||% vulnerability_scan$status %||% "—"
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  sections <- list(
    list(
      title = "Сводка цели",
      text = paste(
        "Протокол Сет собрал открытые и кэшированные GitHub-данные без обращения к ИИ.",
        "Отчет основан на фактических счетчиках, коммитах, репозиториях, ссылках и временной активности."
      ),
      table = overview
    ),
    list(
      title = "Профиль",
      text = "Основные публичные поля профиля GitHub.",
      table = set_prepare_table(
        profile_df,
        c("login", "name", "company", "location", "public_repos", "followers", "following", "account_created_at"),
        c("Логин", "Имя", "Компания", "Локация", "Публичные репозитории", "Подписчики", "Подписки", "Создан"),
        1L
      )
    ),
    list(
      title = "Коммитная активность",
      text = "Сухая статистика по объему и регулярности коммитов.",
      table = set_metric_table(activity_overall)
    ),
    list(
      title = "Популярность репозиториев",
      text = "Сводка по публичным репозиториям, звездам, форкам и расчетному индексу популярности.",
      table = set_metric_table(popularity_overall)
    ),
    list(
      title = "Свои и внешние репозитории",
      text = "Разделение активности между собственными и сторонними репозиториями.",
      table = set_prepare_table(
        ownership_summary,
        c("ownership_type", "repositories", "commits", "commit_share", "avg_commits_per_repo"),
        c("Тип репозитория", "Репозитории", "Коммиты", "Доля коммитов", "Среднее коммитов на репозиторий"),
        6L
      )
    ),
    list(
      title = "Языки и стек",
      text = "Топ языков по публичным репозиториям.",
      table = set_prepare_table(
        language_stats,
        c("language", "repositories", "total_repo_size_kb", "total_repo_stars", "total_repo_forks"),
        c("Язык", "Репозитории", "Размер, КБ", "Звезды", "Форки"),
        8L
      )
    ),
    list(
      title = "Социальное присутствие",
      text = "Счетчики найденных публичных ссылок.",
      table = set_metric_table(social_overall)
    ),
    list(
      title = "Найденные ссылки",
      text = "Первые найденные ссылки из публичных источников. Длинные URL укорочены, чтобы отчет оставался читаемым.",
      table = set_prepare_table(
        links_df,
        c("url", "source", "domain", "profile", "fetched_at"),
        c("Ссылка", "Источник", "Домен", "Профиль", "Дата сбора"),
        8L,
        max_chars = 80L
      )
    ),
    list(
      title = "Последняя временная активность",
      text = "Последние агрегированные точки активности по выбранному временному шагу.",
      table = if (is.data.frame(activity_points) && nrow(activity_points) > 0L) {
        set_prepare_table(
          utils::tail(activity_points, min(12L, nrow(activity_points))),
          c("bucket_time", "activity_type", "activity_value", "repository"),
          c("Период", "Тип активности", "Значение", "Репозиторий"),
          12L,
          max_chars = 70L
        )
      } else {
        data.frame()
      }
    )
  )

  sections <- c(sections, set_vulnerability_sections(vulnerability_scan))

  list(
    profile = profile,
    protocol_type = "set",
    protocol_label = "Сет",
    generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    overview = overview,
    sections = sections,
    plots = set_collect_plot_paths(analysis),
    vulnerability_scan = vulnerability_scan,
    output_dir = if (is.list(analysis)) analysis$output_dir else NA_character_,
    summary_path = if (is.list(analysis)) analysis$summary_path else NA_character_
  )
}

run_isis_protocol <- function(profile,
                              conn,
                              progress = function(value, label = NULL) NULL) {
  profile <- trimws(as.character(profile %||% ""))
  if (!nzchar(profile)) {
    stop("Не указана цель.", call. = FALSE)
  }
  if (is.null(conn)) {
    stop("Необходимо передать объект подключения ClickHouse в параметре 'conn'.", call. = FALSE)
  }

  load_set_protocol_dependencies()

  if (!exists("mcp_chat_with_clickhouse", mode = "function")) {
    stop("mcp_chat_with_clickhouse() is not loaded.", call. = FALSE)
  }

  progress(15, "Подготовка запроса ИСИДЫ")
  result <- mcp_chat_with_clickhouse(
    question = paste(
      "Посмотри всю информацию по пользователю",
      profile,
      "и сделай психологический портрет пользователя с описанием его привычек, характера работы, отношения к безопасности разрабатываемых продуктов и общего психологического портрета"
    ),
    conn = conn,
    allowed_tables = NULL,
    max_rows = 5000,
    max_tool_rounds = 20,
    verbose_tools = TRUE
  )

  progress(85, "Формирование отчета ИСИДЫ")
  final_message <- result$final_message$content %||% ""
  if (is.list(final_message)) {
    final_message <- paste(unlist(final_message, use.names = FALSE), collapse = "\n")
  }
  final_message <- trimws(as.character(final_message))
  if (!nzchar(final_message)) {
    final_message <- "ИСИДА не вернула содержательный ответ."
  }

  report <- list(
    profile = profile,
    protocol_type = "isis",
    protocol_label = "Исида",
    generated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    overview = data.frame(
      "Показатель" = c("Цель", "Источник"),
      "Значение" = c(profile, "MCP + ClickHouse"),
      check.names = FALSE,
      stringsAsFactors = FALSE
    ),
    sections = list(
      list(
        title = "Психологический портрет (ИСИДА)",
        text = "Интерпретация ИСИДЫ на основе данных из ClickHouse.",
        render_markdown = TRUE,
        markdown = final_message,
        table = data.frame()
      )
    ),
    plots = character(),
    vulnerability_scan = list(status = "skipped", message = "not_applicable"),
    output_dir = NA_character_,
    summary_path = NA_character_
  )

  progress(100, "Отчет ИСИДЫ готов")
  list(profile = profile, report = report, result = result)
}

run_set_protocol <- function(profile,
                             token,
                             conn,
                             progress = function(value, label = NULL) NULL) {
  profile <- trimws(as.character(profile %||% ""))
  token <- trimws(as.character(token %||% ""))

  if (!nzchar(profile)) {
    stop("Не указана цель.", call. = FALSE)
  }
  if (!nzchar(token)) {
    stop("В личном кабинете не указан GitHub токен.", call. = FALSE)
  }

  token_arg <- token
  load_set_protocol_dependencies()

  progress(5, "Подготовка протокола Сет")

  progress(15, "Сбор коммитов цели")
  commits_df <- get_user_commits(
    profile = profile,
    token = token_arg,
    include_stats = TRUE,
    conn = conn
  )

  progress(30, "Сбор социальных ссылок")
  links_df <- get_github_social_links(
    profile = profile,
    token = token_arg,
    conn = conn
  )

  progress(45, "Сбор профиля и репозиториев")
  user_info <- get_github_user_info(
    profile = profile,
    token = token_arg,
    include_commit_stats = TRUE,
    conn = conn
  )

  progress(62, "Сбор временной активности")
  activity_timeline <- get_github_user_activity_timeline(
    profile = profile,
    token = token_arg,
    conn = conn,
    start_at = Sys.time() - 365 * 24 * 60 * 60 * 8,
    end_at = Sys.time(),
    step = "month",
    activity_types = c(
      "commit",
      "issue_opened",
      "pr_opened",
      "issue_comment",
      "pr_review_comment",
      "commit_comment",
      "push_event",
      "watch_event",
      "fork_event"
    ),
    include_zero_points = TRUE,
    use_clickhouse_cache = TRUE,
    cache_max_age_hours = 12,
    cache_only = FALSE,
    profile_cache_table = "github_user_profile_native_stats",
    save_to_clickhouse = TRUE
  )

  vulnerability_scan <- run_set_vulnerability_scan(
    profile = profile,
    token = token_arg,
    conn = conn,
    progress = progress
  )

  progress(88, "Локальный статистический анализ")
  analysis <- run_github_profile_analysis(
    commits_df = commits_df,
    links_df = links_df,
    user_info = user_info,
    profile_name = profile
  )

  progress(96, "Сборка структурированного отчета")
  report <- build_set_protocol_report(
    profile = profile,
    commits_df = commits_df,
    links_df = links_df,
    user_info = user_info,
    activity_timeline = activity_timeline,
    analysis = analysis,
    vulnerability_scan = vulnerability_scan
  )

  progress(100, "Отчет готов")

  list(
    profile = profile,
    commits_df = commits_df,
    links_df = links_df,
    user_info = user_info,
    activity_timeline = activity_timeline,
    analysis = analysis,
    report = report
  )
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L) {
    return(y)
  }
  if (is.atomic(x) && length(x) == 1L && is.na(x)) {
    return(y)
  }
  x
}
