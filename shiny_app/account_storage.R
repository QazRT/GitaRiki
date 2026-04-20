find_clickhouse_connector <- function() {
  candidates <- c(
    file.path(getwd(), "R", "ClickHouseConnector.R"),
    file.path(getwd(), "..", "R", "ClickHouseConnector.R"),
    file.path(dirname(normalizePath(getwd(), mustWork = FALSE)), "R", "ClickHouseConnector.R")
  )

  existing <- candidates[file.exists(candidates)]
  if (length(existing) == 0L) {
    return(NA_character_)
  }

  existing[[1]]
}

load_clickhouse_connector <- function() {
  if (exists("connect_clickhouse", mode = "function") &&
      exists("load_df_to_clickhouse", mode = "function") &&
      exists("query_clickhouse", mode = "function")) {
    return(invisible(TRUE))
  }

  connector_path <- find_clickhouse_connector()
  if (is.na(connector_path) || !nzchar(connector_path)) {
    stop("Не найден файл R/ClickHouseConnector.R.", call. = FALSE)
  }

  source(connector_path, encoding = "UTF-8")
  invisible(TRUE)
}

load_githound_renviron <- function() {
  if (isTRUE(getOption("githound.renviron.loaded", FALSE))) {
    return(invisible(TRUE))
  }

  connector_path <- find_clickhouse_connector()
  candidates <- c(
    file.path(getwd(), ".Renviron"),
    file.path(getwd(), "..", ".Renviron")
  )

  if (!is.na(connector_path) && nzchar(connector_path)) {
    candidates <- c(
      candidates,
      file.path(dirname(dirname(normalizePath(connector_path, mustWork = FALSE))), ".Renviron")
    )
  }

  candidates <- unique(normalizePath(candidates, mustWork = FALSE))
  env_file <- candidates[file.exists(candidates)][[1]]
  if (!is.null(env_file) && nzchar(env_file)) {
    readRenviron(env_file)
  }

  options(githound.renviron.loaded = TRUE)
  invisible(TRUE)
}

githound_env <- function(primary_name, legacy_name = NULL, required = TRUE) {
  load_githound_renviron()

  env_names <- c(primary_name, legacy_name)
  env_names <- env_names[nzchar(env_names) & !is.na(env_names)]

  for (env_name in env_names) {
    value <- Sys.getenv(env_name, unset = "")
    if (nzchar(value)) {
      return(value)
    }
  }

  if (isTRUE(required)) {
    stop(
      sprintf(
        "Не задана переменная окружения %s. Добавьте ее в локальный .Renviron.",
        primary_name
      ),
      call. = FALSE
    )
  }

  ""
}

connect_githound_clickhouse <- function() {
  load_clickhouse_connector()

  connect_clickhouse(
    host = githound_env("CLICKHOUSE_HOST", "CH_HOST"),
    port = as.integer(githound_env("CLICKHOUSE_PORT", "CH_PORT")),
    dbname = githound_env("CLICKHOUSE_DB", "CH_DB"),
    user = githound_env("CLICKHOUSE_USER", "CH_USER"),
    password = githound_env("CLICKHOUSE_PASSWORD", "CH_PASSWORD"),
    https = identical(tolower(Sys.getenv("CLICKHOUSE_HTTPS", "false")), "true")
  )
}

githound_sql_string <- function(x) {
  paste0("'", escape_sql_string(enc2utf8(as.character(x))), "'")
}

normalize_githound_email <- function(email) {
  tolower(trimws(enc2utf8(as.character(email))))
}

githound_uuid <- function() {
  hex <- sample(c(0:9, letters[1:6]), 32, replace = TRUE)
  paste0(
    paste0(hex[1:8], collapse = ""), "-",
    paste0(hex[9:12], collapse = ""), "-",
    paste0(hex[13:16], collapse = ""), "-",
    paste0(hex[17:20], collapse = ""), "-",
    paste0(hex[21:32], collapse = "")
  )
}

githound_password_hash <- function(password) {
  password <- enc2utf8(as.character(password))
  if (requireNamespace("digest", quietly = TRUE)) {
    return(digest::digest(password, algo = "sha256", serialize = FALSE))
  }

  paste0("demo-", sum(utf8ToInt(password) * seq_along(utf8ToInt(password))))
}

ensure_githound_accounts_table <- function(conn, table_name = "githound_accounts") {
  load_clickhouse_connector()

  sql <- paste0(
    "CREATE TABLE IF NOT EXISTS ",
    quote_table_ident(table_name, conn$dbname),
    " (",
    "user_id String, ",
    "email String, ",
    "password_hash String, ",
    "nickname String, ",
    "github_token String, ",
    "avatar_id String, ",
    "created_at DateTime, ",
    "updated_at DateTime, ",
    "version UInt64",
    ") ENGINE = ReplacingMergeTree(version) ",
    "ORDER BY email"
  )

  clickhouse_request(conn, sql, parse_json = FALSE)
  invisible(TRUE)
}

get_githound_account <- function(conn, email, table_name = "githound_accounts") {
  ensure_githound_accounts_table(conn, table_name)
  email <- normalize_githound_email(email)

  sql <- paste0(
    "SELECT * FROM ",
    quote_table_ident(table_name, conn$dbname),
    " FINAL WHERE lower(email) = ",
    githound_sql_string(email),
    " ORDER BY version DESC LIMIT 1"
  )

  query_clickhouse(conn, sql)
}

count_githound_accounts <- function(conn, email, table_name = "githound_accounts") {
  ensure_githound_accounts_table(conn, table_name)
  email <- normalize_githound_email(email)

  sql <- paste0(
    "SELECT count() AS n FROM ",
    quote_table_ident(table_name, conn$dbname),
    " FINAL WHERE lower(email) = ",
    githound_sql_string(email)
  )

  out <- query_clickhouse(conn, sql)
  if (!is.data.frame(out) || nrow(out) == 0L) {
    return(0L)
  }

  as.integer(out$n[[1]])
}

login_githound_account <- function(
    conn,
    email,
    password,
    table_name = "githound_accounts"
) {
  email <- normalize_githound_email(email)
  password <- enc2utf8(password)

  if (!nzchar(email) || !nzchar(password)) {
    stop("Введите почту и пароль.", call. = FALSE)
  }

  account <- get_githound_account(conn, email, table_name)
  if (!is.data.frame(account) || nrow(account) == 0L) {
    stop("Аккаунт не найден.", call. = FALSE)
  }

  expected_hash <- githound_password_hash(password)
  if (!identical(as.character(account$password_hash[[1]]), expected_hash)) {
    stop("Неверная почта или пароль.", call. = FALSE)
  }

  account[1, , drop = FALSE]
}

register_githound_account <- function(
    conn,
    email,
    password,
    nickname,
    avatar_id = "egypt_1",
    table_name = "githound_accounts"
) {
  email <- normalize_githound_email(email)
  nickname <- trimws(enc2utf8(nickname))
  password <- enc2utf8(password)

  if (!grepl("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", email)) {
    stop("Введите корректную почту.", call. = FALSE)
  }
  if (nchar(password) < 6L) {
    stop("Пароль должен быть не короче 6 символов.", call. = FALSE)
  }
  if (!nzchar(nickname)) {
    stop("Введите ник.", call. = FALSE)
  }

  existing_count <- count_githound_accounts(conn, email, table_name)
  if (existing_count > 0L) {
    stop("Аккаунт с такой почтой уже существует.", call. = FALSE)
  }

  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  row <- data.frame(
    user_id = githound_uuid(),
    email = email,
    password_hash = githound_password_hash(password),
    nickname = nickname,
    github_token = "",
    avatar_id = avatar_id,
    created_at = now,
    updated_at = now,
    version = as.integer(as.numeric(Sys.time())),
    stringsAsFactors = FALSE
  )

  load_df_to_clickhouse(row, table_name = table_name, conn = conn, append = TRUE)
  row
}

update_githound_account_profile <- function(
    conn,
    email,
    github_token,
    avatar_id,
    table_name = "githound_accounts"
) {
  email <- normalize_githound_email(email)
  account <- get_githound_account(conn, email, table_name)
  if (!is.data.frame(account) || nrow(account) == 0L) {
    stop("Аккаунт не найден.", call. = FALSE)
  }

  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  row <- account[1, , drop = FALSE]
  row$github_token <- enc2utf8(github_token %||% "")
  row$avatar_id <- enc2utf8(avatar_id %||% "egypt_1")
  row$updated_at <- now

  update_sql <- paste0(
    "ALTER TABLE ",
    quote_table_ident(table_name, conn$dbname),
    " UPDATE ",
    "github_token = ",
    githound_sql_string(row$github_token[[1]]),
    ", avatar_id = ",
    githound_sql_string(row$avatar_id[[1]]),
    ", updated_at = toDateTime(",
    githound_sql_string(row$updated_at[[1]]),
    ")",
    " WHERE lower(email) = ",
    githound_sql_string(email)
  )

  clickhouse_request(conn, update_sql, parse_json = FALSE)
  row
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L || is.na(x)) y else x
}
