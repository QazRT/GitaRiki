get_github_user_profile_native_stats <- function(profile,
                                                 token = NULL,
                                                 conn = NULL,
                                                 table_name = "github_user_profile_native_stats") {
  required_packages <- c("gh", "httr", "dplyr")
  for (pkg in required_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
  }
  library(gh)
  library(httr)
  library(dplyr)

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

  username <- extract_username(profile)
  if (is.null(username)) {
    stop("Could not extract GitHub username from: ", profile)
  }

  user_data <- tryCatch({
    gh::gh(
      "GET /users/{username}",
      username = username,
      .token = token
    )
  }, error = function(e) {
    stop("Failed to load GitHub user profile: ", e$message)
  })

  profile_df <- data.frame(
    login = user_data$login %||% username,
    name = user_data$name %||% NA_character_,
    company = user_data$company %||% NA_character_,
    blog = user_data$blog %||% NA_character_,
    location = user_data$location %||% NA_character_,
    email = user_data$email %||% NA_character_,
    bio = user_data$bio %||% NA_character_,
    twitter_username = user_data$twitter_username %||% NA_character_,
    hireable = user_data$hireable %||% NA,
    public_repos = user_data$public_repos %||% 0,
    public_gists = user_data$public_gists %||% 0,
    followers = user_data$followers %||% 0,
    following = user_data$following %||% 0,
    account_type = user_data$type %||% NA_character_,
    site_admin = user_data$site_admin %||% FALSE,
    account_created_at = user_data$created_at %||% NA_character_,
    account_updated_at = user_data$updated_at %||% NA_character_,
    avatar_url = user_data$avatar_url %||% NA_character_,
    html_url = user_data$html_url %||% NA_character_,
    fetched_at = Sys.time(),
    stringsAsFactors = FALSE
  )

  if (!is.null(conn)) {
    if (!exists("load_df_to_clickhouse", mode = "function")) {
      stop("Connection was provided, but function 'load_df_to_clickhouse' is not available.")
    }

    if (exists("table_exists_custom", mode = "function") &&
        exists("clickhouse_request", mode = "function")) {
      escaped_username <- gsub("'", "''", username)
      if (isTRUE(table_exists_custom(conn, table_name))) {
        sql_delete <- paste0(
          "ALTER TABLE ", table_name,
          " DELETE WHERE login = '", escaped_username, "'"
        )
        tryCatch(
          clickhouse_request(conn, sql_delete, parse_json = FALSE),
          error = function(e) warning("Failed to delete previous rows: ", e$message)
        )
      }
    }

    load_df_to_clickhouse(
      df = profile_df,
      table_name = table_name,
      conn = conn,
      overwrite = FALSE,
      append = TRUE
    )
  }

  list(
    profile = profile_df,
    raw = user_data
  )
}
