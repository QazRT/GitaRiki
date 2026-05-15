#!/usr/bin/env Rscript

# Quick smoke test for R/ClickHouseConnector.R
# Usage:
#   Rscript R/check_clickhouse_connector.R
# Required env vars:
#   CH_HOST, CH_PORT, CH_DB, CH_USER, CH_PASSWORD
# Optional env vars:
#   GH_USER, GITHUB_PAT

connector_path <- if (file.exists("R/ClickHouseConnector.R")) {
  "R/ClickHouseConnector.R"
} else if (file.exists("ClickHouseConnector.R")) {
  "ClickHouseConnector.R"
} else {
  stop("Cannot find ClickHouseConnector.R. Run from repo root or from R/.", call. = FALSE)
}
source(connector_path)

repos_source_path <- if (file.exists("R/ETL-Repos.R")) {
  "R/ETL-Repos.R"
} else if (file.exists("ETL-Repos.R")) {
  "ETL-Repos.R"
} else {
  NA_character_
}
if (!is.na(repos_source_path)) {
  source(repos_source_path)
}

required_env <- function(name) {
  value <- Sys.getenv(name, unset = "")
  if (!nzchar(value)) {
    stop(sprintf("Set %s in your local .Renviron before running this smoke test.", name), call. = FALSE)
  }
  value
}

host <- required_env("CH_HOST")
port <- as.integer(required_env("CH_PORT"))
dbname <- required_env("CH_DB")
user <- required_env("CH_USER")
password <- required_env("CH_PASSWORD")
github_user <- Sys.getenv("GH_USER", "torvalds")
github_token <- Sys.getenv("GITHUB_PAT", "")

cat("Connecting to ClickHouse:\n")
cat(sprintf("  host=%s port=%s db=%s user=%s\n", host, port, dbname, user))

conn <- connect_clickhouse(
  host = host,
  port = port,
  dbname = dbname,
  user = user,
  password = password
)

# 1) Ping query
ping <- query_clickhouse(conn, "SELECT 1 AS ok")
print(ping)

# 2) Write + read back
table_name <- "connector_smoke_test"
test_df <- data.frame(
  id = 1:3,
  name = c("alpha", "beta", "gamma"),
  stringsAsFactors = FALSE
)

load_df_to_clickhouse(
  df = test_df,
  table_name = table_name,
  conn = conn,
  overwrite = TRUE,
  append = TRUE
)

result <- import_table(conn, table_name)
cat("\nRead back rows:\n")
print(result)

# 3) get_user_repos/get_github_repos + save output data.frame(s)
repos_fun <- if (exists("get_user_repos", mode = "function")) {
  get("get_user_repos", mode = "function")
} else if (exists("get_github_repos", mode = "function")) {
  get("get_github_repos", mode = "function")
} else {
  NULL
}

write_repo_df <- function(df, target_table) {
  if (!is.data.frame(df)) {
    stop("Expected data.frame for table ", target_table, call. = FALSE)
  }

  load_df_to_clickhouse(
    df = df,
    table_name = target_table,
    conn = conn,
    overwrite = TRUE,
    append = TRUE
  )

  rows_written <- nrow(import_table(conn, target_table))
  cat(sprintf("Saved table %s: %d rows\n", target_table, rows_written))
}

if (is.null(repos_fun)) {
  cat("\nSkip repos test: get_user_repos/get_github_repos not found.\n")
} else {
  cat(sprintf("\nLoading repos for GitHub user: %s\n", github_user))
  repos_out <- repos_fun(
    github_user,
    token = if (nzchar(github_token)) github_token else NULL
  )

  if (is.data.frame(repos_out)) {
    write_repo_df(repos_out, "github_user_repos")
  } else if (is.list(repos_out)) {
    df_idx <- which(vapply(repos_out, is.data.frame, logical(1)))
    if (length(df_idx) == 0L) {
      stop("Repos function returned list without data.frame elements.", call. = FALSE)
    }

    df_names <- names(repos_out)
    for (i in seq_along(df_idx)) {
      idx <- df_idx[[i]]
      nm <- if (!is.null(df_names) && nzchar(df_names[[idx]])) {
        df_names[[idx]]
      } else {
        paste0("part_", i)
      }
      target_table <- paste0("github_user_repos_", gsub("[^A-Za-z0-9_]", "_", nm))
      write_repo_df(repos_out[[idx]], target_table)
    }
  } else {
    stop("Repos function returned unsupported type: ", class(repos_out)[1], call. = FALSE)
  }
}

cat("\nSmoke test completed successfully.\n")
