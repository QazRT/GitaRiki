# ClickHouse connector via direct HTTP requests (stateless mode).

check_clickhouse_dependencies <- function() {
  if (!requireNamespace("curl", quietly = TRUE)) {
    stop("Package 'curl' is required but not installed.", call. = FALSE)
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required but not installed.", call. = FALSE)
  }
}

is_custom_clickhouse_conn <- function(conn) {
  inherits(conn, "gitariki_clickhouse_conn")
}

normalize_table_ref <- function(table_name, default_db) {
  stopifnot(is.character(table_name), length(table_name) == 1L, nzchar(table_name))
  parts <- strsplit(table_name, ".", fixed = TRUE)[[1]]
  if (length(parts) == 2L) {
    list(database = parts[[1]], table = parts[[2]])
  } else if (length(parts) == 1L) {
    list(database = default_db, table = parts[[1]])
  } else {
    stop("Invalid table name format. Use 'table' or 'database.table'.", call. = FALSE)
  }
}

escape_sql_string <- function(x) {
  x <- gsub("\\\\", "\\\\\\\\", x)
  gsub("'", "\\\\'", x)
}

quote_ident_part <- function(x) {
  paste0("`", gsub("`", "``", x, fixed = TRUE), "`")
}

quote_table_ident <- function(table_name, default_db) {
  ref <- normalize_table_ref(table_name, default_db)
  paste0(quote_ident_part(ref$database), ".", quote_ident_part(ref$table))
}

clickhouse_request <- function(conn, sql, parse_json = TRUE) {
  stopifnot(is_custom_clickhouse_conn(conn))
  stopifnot(is.character(sql), length(sql) == 1L, nzchar(sql))

  url <- paste0(
    conn$base_url,
    "/?database=", utils::URLencode(conn$dbname, reserved = TRUE),
    "&default_format=JSON"
  )

  handle <- curl::new_handle()
  curl::handle_setheaders(
    handle,
    "X-ClickHouse-User" = conn$user,
    "X-ClickHouse-Key" = conn$password
  )
  curl::handle_setopt(handle, post = TRUE, postfields = sql)

  response <- curl::curl_fetch_memory(url, handle = handle)
  body <- rawToChar(response$content)

  if (response$status_code >= 300L) {
    stop(
      paste0(
        "ClickHouse HTTP request failed (",
        response$status_code,
        "): ",
        body
      ),
      call. = FALSE
    )
  }

  if (!parse_json) {
    return(invisible(body))
  }

  parsed <- jsonlite::fromJSON(body, simplifyVector = TRUE)
  if (!is.null(parsed$data)) {
    return(as.data.frame(parsed$data, stringsAsFactors = FALSE))
  }
  data.frame()
}

infer_clickhouse_type <- function(x) {
  x_no_na <- x[!is.na(x)]
  base_type <- if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) {
    "DateTime"
  } else if (inherits(x, "Date")) {
    "Date"
  } else if (is.logical(x)) {
    "UInt8"
  } else if (is.integer(x)) {
    "Int32"
  } else if (is.numeric(x)) {
    "Float64"
  } else {
    "String"
  }

  if (anyNA(x)) {
    paste0("Nullable(", base_type, ")")
  } else if (length(x_no_na) == 0L) {
    paste0("Nullable(", base_type, ")")
  } else {
    base_type
  }
}

create_table_from_df <- function(conn, table_name, df) {
  stopifnot(is_custom_clickhouse_conn(conn))
  stopifnot(is.data.frame(df))
  if (ncol(df) == 0L) {
    stop("Cannot create table from a data.frame with 0 columns.", call. = FALSE)
  }

  cols <- names(df)
  if (is.null(cols) || any(cols == "")) {
    stop("All data.frame columns must have names.", call. = FALSE)
  }

  col_defs <- vapply(
    cols,
    function(col) {
      paste0(quote_ident_part(col), " ", infer_clickhouse_type(df[[col]]))
    },
    character(1)
  )

  sql <- paste0(
    "CREATE TABLE ",
    quote_table_ident(table_name, conn$dbname),
    " (",
    paste(col_defs, collapse = ", "),
    ") ENGINE = MergeTree() ORDER BY tuple()"
  )
  clickhouse_request(conn, sql, parse_json = FALSE)
}

table_exists_custom <- function(conn, table_name) {
  ref <- normalize_table_ref(table_name, conn$dbname)
  sql <- paste0(
    "SELECT count() AS n FROM system.tables WHERE database = '",
    escape_sql_string(ref$database),
    "' AND name = '",
    escape_sql_string(ref$table),
    "'"
  )
  out <- clickhouse_request(conn, sql, parse_json = TRUE)
  is.data.frame(out) && nrow(out) > 0L && as.integer(out$n[[1]]) > 0L
}

sanitize_clickhouse_scalar <- function(x) {
  if (length(x) == 0L) {
    return(x)
  }
  
  if (inherits(x, "POSIXct") || inherits(x, "POSIXt")) {
    return(format(x, "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  }
  
  if (inherits(x, "Date")) {
    return(format(x, "%Y-%m-%d"))
  }
  
  if (is.factor(x)) {
    x <- as.character(x)
  }
  
  if (is.character(x)) {
    x <- enc2utf8(x)
    x <- iconv(x, from = "UTF-8", to = "UTF-8", sub = "")
    x <- gsub("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", " ", x, perl = TRUE)
    return(x)
  }
  
  x
}

sanitize_clickhouse_df <- function(df) {
  stopifnot(is.data.frame(df))
  
  cleaned <- lapply(df, sanitize_clickhouse_scalar)
  as.data.frame(cleaned, stringsAsFactors = FALSE, optional = TRUE)
}

serialize_clickhouse_field <- function(value) {
  if (length(value) == 0L || is.na(value)) {
    return("null")
  }
  
  if (is.logical(value)) {
    return(if (isTRUE(value)) "1" else "0")
  }
  
  if (is.integer(value) || is.numeric(value)) {
    return(as.character(value))
  }
  
  jsonlite::toJSON(
    unname(value),
    auto_unbox = TRUE,
    na = "null",
    POSIXt = "string",
    digits = NA
  )
}

serialize_clickhouse_row <- function(row_df) {
  stopifnot(is.data.frame(row_df), nrow(row_df) == 1L)
  
  fields <- vapply(
    names(row_df),
    function(col) {
      paste0(
        jsonlite::toJSON(col, auto_unbox = TRUE),
        ":",
        serialize_clickhouse_field(row_df[[col]][[1]])
      )
    },
    character(1)
  )
  
  paste0("{", paste(fields, collapse = ","), "}")
}

serialize_clickhouse_sql_value <- function(value) {
  if (length(value) == 0L || is.na(value)) {
    return("NULL")
  }
  
  if (inherits(value, "POSIXct") || inherits(value, "POSIXt")) {
    return(paste0("'", escape_sql_string(format(value, "%Y-%m-%d %H:%M:%S", tz = "UTC")), "'"))
  }
  
  if (inherits(value, "Date")) {
    return(paste0("'", escape_sql_string(format(value, "%Y-%m-%d")), "'"))
  }
  
  if (is.logical(value)) {
    return(if (isTRUE(value)) "1" else "0")
  }
  
  if (is.integer(value) || is.numeric(value)) {
    return(as.character(value))
  }
  
  if (is.factor(value)) {
    value <- as.character(value)
  }
  
  if (is.character(value)) {
    return(paste0("'", escape_sql_string(value), "'"))
  }
  
  paste0("'", escape_sql_string(as.character(value)), "'")
}

insert_df_custom <- function(conn, table_name, df) {
  stopifnot(is.data.frame(df))
  if (nrow(df) == 0L) {
    return(invisible(TRUE))
  }
  
  df <- sanitize_clickhouse_df(df)
  
  col_names <- names(df)
  col_sql <- paste(vapply(col_names, quote_ident_part, character(1)), collapse = ", ")
  
  rows_sql <- vapply(
    seq_len(nrow(df)),
    function(i) {
      values_sql <- vapply(
        col_names,
        function(col) serialize_clickhouse_sql_value(df[[col]][[i]]),
        character(1)
      )
      paste0("(", paste(values_sql, collapse = ", "), ")")
    },
    character(1)
  )
  
  sql <- paste0(
    "INSERT INTO ",
    quote_table_ident(table_name, conn$dbname),
    " (", col_sql, ") VALUES\n",
    paste(rows_sql, collapse = ",\n")
  )
  clickhouse_request(conn, sql, parse_json = FALSE)
  invisible(TRUE)
}

# Connect to ClickHouse in stateless HTTP mode.
connect_clickhouse <- function(
    host = "localhost",
    port = 8123,
    dbname = "default",
    user = "default",
    password = "",
    https = FALSE,
    ...
) {
  check_clickhouse_dependencies()

  stopifnot(is.character(host), length(host) == 1L, nzchar(host))
  stopifnot(is.numeric(port), length(port) == 1L, is.finite(port))
  stopifnot(is.character(dbname), length(dbname) == 1L, nzchar(dbname))
  stopifnot(is.character(user), length(user) == 1L)
  stopifnot(is.character(password), length(password) == 1L)
  stopifnot(is.logical(https), length(https) == 1L, !is.na(https))

  scheme <- if (isTRUE(https)) "https" else "http"
  conn <- list(
    host = host,
    port = as.integer(port),
    dbname = dbname,
    user = user,
    password = password,
    https = https,
    base_url = paste0(scheme, "://", host, ":", as.integer(port))
  )
  class(conn) <- c("gitariki_clickhouse_conn", "list")
  conn
}

load_df_to_clickhouse <- function(
    df,
    table_name,
    conn,
    overwrite = FALSE,
    append = TRUE
) {
  stopifnot(is.data.frame(df))
  stopifnot(is.character(table_name), length(table_name) == 1L, nzchar(table_name))
  stopifnot(is.logical(overwrite), length(overwrite) == 1L)
  stopifnot(is.logical(append), length(append) == 1L)

  if (is_custom_clickhouse_conn(conn)) {
    exists <- table_exists_custom(conn, table_name)
    created_new <- FALSE

    if (isTRUE(overwrite) && isTRUE(exists)) {
      clickhouse_request(
        conn,
        paste0("DROP TABLE ", quote_table_ident(table_name, conn$dbname)),
        parse_json = FALSE
      )
      exists <- FALSE
    }

    if (!exists) {
      create_table_from_df(conn, table_name, df)
      exists <- TRUE
      created_new <- TRUE
    }

    if (isTRUE(append) || isTRUE(created_new)) {
      insert_df_custom(conn, table_name, df)
    }

    return(invisible(TRUE))
  }

  table_id <- DBI::SQL(table_name)
  table_exists <- DBI::dbExistsTable(conn, table_id)

  if (isTRUE(overwrite) && isTRUE(table_exists)) {
    DBI::dbRemoveTable(conn, table_id)
    table_exists <- FALSE
  }

  if (!table_exists) {
    DBI::dbCreateTable(conn, table_id, df)
  }

  if (isTRUE(append) || !table_exists) {
    DBI::dbAppendTable(conn, table_id, df)
  }

  invisible(TRUE)
}

query_clickhouse <- function(conn, sql) {
  stopifnot(is.character(sql), length(sql) == 1L, nzchar(sql))

  if (is_custom_clickhouse_conn(conn)) {
    has_format <- grepl("\\bFORMAT\\b", sql, ignore.case = TRUE)
    sql_to_run <- if (has_format) sql else paste0(sql, " FORMAT JSON")
    return(clickhouse_request(conn, sql_to_run, parse_json = TRUE))
  }

  DBI::dbGetQuery(conn, sql)
}

import_table <- function(conn, table_name, limit = NULL) {
  stopifnot(is.character(table_name), length(table_name) == 1L, nzchar(table_name))

  if (!is.null(limit)) {
    stopifnot(is.numeric(limit), length(limit) == 1L, is.finite(limit), limit >= 0)
    limit <- as.integer(limit)
  }

  if (is_custom_clickhouse_conn(conn)) {
    table_sql <- quote_table_ident(table_name, conn$dbname)
    sql <- if (is.null(limit)) {
      paste0("SELECT * FROM ", table_sql)
    } else {
      paste0("SELECT * FROM ", table_sql, " LIMIT ", limit)
    }
    return(query_clickhouse(conn, sql))
  }

  table_sql <- as.character(DBI::dbQuoteIdentifier(conn, table_name))
  sql <- if (is.null(limit)) {
    paste0("SELECT * FROM ", table_sql)
  } else {
    paste0("SELECT * FROM ", table_sql, " LIMIT ", limit)
  }

  DBI::dbGetQuery(conn, sql)
}
