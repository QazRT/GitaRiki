# MCP/OpenAI-compatible helpers for AI access to package data sources.

.mcp_assert_scalar_character <- function(x, arg_name, allow_empty = FALSE) {
  ok <- is.character(x) && length(x) == 1L && (allow_empty || nzchar(x))
  if (!isTRUE(ok)) {
    stop("`", arg_name, "` must be a ", if (allow_empty) "character scalar." else "non-empty character scalar.", call. = FALSE)
  }
  invisible(TRUE)
}

.mcp_assert_scalar_numeric_ge <- function(x, arg_name, min_value = 0) {
  ok <- is.numeric(x) && length(x) == 1L && is.finite(x) && !is.na(x) && x >= min_value
  if (!isTRUE(ok)) {
    stop("`", arg_name, "` must be numeric scalar >= ", min_value, ".", call. = FALSE)
  }
  invisible(TRUE)
}

.mcp_assert_scalar_logical <- function(x, arg_name) {
  if (!(is.logical(x) && length(x) == 1L && !is.na(x))) {
    stop("`", arg_name, "` must be TRUE/FALSE scalar.", call. = FALSE)
  }
  invisible(TRUE)
}

.mcp_null <- function(x, y) {
  if (is.null(x)) y else x
}

.mcp_json <- function(x) {
  jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    dataframe = "rows",
    POSIXt = "ISO8601",
    digits = NA
  )
}

.mcp_normalize_base_url <- function(base_url) {
  sub("/+$", "", base_url)
}

.mcp_find_tool <- function(tools, name) {
  for (tool in tools) {
    if (is.list(tool) && identical(tool$name, name)) {
      return(tool)
    }
  }
  NULL
}

.mcp_tool_arguments <- function(arguments) {
  if (is.null(arguments) || identical(arguments, "")) {
    return(list())
  }
  if (is.character(arguments) && length(arguments) == 1L) {
    return(jsonlite::fromJSON(arguments, simplifyVector = FALSE))
  }
  if (is.list(arguments)) {
    return(arguments)
  }
  stop("Tool arguments must be a JSON string or a named list.", call. = FALSE)
}

.mcp_tool_result_message <- function(tool_call, result) {
  list(
    role = "tool",
    tool_call_id = .mcp_null(tool_call$id, ""),
    name = tool_call[["function"]]$name,
    content = as.character(.mcp_json(result))
  )
}

.mcp_summarize_tool_result <- function(result) {
  if (!is.list(result)) {
    return(list(type = class(result)[[1L]], value = as.character(result)[[1L]]))
  }

  out <- list(fields = names(result))
  if (!is.null(result$error)) {
    out$error <- isTRUE(result$error)
  }
  if (!is.null(result$message)) {
    out$message <- as.character(result$message)
  }
  if (!is.null(result$sql)) {
    out$sql <- as.character(result$sql)
  }
  if (!is.null(result$row_count)) {
    out$row_count <- as.integer(result$row_count)
  }
  if (!is.null(result$columns)) {
    out$columns <- as.character(result$columns)
  }
  if (!is.null(result$tables) && is.data.frame(result$tables)) {
    out$table_count <- nrow(result$tables)
    if (all(c("database", "name") %in% names(result$tables))) {
      out$tables <- paste(result$tables$database, result$tables$name, sep = ".")
    }
  }
  out
}

.mcp_tool_event <- function(round_idx, tool_call, arguments, result) {
  list(
    round = as.integer(round_idx),
    tool_call_id = .mcp_null(tool_call$id, ""),
    name = tool_call[["function"]]$name,
    arguments = arguments,
    result_summary = .mcp_summarize_tool_result(result)
  )
}

.mcp_openai_headers <- function(api_key) {
  headers <- c("Content-Type" = "application/json")
  if (is.character(api_key) && length(api_key) == 1L && nzchar(api_key)) {
    headers <- c(headers, "Authorization" = paste("Bearer", api_key))
  }
  headers
}

.mcp_openai_message <- function(response) {
  choices <- response$choices
  if (is.null(choices) || length(choices) == 0L || is.null(choices[[1L]]$message)) {
    stop("OpenAI-compatible response does not contain `choices[[1]]$message`.", call. = FALSE)
  }
  choices[[1L]]$message
}

.mcp_has_tool_calls <- function(message) {
  !is.null(message$tool_calls) && length(message$tool_calls) > 0L
}

.mcp_language_guard_prompt <- function() {
  paste(
    "Language rule: always answer the user in Russian.",
    "Use Russian for explanations, summaries, recommendations, and error descriptions.",
    "Keep code, SQL, identifiers, table names, column names, package names, and API fields unchanged."
  )
}

.mcp_strip_sql_semicolon <- function(sql) {
  sub(";\\s*$", "", trimws(sql), perl = TRUE)
}

.mcp_validate_readonly_sql <- function(sql) {
  .mcp_assert_scalar_character(sql, "sql")
  sql <- .mcp_strip_sql_semicolon(sql)

  if (grepl(";", sql, fixed = TRUE)) {
    stop("Only one SQL statement is allowed.", call. = FALSE)
  }

  if (!grepl("^(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\\b", sql, ignore.case = TRUE, perl = TRUE)) {
    stop("Only read-only SQL statements are allowed.", call. = FALSE)
  }

  forbidden <- paste(
    c(
      "INSERT", "ALTER", "DROP", "TRUNCATE", "CREATE", "DELETE", "UPDATE",
      "OPTIMIZE", "KILL", "ATTACH", "DETACH", "RENAME", "GRANT", "REVOKE",
      "SYSTEM", "INTO\\s+OUTFILE", "FORMAT"
    ),
    collapse = "|"
  )
  if (grepl(paste0("\\b(", forbidden, ")\\b"), sql, ignore.case = TRUE, perl = TRUE)) {
    stop("The SQL statement contains an operation that is not allowed for AI tools.", call. = FALSE)
  }
  if (grepl("\\b(url|file|s3|hdfs|jdbc|mysql|postgresql|mongodb|remote|cluster|input)\\s*\\(", sql, ignore.case = TRUE, perl = TRUE)) {
    stop("External ClickHouse table functions are not allowed for AI tools.", call. = FALSE)
  }

  sql
}

.mcp_sql_needs_limit <- function(sql) {
  grepl("^(SELECT|WITH)\\b", sql, ignore.case = TRUE, perl = TRUE) &&
    !grepl("\\bLIMIT\\b", sql, ignore.case = TRUE, perl = TRUE)
}

.mcp_apply_sql_limit <- function(sql, max_rows) {
  .mcp_assert_scalar_numeric_ge(max_rows, "max_rows", min_value = 1)
  sql <- .mcp_validate_readonly_sql(sql)
  if (.mcp_sql_needs_limit(sql)) {
    sql <- paste(sql, "LIMIT", as.integer(max_rows))
  }
  sql
}

.mcp_default_db <- function(conn, default_db = "default") {
  if (is.list(conn) && !is.null(conn$dbname) && nzchar(as.character(conn$dbname))) {
    return(as.character(conn$dbname))
  }
  default_db
}

.mcp_normalize_table_name <- function(table_name, default_db = "default") {
  table_name <- gsub("`", "", trimws(table_name), fixed = TRUE)
  table_name <- ifelse(
    grepl(".", table_name, fixed = TRUE),
    table_name,
    paste(default_db, table_name, sep = ".")
  )
  tolower(table_name)
}

.mcp_extract_table_refs <- function(sql) {
  pattern <- "\\b(?:FROM|JOIN)\\s+(`[^`]+`(?:\\.`[^`]+`)?|[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)?)"
  matches <- regmatches(sql, gregexpr(pattern, sql, ignore.case = TRUE, perl = TRUE))[[1L]]
  if (length(matches) == 0L || identical(matches, character(0))) {
    return(character())
  }
  refs <- sub("^\\s*(?:FROM|JOIN)\\s+", "", matches, ignore.case = TRUE, perl = TRUE)
  refs[!grepl("^\\(", refs)]
}

.mcp_check_allowed_tables <- function(sql, allowed_tables, default_db = "default") {
  if (is.null(allowed_tables)) {
    return(invisible(TRUE))
  }
  if (!is.character(allowed_tables) || length(allowed_tables) == 0L) {
    stop("`allowed_tables` must be NULL or a non-empty character vector.", call. = FALSE)
  }

  refs <- .mcp_extract_table_refs(sql)
  if (length(refs) == 0L) {
    return(invisible(TRUE))
  }

  allowed <- unique(vapply(allowed_tables, .mcp_normalize_table_name, character(1), default_db = default_db))
  used <- unique(vapply(refs, .mcp_normalize_table_name, character(1), default_db = default_db))
  denied <- setdiff(used, allowed)
  if (length(denied) > 0L) {
    stop("SQL references tables outside `allowed_tables`: ", paste(denied, collapse = ", "), call. = FALSE)
  }
  invisible(TRUE)
}

.mcp_filter_allowed_tables <- function(df, allowed_tables, default_db = "default") {
  if (is.null(allowed_tables) || !is.data.frame(df) || nrow(df) == 0L) {
    return(df)
  }
  if (!all(c("database", "name") %in% names(df))) {
    return(df)
  }
  allowed <- unique(vapply(allowed_tables, .mcp_normalize_table_name, character(1), default_db = default_db))
  actual <- .mcp_normalize_table_name(paste(df$database, df$name, sep = "."), default_db = default_db)
  df[actual %in% allowed, , drop = FALSE]
}

.mcp_table_allowed <- function(table_name, allowed_tables, default_db = "default") {
  if (is.null(allowed_tables)) {
    return(TRUE)
  }
  .mcp_normalize_table_name(table_name, default_db) %in%
    vapply(allowed_tables, .mcp_normalize_table_name, character(1), default_db = default_db)
}

#' Create a MCP-like tool definition.
#'
#' The returned object can be converted to an OpenAI-compatible function tool
#' schema and dispatched locally when the model returns a tool call.
#'
#' @param name Tool name.
#' @param description Human-readable tool description for the model.
#' @param parameters JSON Schema object for function parameters.
#' @param handler R function that accepts a named list of arguments.
#'
#' @return Tool object.
#' @export
mcp_tool <- function(name, description, parameters, handler) {
  .mcp_assert_scalar_character(name, "name")
  .mcp_assert_scalar_character(description, "description")
  if (!is.list(parameters)) {
    stop("`parameters` must be a JSON Schema list.", call. = FALSE)
  }
  if (!is.function(handler)) {
    stop("`handler` must be a function.", call. = FALSE)
  }

  out <- list(
    name = name,
    description = description,
    parameters = parameters,
    handler = handler
  )
  class(out) <- c("gitariki_mcp_tool", "list")
  out
}

#' Convert a MCP-like tool to OpenAI-compatible tool schema.
#'
#' @param tool Tool created by `mcp_tool()`, or an already OpenAI-compatible
#'   tool schema.
#'
#' @return List suitable for the `tools` field of Chat Completions APIs.
#' @export
mcp_tool_to_openai <- function(tool) {
  if (is.list(tool) && identical(tool$type, "function") && !is.null(tool[["function"]])) {
    return(tool)
  }
  if (!inherits(tool, "gitariki_mcp_tool")) {
    stop("`tool` must be created by `mcp_tool()`.", call. = FALSE)
  }
  list(
    type = "function",
    `function` = list(
      name = tool$name,
      description = tool$description,
      parameters = tool$parameters
    )
  )
}

#' Convert several MCP-like tools to OpenAI-compatible schemas.
#'
#' @param tools List of tools created by `mcp_tool()`.
#'
#' @return List of OpenAI-compatible function tool schemas.
#' @export
mcp_tools_to_openai <- function(tools) {
  if (is.null(tools)) {
    return(list())
  }
  if (!is.list(tools)) {
    stop("`tools` must be a list.", call. = FALSE)
  }
  lapply(tools, mcp_tool_to_openai)
}

#' Call a registered MCP-like tool.
#'
#' @param tools List of tools created by `mcp_tool()`.
#' @param name Tool name.
#' @param arguments Tool arguments as a JSON string or named list.
#'
#' @return Handler result.
#' @export
mcp_call_tool <- function(tools, name, arguments = list()) {
  .mcp_assert_scalar_character(name, "name")
  tool <- .mcp_find_tool(tools, name)
  if (is.null(tool)) {
    stop("Unknown MCP tool: ", name, call. = FALSE)
  }
  tool$handler(.mcp_tool_arguments(arguments))
}

#' Build a Chat Completions message.
#'
#' @param role Message role, for example `system`, `user`, `assistant`, or
#'   `tool`.
#' @param content Message content.
#' @param ... Extra message fields.
#'
#' @return Message list.
#' @export
mcp_message <- function(role, content, ...) {
  .mcp_assert_scalar_character(role, "role")
  list(role = role, content = content, ...)
}

#' Execute an OpenAI-compatible HTTP request.
#'
#' @param body Request body list.
#' @param api_key API key. If empty, no Authorization header is sent.
#' @param base_url API base URL, for example `https://api.openai.com/v1`.
#' @param endpoint Endpoint path.
#' @param timeout Timeout in seconds.
#'
#' @return Parsed JSON response as a list.
#' @export
mcp_openai_request <- function(
    body,
    api_key = Sys.getenv("OPENAI_API_KEY", unset = ""),
    base_url = Sys.getenv("OPENAI_BASE_URL", unset = "https://api.openai.com/v1"),
    endpoint = "/chat/completions",
    timeout = 120
) {
  if (!requireNamespace("curl", quietly = TRUE)) {
    stop("Package 'curl' is required but not installed.", call. = FALSE)
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required but not installed.", call. = FALSE)
  }
  if (!is.list(body)) {
    stop("`body` must be a list.", call. = FALSE)
  }
  .mcp_assert_scalar_character(base_url, "base_url")
  .mcp_assert_scalar_character(endpoint, "endpoint")
  .mcp_assert_scalar_numeric_ge(timeout, "timeout", min_value = 1)

  url <- paste0(.mcp_normalize_base_url(base_url), endpoint)
  handle <- curl::new_handle()
  do.call(curl::handle_setheaders, c(list(handle = handle), as.list(.mcp_openai_headers(api_key))))
  curl::handle_setopt(
    handle,
    post = TRUE,
    postfields = as.character(.mcp_json(body)),
    timeout = as.integer(timeout)
  )

  response <- curl::curl_fetch_memory(url, handle = handle)
  response_body <- rawToChar(response$content)

  if (response$status_code >= 300L) {
    stop("OpenAI-compatible request failed (", response$status_code, "): ", response_body, call. = FALSE)
  }

  jsonlite::fromJSON(response_body, simplifyVector = FALSE)
}

#' Run an OpenAI-compatible chat with MCP-like tool calling.
#'
#' @param messages List of OpenAI-compatible messages.
#' @param tools List of tools created by `mcp_tool()`.
#' @param model Model name.
#' @param api_key API key. If empty, no Authorization header is sent.
#' @param base_url API base URL.
#' @param system_prompt Optional system prompt prepended to `messages`.
#' @param temperature Sampling temperature.
#' @param max_tokens Optional max output tokens.
#' @param tool_choice OpenAI-compatible tool choice value.
#' @param max_tool_rounds Max number of tool-call iterations.
#' @param timeout Timeout in seconds.
#' @param extra_body Extra request body fields.
#' @param verbose_tools If `TRUE`, prints tool calls as they are executed.
#'
#' @return List with `messages`, last `response`, `final_message`, and
#'   `tool_events`.
#' @export
mcp_openai_chat <- function(
    messages,
    tools = list(),
    model = Sys.getenv("OPENAI_MODEL", unset = "gpt-4.1-mini"),
    api_key = Sys.getenv("OPENAI_API_KEY", unset = ""),
    base_url = Sys.getenv("OPENAI_BASE_URL", unset = "https://api.openai.com/v1"),
    system_prompt = NULL,
    temperature = 0,
    max_tokens = NULL,
    tool_choice = "auto",
    max_tool_rounds = 4,
    timeout = 120,
    extra_body = list(),
    verbose_tools = FALSE
) {
  .mcp_assert_scalar_character(model, "model")
  if (!is.list(messages) || length(messages) == 0L) {
    stop("`messages` must be a non-empty list of messages.", call. = FALSE)
  }
  .mcp_assert_scalar_numeric_ge(max_tool_rounds, "max_tool_rounds", min_value = 0)
  .mcp_assert_scalar_logical(verbose_tools, "verbose_tools")
  if (!is.list(extra_body)) {
    stop("`extra_body` must be a list.", call. = FALSE)
  }

  if (!is.null(system_prompt)) {
    .mcp_assert_scalar_character(system_prompt, "system_prompt", allow_empty = TRUE)
    messages <- c(list(mcp_message("system", system_prompt)), messages)
  }
  messages <- c(list(mcp_message("system", .mcp_language_guard_prompt())), messages)

  openai_tools <- mcp_tools_to_openai(tools)
  last_response <- NULL
  tool_events <- list()

  for (round_idx in seq_len(max_tool_rounds + 1L)) {
    body <- list(
      model = model,
      messages = messages,
      temperature = temperature
    )

    if (!is.null(max_tokens)) {
      body$max_tokens <- max_tokens
    }
    if (length(openai_tools) > 0L) {
      body$tools <- openai_tools
      body$tool_choice <- tool_choice
    }
    if (length(extra_body) > 0L) {
      for (field in names(extra_body)) {
        body[[field]] <- extra_body[[field]]
      }
    }

    last_response <- mcp_openai_request(
      body = body,
      api_key = api_key,
      base_url = base_url,
      endpoint = "/chat/completions",
      timeout = timeout
    )

    assistant_message <- .mcp_openai_message(last_response)
    messages <- c(messages, list(assistant_message))

    if (!.mcp_has_tool_calls(assistant_message)) {
      return(list(
        messages = messages,
        response = last_response,
        final_message = assistant_message,
        tool_events = tool_events
      ))
    }

    if (round_idx > max_tool_rounds) {
      stop("Tool-call loop exceeded `max_tool_rounds`.", call. = FALSE)
    }

    for (tool_call in assistant_message$tool_calls) {
      arguments <- .mcp_tool_arguments(tool_call[["function"]]$arguments)
      if (isTRUE(verbose_tools)) {
        message("[mcp] tool=", tool_call[["function"]]$name, " args=", as.character(.mcp_json(arguments)))
      }
      result <- tryCatch(
        mcp_call_tool(
          tools = tools,
          name = tool_call[["function"]]$name,
          arguments = arguments
        ),
        error = function(e) list(error = TRUE, message = conditionMessage(e))
      )
      tool_events[[length(tool_events) + 1L]] <- .mcp_tool_event(round_idx, tool_call, arguments, result)
      messages <- c(messages, list(.mcp_tool_result_message(tool_call, result)))
    }
  }

  list(
    messages = messages,
    response = last_response,
    final_message = .mcp_openai_message(last_response),
    tool_events = tool_events
  )
}

#' Extract MCP tool-call trace from a chat result.
#'
#' @param result Result returned by `mcp_openai_chat()` or
#'   `mcp_chat_with_clickhouse()`.
#'
#' @return Data frame with executed tool calls.
#' @export
mcp_tool_trace <- function(result) {
  events <- result$tool_events
  if (is.null(events) || length(events) == 0L) {
    return(data.frame(
      round = integer(),
      name = character(),
      arguments_json = character(),
      result_json = character(),
      stringsAsFactors = FALSE
    ))
  }

  rows <- lapply(events, function(event) {
    data.frame(
      round = event$round,
      name = event$name,
      arguments_json = as.character(.mcp_json(event$arguments)),
      result_json = as.character(.mcp_json(event$result_summary)),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, rows)
}

#' Query ClickHouse for AI tool use.
#'
#' This helper enforces a read-only SQL policy, blocks multiple statements,
#' appends a LIMIT to SELECT/WITH queries when missing, and returns a compact
#' JSON-serializable result object.
#'
#' @param conn ClickHouse connection.
#' @param sql SQL statement.
#' @param max_rows Max rows returned to the model.
#' @param allowed_tables Optional vector of allowed table names.
#'
#' @return List with columns, row count, and data.
#' @export
mcp_query_clickhouse_for_ai <- function(
    conn,
    sql,
    max_rows = 100,
    allowed_tables = NULL
) {
  default_db <- .mcp_default_db(conn)
  sql <- .mcp_apply_sql_limit(sql, max_rows = max_rows)
  .mcp_check_allowed_tables(sql, allowed_tables = allowed_tables, default_db = default_db)

  data <- query_clickhouse(conn, sql)
  if (!is.data.frame(data)) {
    data <- as.data.frame(data, stringsAsFactors = FALSE)
  }

  list(
    sql = sql,
    row_count = nrow(data),
    max_rows = as.integer(max_rows),
    truncated = nrow(data) >= as.integer(max_rows),
    columns = names(data),
    data = data
  )
}

#' Create ClickHouse MCP-like tools.
#'
#' The tool set contains `clickhouse_list_tables`,
#' `clickhouse_describe_table`, and `clickhouse_query`.
#'
#' @param conn ClickHouse connection.
#' @param allowed_tables Optional vector of tables visible to the model.
#' @param max_rows Max rows returned by `clickhouse_query`.
#'
#' @return List of MCP-like tools.
#' @export
mcp_create_clickhouse_tools <- function(
    conn,
    allowed_tables = NULL,
    max_rows = 100
) {
  .mcp_assert_scalar_numeric_ge(max_rows, "max_rows", min_value = 1)
  default_db <- .mcp_default_db(conn)

  list(
    mcp_tool(
      name = "clickhouse_list_tables",
      description = "List ClickHouse tables available for analysis.",
      parameters = list(
        type = "object",
        properties = list(),
        additionalProperties = FALSE
      ),
      handler = function(arguments) {
        sql <- paste(
          "SELECT database, name, engine, total_rows, total_bytes",
          "FROM system.tables",
          "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')",
          "ORDER BY database, name",
          "LIMIT 500"
        )
        tables <- query_clickhouse(conn, sql)
        tables <- .mcp_filter_allowed_tables(tables, allowed_tables, default_db = default_db)
        list(row_count = nrow(tables), tables = tables)
      }
    ),
    mcp_tool(
      name = "clickhouse_describe_table",
      description = "Describe the columns and types of one ClickHouse table.",
      parameters = list(
        type = "object",
        properties = list(
          table_name = list(
            type = "string",
            description = "Table name, optionally qualified as database.table."
          )
        ),
        required = I(c("table_name")),
        additionalProperties = FALSE
      ),
      handler = function(arguments) {
        table_name <- arguments$table_name
        .mcp_assert_scalar_character(table_name, "table_name")
        if (!.mcp_table_allowed(table_name, allowed_tables, default_db = default_db)) {
          stop("Table is not allowed for AI access: ", table_name, call. = FALSE)
        }
        data <- query_clickhouse(conn, paste("DESCRIBE TABLE", quote_table_ident(table_name, default_db)))
        list(table_name = table_name, columns = data)
      }
    ),
    mcp_tool(
      name = "clickhouse_query",
      description = paste(
        "Run a read-only ClickHouse SQL query for analysis.",
        "Use only SELECT, WITH, SHOW, DESCRIBE, DESC, or EXPLAIN.",
        "Do not request mutations or DDL. Results are limited automatically."
      ),
      parameters = list(
        type = "object",
        properties = list(
          sql = list(
            type = "string",
            description = "Read-only ClickHouse SQL query."
          )
        ),
        required = I(c("sql")),
        additionalProperties = FALSE
      ),
      handler = function(arguments) {
        mcp_query_clickhouse_for_ai(
          conn = conn,
          sql = arguments$sql,
          max_rows = max_rows,
          allowed_tables = allowed_tables
        )
      }
    )
  )
}

#' Locate an AI configuration file shipped with the package.
#'
#' @param ... Path components under `inst/ai` in the source tree, or under
#'   `ai` after package installation.
#'
#' @return Existing file path.
#' @export
mcp_ai_file_path <- function(...) {
  parts <- c(...)
  pkg <- tryCatch(utils::packageName(environment(mcp_ai_file_path)), error = function(e) NULL)

  if (!is.null(pkg) && nzchar(pkg)) {
    installed_path <- do.call(
      system.file,
      c(as.list(c("ai", parts)), list(package = pkg, mustWork = FALSE))
    )
    if (nzchar(installed_path) && file.exists(installed_path)) {
      return(installed_path)
    }
  }

  source_path <- do.call(file.path, as.list(c(getwd(), "inst", "ai", parts)))
  if (file.exists(source_path)) {
    return(normalizePath(source_path, winslash = "/", mustWork = TRUE))
  }

  stop("AI file was not found: ", file.path(parts), call. = FALSE)
}

#' Read an AI prompt, RAG, or settings file.
#'
#' @param ... Path components under `inst/ai`.
#'
#' @return UTF-8 character scalar.
#' @export
mcp_read_ai_file <- function(...) {
  path <- mcp_ai_file_path(...)
  paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
}

#' Read default OpenAI/MCP settings.
#'
#' @param path Settings path under `inst/ai`.
#'
#' @return Parsed JSON settings.
#' @export
mcp_default_settings <- function(path = c("settings", "openai_mcp.json")) {
  if (!is.character(path) || length(path) == 0L) {
    stop("`path` must be a character vector of path parts.", call. = FALSE)
  }
  jsonlite::fromJSON(do.call(mcp_read_ai_file, as.list(path)), simplifyVector = FALSE)
}

#' Load default AI prompts, RAG context, and settings.
#'
#' @return List with `prompts`, `rag`, and `settings`.
#' @export
mcp_default_ai_assets <- function() {
  read_asset <- function(path) {
    do.call(mcp_read_ai_file, as.list(path))
  }

  list(
    prompts = list(
      system = read_asset(c("prompts", "system.md")),
      role = read_asset(c("prompts", "role.md"))
    ),
    rag = list(
      clickhouse_context = read_asset(c("rag", "clickhouse_context.md")),
      security_rules = read_asset(c("rag", "security_rules.md"))
    ),
    settings = mcp_default_settings()
  )
}

#' Ask an OpenAI-compatible model a question with ClickHouse tools enabled.
#'
#' @param question User question.
#' @param conn ClickHouse connection.
#' @param model Model name.
#' @param api_key API key. If empty, no Authorization header is sent.
#' @param base_url API base URL.
#' @param allowed_tables Optional vector of ClickHouse tables visible to AI.
#' @param max_rows Max rows returned per ClickHouse query.
#' @param system_prompt Optional system prompt.
#' @param ... Extra arguments passed to `mcp_openai_chat()`.
#'
#' @return Chat result from `mcp_openai_chat()`.
#' @export
mcp_chat_with_clickhouse <- function(
    question,
    conn,
    model = Sys.getenv("OPENAI_MODEL", unset = "gpt-4.1-mini"),
    api_key = Sys.getenv("OPENAI_API_KEY", unset = ""),
    base_url = Sys.getenv("OPENAI_BASE_URL", unset = "https://api.openai.com/v1"),
    allowed_tables = NULL,
    max_rows = 100,
    system_prompt = NULL,
    ...
) {
  .mcp_assert_scalar_character(question, "question")
  if (is.null(system_prompt)) {
    system_prompt <- tryCatch(
      mcp_read_ai_file("prompts", "system.md"),
      error = function(e) paste(
        "You are an analytical assistant.",
        "Use ClickHouse tools only for read-only data access.",
        "Explain assumptions and do not invent data.",
        "Always answer the user in Russian."
      )
    )
  }

  tools <- mcp_create_clickhouse_tools(
    conn = conn,
    allowed_tables = allowed_tables,
    max_rows = max_rows
  )

  mcp_openai_chat(
    messages = list(mcp_message("user", question)),
    tools = tools,
    model = model,
    api_key = api_key,
    base_url = base_url,
    system_prompt = system_prompt,
    ...
  )
}
