#' GitaRikiOSAryans: GitHub Profile Analytics and Vulnerability Scanning
#'
#' GitaRikiOSAryans provides tools for collecting public GitHub profile data,
#' loading analytical tables into ClickHouse, scanning dependency snapshots with
#' Syft and OSV, and querying ClickHouse through an OpenAI-compatible
#' tool-calling interface.
#'
#' The package is organized around four workflows:
#'
#' \itemize{
#'   \item GitHub data collection with ClickHouse caching.
#'   \item ClickHouse import and read helpers.
#'   \item OSV vulnerability preparation, lookup, and Syft SBOM scans.
#'   \item MCP-like tools for OpenAI-compatible analytical chat over ClickHouse.
#' }
#'
#' @seealso
#' [connect_clickhouse()],
#' [get_user_commits()],
#' [prepare_osv_database()],
#' [scan_path_with_syft_and_osv()],
#' [mcp_chat_with_clickhouse()]
#'
#' @keywords internal
"_PACKAGE"
