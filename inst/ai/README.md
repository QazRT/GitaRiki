# AI Assets

This directory contains files intended for future RAG, prompts, and runtime
settings used by the OpenAI-compatible MCP module.

Recommended usage:

- `prompts/system.md`: baseline system prompt for the assistant.
- `prompts/role.md`: role/task prompt that can be appended per scenario.
- `rag/clickhouse_context.md`: data-access context for ClickHouse-backed tools.
- `rag/security_rules.md`: safety rules for read-only database access.
- `settings/openai_mcp.json`: default model, API, and tool settings.

In R, read these files with:

```r
mcp_read_ai_file("prompts", "system.md")
mcp_default_settings()
```
