# ClickHouse Context

The package can store extracted GitHub, repository, commit, link, dependency,
SBOM, and vulnerability data in ClickHouse.

The AI module exposes ClickHouse through three tools:

- `clickhouse_list_tables`: lists available non-system tables.
- `clickhouse_describe_table`: returns columns and types for one table.
- `clickhouse_query`: runs a read-only SQL query and returns a limited result
  set.

General query guidance:

- Inspect tables before using them.
- Use `count()`, `uniqExact()`, `min()`, `max()`, and grouped aggregations for
  summaries.
- Select only columns needed for the answer.
- Add explicit filters for user, repository, package, date, or vulnerability
  identifiers when they are known.
- Avoid broad `SELECT *` queries unless the user asks for a small sample.

Expected data domains may include:

- GitHub profiles and repositories.
- Commits and commit-level metadata.
- Repository structure and links.
- Dependency manifests and generated SBOM components.
- OSV vulnerability records and vulnerability search results.
