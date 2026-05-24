# Project Next Steps

Context marker: `2026-05-24 01:57:14 BST`

Project score: `7.5/10`

Reason: the platform already has a working Spark 4.1.2, Delta 4.2.0, MinIO, Spark History, ClickHouse, and ClickStack loop. The biggest missing pieces are cleaner loader internals, richer normalized observability tables, and a repeatable way to compare optimization experiments.

## Immediate Priorities

1. Refactor the Go event-log loader by entity.
   - Keep `main.go` as a thin entrypoint.
   - Move config, MinIO reading, event parsing, ClickHouse writing, and table-specific extractors into separate packages.
   - Preserve the current ingestion behavior first; change structure before adding more tables.

2. Add SQL-to-work traceability tables.
   - Add `spark_jobs`.
   - Add `spark_sql_execution_jobs`.
   - Use these as the bridge from SQL execution -> job -> stage -> task.

3. Add adaptive physical plan history.
   - Normalize `org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate`.
   - Store both the plan text and `sparkPlanInfo` JSON.
   - This is needed because the physical plan is execution-level data, while tasks only carry runtime metrics.

4. Add small fixture-based loader tests.
   - Use captured Spark event-log JSON snippets.
   - Test parsing and ClickHouse row mapping without needing Docker.
   - Keep full Compose E2E tests for integration validation only.

## Spark Connect Track

Classic `spark-submit` remains the primary execution path for v0.

Spark Connect is still useful later for agent-driven interactive sessions, but it should be treated as an additional control plane, not as the first replacement for `spark-submit`.

Good future checks:

- Confirm Spark Connect sessions emit equivalent Spark event logs for SQL/stage/task analysis.
- Define how agents set application names, session metadata, and experiment IDs.
- Decide whether Spark Connect workloads should reuse the same `spark-logs` bucket and ClickHouse loader path.

## Codex Suggestion

These are low-hanging improvements I would add next for a Spark optimization and agent-study platform:

- Add a small `src/workloads/` catalog with named workloads, parameters, expected data paths, and app-name conventions.
- Add experiment metadata fields to ClickHouse tables or a separate `spark_experiments` table so optimization runs can be compared cleanly.
- Add query examples for common questions: slowest task, biggest shuffle, SQL execution timeline, and plan changes after AQE.
- Keep ClickStack as the main UI for now; avoid building a custom dashboard until the observability model stabilizes.

## Current Non-Goals

- Do not move Spark data logs into ClickHouse directly from Spark.
- Do not replace `spark-submit` with Spark Connect yet.
- Do not depend on `docs/old-brainstorm` for the final project flow.
- Do not download jars during image builds; `make bootstrap` owns dependency download.
