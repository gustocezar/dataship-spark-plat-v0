# Agent README

Context marker: `2026-05-24 01:57:14 BST`

Project score: `7.5/10`

This file is a handoff note for another agent starting in a new session.

## Boundaries

- Work inside `/home/philot/compendium/forge/dataship/spark-plat-v0` unless the user explicitly changes scope.
- Do not use final project code from `docs/old-brainstorm`; it is historical validation material only.
- Keep project files and docs in English.
- Do not remove local jars, Python wheels, or vendored Go dependencies unless the user asks.
- Keep Docker image builds based on project-local images and project-local dependency caches.

## Project Purpose

This is a local Spark optimization and observability study platform for future agent workflows.

Current stack:

- Spark `4.1.2`
- Delta Lake `4.2.0`
- MinIO for lakehouse data and Spark event logs
- Spark History Server reading event logs from MinIO
- ClickHouse for raw and normalized Spark execution logs
- ClickStack for querying those ClickHouse logs
- Go `eventlog-loader` image, run on demand

Classic `spark-submit` is the primary path for v0. Spark Connect is a future track for interactive agent sessions. Job-specific Spark app names should be declared in runner scripts and passed to `SparkSessionFactory.get_or_create`.

## Read These First

- `README.md`: quick start and service roles.
- `docs/architecture.md`: architecture decisions and component responsibilities.
- `docs/compatibility.md`: version compatibility notes.
- `docs/operations.md`: operational commands and usage notes.
- `docs/next-steps.md`: near-term roadmap and Codex suggestions.
- `docs/logs-info/README.md`: entry point for Spark log and ClickHouse observability docs.
- `MEMORY.md`: compact decision history.

## Useful Commands

- `make bootstrap`: download jars, Python wheels, and Go vendor dependencies once.
- `make build`: build all project-local Docker images.
- `make validate`: validate required local images before Compose.
- `make compose`: start the platform and run readiness checks.
- `make smoke`: submit landing ingestion, the bronze Delta job, and the sanity check, then validate MinIO plus Spark History.
- `make spark-logs`: run the Go loader and validate ClickHouse ingestion.
- `make services`: print service URLs, credentials, and UI click paths.
- `make tests`: run fast Python unit tests through `uv` with fake Spark objects.
- `make down`: stop Compose without deleting caches.
- `make removeimage`: remove local project images only.

## Key Files

- `.env.example`: default configuration and image names.
- `build/docker-compose.yml`: final Compose stack.
- `src/apps/sample_scripts`: current sample workloads: landing ingestion, bronze job, and sanity check.
- `src/config/lakehouse.yaml`: lakehouse layers and entity-level read/write config.
- `src/spark_platform/`: reusable Spark config, session, IO, factory/spec, and logger utilities.
- `src/spark_platform/io/specs.py`: read/write spec validation for testable IO.
- `src/spark_platform/io/datasets.py`: simple read/write helpers.
- `src/spark_platform/jobs/base.py`: SparkPlatJob ABC/template for app scripts.
- `tests/fakes/spark.py`: fake Spark fluent API objects for unit tests without a Spark cluster.
- `build/images/spark/`: Spark runtime image and Spark Python requirements.
- `build/images/spark-history/`: Spark History image wrapper.
- `build/images/minio/`: MinIO server/client wrappers and bucket init.
- `build/images/clickhouse/`: ClickHouse wrapper image.
- `build/images/eventlog-loader/main.go`: current monolithic loader entrypoint.
- `build/clickhouse/init/001_spark_observability.sql`: ClickHouse observability schema.
- `build/config/spark/`: Spark defaults, logging config, and local jars.

## Observability Model

Spark writes event logs to MinIO bucket `spark-logs`, prefix `events/`.

Spark writes Delta data to MinIO bucket `lakehouse`.

The Go loader reads Spark event logs from MinIO and inserts:

- broad raw JSON into `spark_raw_events`
- SQL start/end rows
- completed stage rows
- completed task rows
- ingested file metadata

The current physical plan is stored at SQL execution level. Tasks do not each contain full physical plans; tasks provide runtime metrics underneath stages.

## Next Best Work

1. Refactor the Go loader into packages by responsibility and normalized entity.
2. Add `spark_jobs` and `spark_sql_execution_jobs`.
3. Add adaptive execution plan updates.
4. Add event-log fixture tests for loader parsing.
5. Keep Spark Connect as a later control-plane experiment.

The last validated E2E state successfully ran bootstrap, build, compose, smoke, MinIO verification, Spark History verification, and ClickHouse ingestion.
