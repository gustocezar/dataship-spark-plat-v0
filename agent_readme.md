# Agent README

Context marker: `2026-05-24 18:00:27 BST`

Project score: `8/10`

This file is a handoff note for another agent starting in a new session.

## Boundaries

- Work inside `/home/philot/compendium/forge/dataship/spark-plat-v0` unless the user explicitly changes scope.
- Keep project files and docs in English.
- Do not remove local jars, Python wheels, or vendored Go dependencies unless the user asks.
- Keep Docker image builds based on project-local images and project-local dependency caches.
- Treat `docs/old-brainstorm` as historical validation notes only.

## Project Purpose

This is a local Spark optimization and observability study platform for future agent workflows.

Current stack:

- Spark `4.1.2`
- Delta Lake `4.2.0`
- MinIO for lakehouse data and Spark event logs
- Spark History Server reading event logs from MinIO
- ClickHouse for raw and normalized Spark execution logs
- ClickStack or another ClickHouse UI can query those logs externally
- Go `eventlog-loader` image, run on demand
- Python project tooling through `uv`

Classic `spark-submit` is the primary path for v0. Spark Connect is a future track for interactive agent sessions. Job-specific Spark app names should be declared in runner scripts and passed to `SparkSessionFactory.get_or_create`.

## Read These First

- `README.md`: quick start and service roles.
- `docs/dev-design/architecture.md`: architecture decisions and component responsibilities.
- `docs/dev-design/operations.md`: operational commands, cleanup behavior, credentials, and service access.
- `docs/dev-design/design-pattern-v0-disccusion.md`: agentic Spark optimization guardrail rationale and design discussion.
- `docs/machine-requirements.md`: required host tools, validated versions, and install notes.
- `docs/dev-design/compatibility.md`: version compatibility notes.
- `docs/next-steps.md`: near-term roadmap and Codex suggestions.
- `docs/logs-info/README.md`: entry point for Spark log and ClickHouse observability docs.
- `src/apps/sample_scripts/README.md`: sample job segmentation and script contract.
- `MEMORY.md`: compact decision history.

## Useful Commands

- `make bootstrap`: download jars, Python wheels, and Go vendor dependencies once.
- `make build`: build all project-local Docker images.
- `make validate`: validate required local images before Compose.
- `make compose`: start the platform and run readiness checks.
- `make ingest-landing`: submit only the sample customer landing ingestion.
- `make bronze`: submit only the customer landing-to-bronze SparkPlatJob.
- `make sanity`: submit only the customer sanity validation.
- `make smoke`: submit landing ingestion, the bronze Delta job, and the sanity check, then validate MinIO plus Spark History.
- `make spark-logs`: run the Go loader and validate ClickHouse ingestion.
- `make services`: print service URLs, credentials, and UI click paths.
- `make tests`: run fast Python unit tests through `uv` with fake Spark objects.
- `make test`: alias for `make tests`.
- `make down`: stop Compose without deleting local data.
- `make clean-data`: delete local MinIO/ClickHouse/metrics state, including a Docker-root fallback for root-owned bind-mounted files.
- `make removeimage`: remove local project images only; caches remain.

## Key Files

- `.env.example`: default configuration and image names.
- `Makefile`: main local workflow contract.
- `build/docker-compose.yml`: final Compose stack.
- `build/scripts/`: bootstrap, validation, readiness, smoke, services, and ClickHouse checks.
- `build/images/spark/`: Spark runtime image and Spark Python requirements. The final runtime user is `spark` (`uid=185`).
- `build/images/spark-history/`: Spark History image wrapper. The final runtime user is `spark` (`uid=185`).
- `build/images/minio/`: MinIO server/client wrappers and bucket init.
- `build/images/clickhouse/`: ClickHouse wrapper image.
- `build/images/eventlog-loader/main.go`: current monolithic loader entrypoint.
- `build/clickhouse/init/001_spark_observability.sql`: ClickHouse observability schema.
- `build/config/spark/`: Spark defaults, logging config, and local jars.
- `src/config/lakehouse.yaml`: lakehouse layers and entity-level read/write config.
- `src/apps/sample_scripts`: current sample workloads: landing ingestion, bronze job, and sanity check.
- `src/apps/template.md`: contract template for future Spark scripts.
- `src/spark_platform/config/loader.py`: YAML config loading and env expansion.
- `src/spark_platform/session/factory.py`: Delta-enabled SparkSession factory.
- `src/spark_platform/io/specs.py`: read/write spec validation for testable IO.
- `src/spark_platform/io/datasets.py`: simple read/write helpers.
- `src/spark_platform/jobs/base.py`: SparkPlatJob ABC/template for app scripts.
- `src/spark_platform/utils/logger.py`: project logger.
- `src/spark_platform/utils/plan_debug.py`: optional commented physical-plan helper.
- `tests/fakes/spark.py`: fake Spark fluent API objects for unit tests without a Spark cluster.
- `docs/assets/spark-platform-v0-visual-prompt.md`: detailed prompt for generating the README architecture asset.

## Observability Model

Spark writes event logs to MinIO bucket `spark-logs`, prefix `events/`.

Spark writes lakehouse data to MinIO bucket `lakehouse` using `landing/`, `bronze/`, `silver/`, and `gold/` prefixes. The current customer sample writes JSON to landing and Delta to bronze.

The Go loader reads Spark event logs from MinIO and inserts:

- broad raw JSON into `spark_raw_events`
- SQL start/end rows
- completed stage rows
- completed task rows
- ingested file metadata

The current physical plan is stored at SQL execution level. Tasks do not each contain full physical plans; tasks provide runtime metrics underneath stages.

## Operational Notes

- `make smoke` currently validates landing JSON, bronze Delta, Spark History indexing, MinIO event logs, and the sanity job.
- `make spark-logs` loads event logs into ClickHouse and validates table counts.
- `make clean-data` may use a temporary Docker-root cleanup path because MinIO and ClickHouse bind mounts can contain root-owned files.
- Spark runtime and Spark History were updated to return to `uid=185(spark)` after image build steps.
- `docs/old-brainstorm` is preserved only as historical context; do not build new runtime behavior from it.

## Next Best Work

1. Refactor the Go loader into packages by responsibility and normalized entity.
2. Add `spark_jobs` and `spark_sql_execution_jobs` normalized tables.
3. Add adaptive execution plan updates.
4. Add event-log fixture tests for loader parsing.
5. Keep Spark Connect as a later control-plane experiment.

The last validated E2E state successfully ran image removal, bootstrap, build, compose, Spark user checks, smoke, MinIO verification, Spark History verification, and ClickHouse ingestion. After that validation, the stack and project images were removed again at the user's request.
