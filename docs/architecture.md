# Architecture

## Decision Summary

Spark Platform v0 uses MinIO as the durable raw object store and ClickHouse as the analytical store for parsed Spark execution observability.

The core data flow is:

```text
Spark jobs -> MinIO lakehouse bucket -> Delta tables
Spark event logs -> MinIO spark-logs bucket -> Spark History
Spark event logs -> Go eventlog-loader -> ClickHouse
```

## Image Model

All services run from project-local images after `make build`. The base images are still official upstream images, but Compose points only at local project tags so `make compose` does not silently pull or drift. Each Docker build uses only its own image directory as context; no image build receives the full project tree.

Spark master and workers share the same runtime image, `spark-plat-v0-spark:4.1.2`. This is intentional: with the current official Spark 4.1.x image, master, worker, submit client, and History Server are all commands from the same Spark distribution.

Spark History uses a thin dedicated image, `spark-plat-v0-spark-history:4.1.2`, that extends the Spark runtime image and only changes the entrypoint. This keeps parity with the older formation layout without duplicating Spark, jars, or Python dependencies.

MinIO has two image definitions under `build/images/minio` because MinIO server and MinIO client are separate upstream images. The client image contains `init-buckets.sh`, which waits for the MinIO API, creates the `lakehouse` and `spark-logs` buckets, creates `bronze/`, `silver/`, and `gold/` lakehouse prefixes, and creates the `events/` prefix used by Spark event logs.

## Dependency Model

Spark JVM dependencies are resolved once by `make bootstrap` into `build/config/spark/jars`. Before `make build`, `build/scripts/prepare-image-contexts.sh` stages the Spark-only build context under `build/images/spark/context`. The Spark runtime image copies these staged jars and does not use Maven during `make build` or `spark-submit`.

Spark Python dependencies live in `build/images/spark/requirements.txt`. Bootstrap downloads wheels into `build/cache/python-wheels`, and the Spark image installs from that local wheel cache. `pyspark` and `delta-spark` are intentionally not installed by pip because the official Spark image owns PySpark and Delta is provided by Spark/Scala jars.

## Why Event Logs Go To MinIO First

`spark.eventLog.dir` expects a filesystem-compatible destination such as `file://`, HDFS, or `s3a://`. ClickHouse is an analytical database, not a Hadoop-compatible filesystem. Pointing Spark directly at ClickHouse is not a valid event-log target.

Keeping raw event logs in MinIO gives us a replayable source of truth. If the parser changes, the ClickHouse schema evolves, or a loader bug is found, the raw logs can be reprocessed without rerunning Spark jobs.

## Why The Loader Is Manual

The loader runs on demand through `make spark-logs`. It is intentionally not a long-running service for v0 because the local workflow is easier to reason about:

1. Start infrastructure with `make compose`.
2. Submit or run Spark jobs.
3. Load event logs into ClickHouse with `make spark-logs`.

This keeps failure modes clear and makes the command easy to wrap later in a richer Make workflow or CI job.


## Local State And Permissions

The Compose stack keeps MinIO and ClickHouse state in project bind mounts under `build/var` so users can inspect local data without Docker volume tooling. The tradeoff is host-file ownership friction: stateful services may write files as container users/root.

This was observed during local cleanup on May 24, 2026, when MinIO and ClickHouse files could not be removed by the host user. The operational mitigation is documented in `docs/operations.md` under `Permission Risk Note`.

## Buckets

- `lakehouse`: Delta data and demo datasets.
- `spark-logs`: Spark event logs under `events/`.

## Observability Tables

ClickHouse stores both raw and normalized layers:

- `spark_eventlog_files`: idempotency and ingestion metadata.
- `spark_raw_events`: raw Spark listener events.
- `spark_sql_executions`: SQL execution starts and physical plans.
- `spark_sql_execution_ends`: SQL execution end events.
- `spark_stages`: completed Spark stages.
- `spark_tasks`: task-level metrics.

## References

- Spark History Server documentation: https://spark.apache.org/docs/latest/monitoring.html
- Spark application submission and dependency packaging: https://spark.apache.org/docs/4.0.0/submitting-applications.html
- Spark 4.1.2 downloads and runtime notes: https://spark.apache.org/downloads.html
- Spark official Docker image tags: https://hub.docker.com/_/spark
- Delta Spark 4.1 / Scala 2.13 Maven artifacts: https://repo1.maven.org/maven2/io/delta/

## Deferred Work

Spark Connect is intentionally deferred. The v0 runtime uses classic `spark-submit` because it is the most direct path for validating History Server, event logs, Delta, and ClickHouse ingestion.

## Reusable Spark App Utilities

Spark application code lives under `src/`. The current utility layer is intentionally small and testable without a Spark cluster:

- `src/config/lakehouse.yaml` defines lakehouse layers and entity read/write parameters.
- `src/spark_platform/session/factory.py` owns Spark session defaults and lets scripts pass extra configuration.
- `src/spark_platform/io/specs.py` validates IO specs used by dataset helpers.
- `src/spark_platform/io/datasets.py` provides the stable public read/write helpers.
- `src/spark_platform/jobs/base.py` defines the ABC/template contract used by app scripts.
- `tests/fakes/spark.py` provides fake Spark reader/writer objects for fast unit tests.

This keeps smoke and future workloads focused on pipeline behavior instead of repeating Spark, path, and IO setup.

Package boundary note: `session/` owns Spark runtime lifecycle; `jobs/` owns the app execution contract; `io/` owns dataset specs and Spark reader/writer calls. Job-specific app names stay in runner scripts, not in shared lakehouse config.
