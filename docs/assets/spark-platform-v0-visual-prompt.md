# Spark Platform v0 Visual Asset Prompt

Target output filename after generation: `docs/assets/spark-platform-v0-architecture.png`

Use this prompt to generate a high-resolution visual asset for the project README. The asset should explain the platform architecture and engineering practices in one dense but readable technical diagram.

## Primary Prompt

Create a polished, high-resolution technical architecture visual for a local data platform called "Spark Platform v0". The visual should feel like a serious engineering system map for a Spark optimization and observability lab, not a marketing hero. It should clearly show a local Docker Compose platform that runs Apache Spark 4.1.2, Delta Lake 4.2.0, MinIO, Spark History Server, ClickHouse, and a small Go event-log loader.

Design the image as a clean left-to-right architecture diagram with three horizontal lanes:

1. Control and build lane at the top.
2. Lakehouse data flow lane in the middle.
3. Observability and operations lane at the bottom.

The composition should be readable at README width but detailed enough for a 4K asset. Use crisp vector-like shapes, small labeled service cards, directional arrows, subtle grid alignment, and concise labels. Keep labels short and legible. Use a restrained dark-neutral or light-neutral engineering palette with accent colors for each technology: Spark in warm orange, Delta Lake in blue/teal, MinIO in red, ClickHouse in yellow/black, Go loader in cyan, Docker/Compose in blue, Python/uv/testing in violet or green. Avoid a one-color theme.

## Architecture Content To Show

Show the local workstation as the starting point on the far left. Label it "Developer workstation" and "Makefile-driven local lab". From the workstation, show a command/control strip with these Make targets as compact chips or command badges:

- `make bootstrap`
- `make build`
- `make validate`
- `make compose`
- `make smoke`
- `make spark-logs`
- `make services`
- `make tests`
- `make clean-data`
- `make removeimage`

Under the control strip, show the dependency and build model:

- `build/config/spark/jars` for local Spark JVM jars
- `build/cache/python-wheels` for local Python wheels
- `build/images/eventlog-loader/vendor` for vendored Go dependencies
- project-local Docker images only after build
- official pinned base images used during bootstrap/build

Represent the local Docker Compose stack as a framed platform area named "Docker Compose: spark-plat-v0". Inside it, include service cards for:

- `spark-master`, Apache Spark 4.1.2, runs as `uid=185(spark)`
- `spark-worker`, Apache Spark 4.1.2, runs as `uid=185(spark)`
- `spark-history`, Spark History Server, reads event logs from MinIO, runs as `uid=185(spark)`
- `minio`, object storage for lakehouse and Spark event logs
- `minio-init`, creates buckets and prefixes
- `clickhouse`, analytical database for Spark observability
- `eventlog-loader`, small Go image, manual one-shot loader

Show the project-local image tags near the Compose area:

- `spark-plat-v0-spark:4.1.2`
- `spark-plat-v0-spark-history:4.1.2`
- `spark-plat-v0-minio:2025-09-07`
- `spark-plat-v0-minio-mc:2025-08-13`
- `spark-plat-v0-clickhouse:26.5.1`
- `spark-plat-v0-eventlog-loader:go1.26`

## Data Flow To Show

In the middle lane, show the lakehouse medallion flow through MinIO bucket `lakehouse`:

- sample ingestion script: `simple_persist_customers_landing.py`
- writes JSON to `s3a://lakehouse/landing/customer`
- contract-based job: `smoke_job_plat_minio.py`
- reads landing JSON
- uses `SparkPlatJob` and DataFrame `.transform(...)`
- writes Delta to `s3a://lakehouse/bronze/customer`
- future medallion prefixes shown as available paths: `silver/customer`, `gold/customer`
- sanity script: `check_sanity.py`
- validates landing rows, bronze rows, expected columns, grouped execution

Use arrows to show:

`sample ingest -> landing JSON -> bronze SparkPlatJob -> bronze Delta -> sanity check`

Make the Delta table visibly different from raw JSON. For example, JSON files can appear as plain document tiles, while Delta can appear as a table block with `_delta_log` and parquet tiles.

## Reusable Spark Application Layer

Add a code/framework panel near the Spark jobs named "Reusable Spark app utilities". Inside it, show these modules as small connected blocks:

- `src/config/lakehouse.yaml`: entity and layer IO config
- `config/loader.py`: loads YAML and expands env vars
- `session/factory.py`: Delta-enabled SparkSession defaults
- `io/specs.py`: read/write spec validation
- `io/datasets.py`: Delta/JSON read helpers and Delta/JSON write helpers
- `jobs/base.py`: `SparkPlatJob` ABC/template
- `utils/logger.py`: consistent app logging
- `utils/plan_debug.py`: optional local plan inspection reference

Show that sample scripts import these utilities instead of repeating Spark setup, paths, and IO boilerplate.

## Observability Flow To Show

In the bottom lane, show Spark event logs flowing separately from lakehouse data:

- Spark jobs write event logs to MinIO bucket `spark-logs`, prefix `events/`
- Spark History reads `s3a://spark-logs/events`
- `eventlog-loader` reads the same event-log files from MinIO
- loader writes to ClickHouse database `spark_observability`

Inside ClickHouse, show a compact table list:

- `spark_eventlog_files`
- `spark_raw_events`
- `spark_sql_executions`
- `spark_sql_execution_ends`
- `spark_stages`
- `spark_tasks`

Add callouts explaining the observability semantics:

- raw Spark listener JSON is retained for replay
- SQL physical plans are stored at SQL execution level
- stages and tasks provide execution metrics under each SQL/job execution
- task rows do not each contain a full physical plan
- loader is manual in v0: `make spark-logs`

Also show ClickStack or a generic ClickHouse UI outside the Compose stack as an optional viewer connected to ClickHouse. Label it "ClickStack / UI: query raw events, SQL plans, stages, tasks".

## Testing And DataOps Practices To Show

Add a quality and operations ribbon or side panel with these practices:

- `uv` manages Python project dependencies
- `make tests` runs fast unit tests without a Spark cluster
- fake Spark reader/writer objects validate PySpark fluent API behavior
- `make smoke` validates landing, bronze, sanity, MinIO, Spark History, event logs
- `make spark-logs` validates ClickHouse ingestion
- `make clean-data` cleans local bind-mounted state and has a Docker-root fallback for root-owned files
- Spark runtime and Spark History return to upstream `uid=185(spark)` after image build steps
- jars, wheels, and Go vendor caches are preserved across image removal

Show `clean-data` as a small maintenance tool, not as a main data flow. Indicate that MinIO and ClickHouse bind mounts can create root-owned files, so cleanup has a safe fallback.

## Version And Boundary Labels

Include a small version badge cluster:

- Spark 4.1.2
- Delta Lake 4.2.0
- ClickHouse 26.5.1
- MinIO 2025-09-07
- Go 1.26 loader
- Python tests with uv

Include a boundary note:

- v0 uses classic `spark-submit`
- Spark Connect is deferred for future interactive agent control-plane work
- Spark event logs go to MinIO first, not directly to ClickHouse
- ClickHouse is analytical storage, not a Hadoop-compatible event-log filesystem

## Visual Style Requirements

Use a professional technical diagram style. It should look like an architecture asset from a senior data platform team. Use subtle shadows, consistent spacing, and precise arrows. Make the central Compose area visually dominant. Avoid decorative blobs, oversized hero typography, cartoon characters, stock photos, or vague abstract clouds. Do not use exact vendor logos if licensing is uncertain; use labeled service badges and color accents instead. Make text readable and aligned. Avoid tiny unreadable labels.

Recommended aspect ratio: 16:9. Recommended size: 3840 x 2160. Keep enough top and bottom margin for README rendering. Do not crop arrows or labels. Leave a small title at the top: "Spark Platform v0 - Local Spark Lakehouse and Observability Lab".

## Negative Prompt

Do not create a marketing landing page. Do not show a generic cloud architecture with AWS services unless they are clearly represented only as S3A-compatible object storage behavior inside MinIO. Do not invent Kubernetes, Airflow, Kafka, Databricks, Iceberg, Trino, or cloud services that are not in the project. Do not show Spark Connect as active. Do not show logs going directly from Spark into ClickHouse. Do not show physical plans at task level. Do not show MinIO as the ClickHouse storage layer. Do not use random database names or unpinned versions. Do not include unreadable paragraphs inside the image. Do not include real passwords or secrets. Do not include people, mascots, decorative animals, or fantasy elements.

## Short Alt Text

Spark Platform v0 architecture diagram showing Make-driven Docker Compose services for Spark 4.1.2, Delta Lake, MinIO lakehouse data, Spark event logs, Spark History, Go event-log loading, ClickHouse observability tables, and uv-backed tests.
