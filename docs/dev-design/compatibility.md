# Compatibility Notes

## Runtime Versions

- Spark: `4.1.2`
- Spark base image: `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu`
- Spark runtime image: `spark-plat-v0-spark:4.1.2`
- Spark History image: `spark-plat-v0-spark-history:4.1.2`
- Scala: `2.13`
- Java: `17`
- Delta Lake: `4.2.0`
- Hadoop AWS connector: `3.4.2`
- MinIO base image: `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1`
- MinIO client base image: `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z-cpuv1`
- ClickHouse base image: `clickhouse:26.5.1-jammy`
- Go loader build image: `golang:1.26-bookworm`

## Current Spark 4.1 Practice

The Spark 4.1.x official image already contains the Spark distribution used to launch master, worker, submit, and History Server commands. A separate History image is not technically required, but v0 keeps a thin History image for an explicit service boundary and for parity with the older formation layout.

Local jars are still important. Spark supports dependency resolution at submit time, but this platform needs a consistent classpath for Spark jobs and for History Server. History reads event logs from `s3a://spark-logs/events`, so S3A and Hadoop AWS jars must be available to the History process, not only to a submitted job.

## Local Jar Cache

Spark jars are cached in `build/config/spark/jars`. `make build` stages them into `build/images/spark/context/jars`, and the Spark image copies jars from that image-local context. It never resolves Maven dependencies during `make build`.

`make bootstrap` creates `build/config/spark/jars/.bootstrap-manifest` after resolving dependencies. Later bootstrap runs validate the manifest and skip downloading when all listed jars exist.

## Python Requirements

Spark Python requirements live in `build/images/spark/requirements.txt`. Bootstrap downloads wheels into `build/cache/python-wheels`; build stages them into `build/images/spark/context/python-wheels` and installs from that local wheel cache.

Do not add `pyspark` or `delta-spark` to this file without a specific compatibility test. The official Spark image owns PySpark for Spark 4.1.2, and Delta compatibility is controlled by the `delta-spark_4.1_2.13` jar.

## S3A And Delta

Spark and Spark History both need the S3A classpath because event logs live at `s3a://spark-logs/events`. If Hadoop AWS dependencies are only passed through `spark-submit`, History Server cannot read the event logs. This is why jars are baked into the Spark runtime image.

For v0, Delta writes to MinIO are intended for a single local Spark cluster and a single writer per table. Concurrent multi-cluster writes to the same Delta table need additional coordination beyond this lab.

## Event Log Format

Spark 4 event logs use event log v2 and may be Zstandard-compressed. The Go loader reads `.zstd` event log files and stores both raw JSON events and selected normalized projections in ClickHouse.
