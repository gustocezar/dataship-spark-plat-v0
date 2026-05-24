# Spark Event Log Coverage

## What The Loader Gets Today

The loader reads Spark event-log objects from the MinIO `spark-logs` bucket under the `events/` prefix. It reads Zstandard-compressed event log files and writes parsed JSON lines to `spark_raw_events`.

For the current smoke runs, these event types are present in `spark_raw_events`:

```text
SparkListenerTaskStart
SparkListenerTaskEnd
org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
SparkListenerStageExecutorMetrics
SparkListenerJobEnd
SparkListenerJobStart
SparkListenerStageCompleted
SparkListenerStageSubmitted
org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
SparkListenerExecutorMetricsUpdate
SparkListenerBlockManagerAdded
SparkListenerLogStart
SparkListenerApplicationEnd
SparkListenerEnvironmentUpdate
SparkListenerExecutorAdded
SparkListenerUnpersistRDD
SparkListenerApplicationStart
SparkListenerResourceProfileAdded
```

The exact list changes with workload type and Spark configuration.

## What Is Normalized Today

The loader currently normalizes these event families:

- `SparkListenerSQLExecutionStart` -> `spark_sql_executions`
- `SparkListenerSQLExecutionEnd` -> `spark_sql_execution_ends`
- `SparkListenerStageCompleted` -> `spark_stages`
- `SparkListenerTaskEnd` -> `spark_tasks`

Everything else is still queryable through `spark_raw_events.raw`.

## Physical Plan Semantics

`physicalPlanDescription` describes a SQL execution plan. It is associated with `executionId` and is not emitted once per task.

With adaptive query execution enabled, Spark may emit updates through:

```text
org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate
```

Those events can contain:

- `executionId`
- `physicalPlanDescription`
- `sparkPlanInfo`
- operator tree metadata
- operator-level metric definitions

Those adaptive updates are currently available in `spark_raw_events`, but they are not yet normalized into a dedicated table.

## What Spark Also Generates That We Do Not Normalize Yet

The raw JSON already contains more than the current normalized tables expose. Useful candidates include:

- Application lifecycle: `SparkListenerApplicationStart`, `SparkListenerApplicationEnd`.
- Environment and Spark properties: `SparkListenerEnvironmentUpdate`.
- Job-level metadata and job-to-stage links: `SparkListenerJobStart`, `SparkListenerJobEnd`.
- Stage submitted metadata and RDD lineage: `SparkListenerStageSubmitted`.
- Task start timing and locality: `SparkListenerTaskStart`.
- Adaptive SQL plan updates: `SparkListenerSQLAdaptiveExecutionUpdate`.
- Driver accumulator updates: `SparkListenerDriverAccumUpdates`.
- Executor and block manager lifecycle: `SparkListenerExecutorAdded`, `SparkListenerBlockManagerAdded`.
- Executor metrics updates: `SparkListenerExecutorMetricsUpdate`, `SparkListenerStageExecutorMetrics`.

## What Is Outside Spark Event Logs

The loader does not currently ingest non-event-log artifacts such as:

- Driver stdout/stderr or log4j text logs.
- Executor stdout/stderr or log4j text logs.
- Container runtime logs from Docker.
- Spark History Server process logs.
- MinIO or ClickHouse service logs.

Those are separate log streams. If we need them later, they should be modeled as a separate ingestion path from the Spark event-log loader.

## Answer: Do We Already Capture Everything Spark Generates?

No, not if "everything" means every log stream Spark can emit.

Yes, for the current v0 event-log objective, the loader stores the raw Spark event-log events it reads. That gives us replayable coverage for event-log analytics.

The remaining work is not primarily about capturing more raw event-log data. It is about normalizing more event types into first-class ClickHouse tables so they are easier to query and join.
