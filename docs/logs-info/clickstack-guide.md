# ClickStack Guide

## Source Setup

When adding a source in ClickStack for the current v0 stack:

- Source data type: `Log`
- Database: `spark_observability`
- Table: `spark_raw_events`
- Timestamp column: `ingested_at`
- Default select:

```sql
ingested_at AS Timestamp, raw AS Body
```

This makes ClickStack display raw Spark listener events as log bodies while still exposing fields such as `event_type`, `app_id`, `event_uid`, `bucket`, and `object_key` as searchable columns.

## Where To Type Filters

In the ClickStack Search screen, the top bar in `SQL` mode expects a SQL `WHERE` clause, not a full `SELECT` query.

Examples:

```sql
app_id = 'app-20260524002159-0001'
```

```sql
event_type = 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'
```

```sql
event_type = 'org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate'
```

```sql
event_type = 'SparkListenerTaskEnd'
```

## Useful Event Filters

Initial SQL physical plans:

```sql
event_type = 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'
```

Adaptive physical plan updates:

```sql
event_type = 'org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate'
```

Completed task metrics:

```sql
event_type = 'SparkListenerTaskEnd'
```

Stage completion metadata:

```sql
event_type = 'SparkListenerStageCompleted'
```

Job-to-stage and SQL execution linkage:

```sql
event_type = 'SparkListenerJobStart'
```

Application lifecycle:

```sql
event_type IN ('SparkListenerApplicationStart', 'SparkListenerApplicationEnd')
```

## ClickHouse SQL Examples

Use ClickHouse HTTP, `clickhouse-client`, or a SQL editor when you need full `SELECT` queries.

List event types:

```sql
SELECT event_type, count()
FROM spark_raw_events
GROUP BY event_type
ORDER BY count() DESC;
```

Read current normalized SQL plans:

```sql
SELECT
  app_id,
  execution_id,
  description,
  physical_plan
FROM spark_sql_executions
ORDER BY start_time_ms DESC;
```

Find heavy task metrics:

```sql
SELECT
  app_id,
  stage_id,
  task_id,
  duration_ms,
  executor_run_time_ms,
  shuffle_read_bytes,
  shuffle_write_bytes,
  input_bytes,
  output_bytes
FROM spark_tasks
ORDER BY duration_ms DESC
LIMIT 50;
```
