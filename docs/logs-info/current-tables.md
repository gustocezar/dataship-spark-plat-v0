# Current ClickHouse Tables

## `spark_eventlog_files`

Tracks event-log files that were ingested. The loader uses this table to avoid re-ingesting the same object when the bucket, object key, and ETag already exist.

Main columns:

- `bucket`
- `object_key`
- `etag`
- `size`
- `last_modified`
- `line_count`
- `ingested_at`

## `spark_raw_events`

Stores every parsed Spark event-log JSON line as raw text.

Main columns:

- `event_uid`: loader-generated event identity.
- `app_id`: Spark application id inferred from event content or object path.
- `event_type`: Spark listener event type.
- `event_time_ms`: event timestamp when the loader can infer it.
- `bucket`, `object_key`, `line_no`: source location.
- `raw`: original Spark event JSON.
- `ingested_at`: ClickHouse ingestion time.

Use this table when the event type is not normalized yet.

## `spark_sql_executions`

Stores SQL execution start events.

Main columns:

- `app_id`
- `execution_id`
- `description`
- `details`
- `physical_plan`
- `start_time_ms`
- `event_uid`
- `ingested_at`

The `physical_plan` column is the SQL-execution-level plan from `physicalPlanDescription`. It is not a per-task plan.

## `spark_sql_execution_ends`

Stores SQL execution end events.

Main columns:

- `app_id`
- `execution_id`
- `end_time_ms`
- `error_message`
- `event_uid`
- `ingested_at`

## `spark_stages`

Stores completed stage metadata from `SparkListenerStageCompleted`.

Main columns:

- `app_id`
- `stage_id`
- `stage_attempt_id`
- `stage_name`
- `num_tasks`
- `submission_time_ms`
- `completion_time_ms`
- `event_uid`
- `ingested_at`

## `spark_tasks`

Stores completed task metrics from `SparkListenerTaskEnd`.

Main columns:

- Task identity: `app_id`, `stage_id`, `stage_attempt_id`, `task_id`, `task_index`, `task_attempt`.
- Location: `executor_id`, `host`.
- Time: `launch_time_ms`, `finish_time_ms`, `duration_ms`, `executor_run_time_ms`, `executor_cpu_time_ns`.
- Status: `task_type`, `successful`, `reason`.
- Memory and I/O: `peak_execution_memory`, `input_bytes`, `input_records`, `output_bytes`, `output_records`, `shuffle_read_bytes`, `shuffle_write_bytes`.
- Source: `event_uid`, `ingested_at`.
