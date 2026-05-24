CREATE DATABASE IF NOT EXISTS spark_observability;

CREATE TABLE IF NOT EXISTS spark_observability.spark_eventlog_files
(
    bucket String,
    object_key String,
    etag String,
    size UInt64,
    last_modified DateTime64(3, 'UTC'),
    line_count UInt64,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (bucket, object_key, etag);

CREATE TABLE IF NOT EXISTS spark_observability.spark_raw_events
(
    event_uid String,
    app_id String,
    event_type LowCardinality(String),
    event_time_ms UInt64,
    bucket String,
    object_key String,
    line_no UInt64,
    raw String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (app_id, event_type, event_uid);

CREATE TABLE IF NOT EXISTS spark_observability.spark_sql_executions
(
    app_id String,
    execution_id UInt64,
    description String,
    details String,
    physical_plan String,
    start_time_ms UInt64,
    event_uid String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (app_id, execution_id);

CREATE TABLE IF NOT EXISTS spark_observability.spark_sql_execution_ends
(
    app_id String,
    execution_id UInt64,
    end_time_ms UInt64,
    error_message String,
    event_uid String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (app_id, execution_id, event_uid);

CREATE TABLE IF NOT EXISTS spark_observability.spark_stages
(
    app_id String,
    stage_id UInt64,
    stage_attempt_id UInt64,
    stage_name String,
    num_tasks UInt64,
    submission_time_ms UInt64,
    completion_time_ms UInt64,
    event_uid String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (app_id, stage_id, stage_attempt_id);

CREATE TABLE IF NOT EXISTS spark_observability.spark_tasks
(
    app_id String,
    stage_id UInt64,
    stage_attempt_id UInt64,
    task_id UInt64,
    task_index UInt64,
    task_attempt UInt64,
    executor_id String,
    host String,
    launch_time_ms UInt64,
    finish_time_ms UInt64,
    duration_ms UInt64,
    task_type LowCardinality(String),
    successful UInt8,
    reason String,
    executor_run_time_ms UInt64,
    executor_cpu_time_ns UInt64,
    peak_execution_memory UInt64,
    input_bytes UInt64,
    input_records UInt64,
    output_bytes UInt64,
    output_records UInt64,
    shuffle_read_bytes UInt64,
    shuffle_write_bytes UInt64,
    event_uid String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (app_id, stage_id, stage_attempt_id, task_id, event_uid);
