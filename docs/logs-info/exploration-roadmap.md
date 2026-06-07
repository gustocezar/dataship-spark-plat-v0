# Exploration Roadmap

## Current Useful Questions

With the current tables, we can already answer:

- Which Spark apps were ingested?
- Which SQL executions ran?
- What initial physical plan did each SQL execution register?
- Which stages completed?
- Which tasks were slowest?
- Which tasks read, wrote, or shuffled the most data?
- Did a SQL execution end with an error?
- Which raw event types exist for a workload?

## Next Tables To Add

### `spark_applications`

Source events:

- `SparkListenerApplicationStart`
- `SparkListenerApplicationEnd`

Useful fields:

- `app_id`
- `app_name`
- `spark_user`
- `start_time_ms`
- `end_time_ms`
- `spark_version`

### `spark_jobs`

Source events:

- `SparkListenerJobStart`
- `SparkListenerJobEnd`

Useful fields:

- `app_id`
- `job_id`
- `submission_time_ms`
- `completion_time_ms`
- `result`
- `stage_ids`
- `sql_execution_id` from event properties when present

This is the main missing bridge for SQL execution -> job -> stage -> task analysis.

### `spark_sql_plan_updates`

Source event:

- `org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate`

Useful fields:

- `app_id`
- `execution_id`
- `event_uid`
- `physical_plan`
- `spark_plan_info_json`
- `event_time_ms`

This table would make adaptive-query execution analysis much easier than searching raw JSON.

### `spark_sql_plan_nodes`

Source data:

- `sparkPlanInfo` from SQL execution start and adaptive update events.

Useful fields:

- `app_id`
- `execution_id`
- `event_uid`
- `node_id`
- `parent_node_id`
- `node_name`
- `simple_string`
- `metadata_json`
- `metrics_json`

This would allow operator-level query analysis.

### `spark_executor_metrics`

Source events:

- `SparkListenerExecutorMetricsUpdate`
- `SparkListenerStageExecutorMetrics`

Useful fields:

- `app_id`
- `executor_id`
- `stage_id`
- `stage_attempt_id`
- JVM heap/non-heap metrics
- memory metrics
- process tree metrics when available

## Suggested Next Implementation Order

1. Refactor the Go loader into small packages by responsibility and normalized entity while keeping current behavior unchanged.
2. Add `spark_jobs` and `spark_sql_execution_jobs` bridge tables.
3. Add `spark_sql_plan_updates` for adaptive physical plans.
4. Add `spark_applications` for app-level context.
5. Add plan-node extraction from `sparkPlanInfo` if operator-level analysis becomes important.
6. Add executor metrics if capacity or resource analysis becomes important.

This order prioritizes SQL-to-task traceability before broader infrastructure metrics.

For the broader project roadmap, see `../next-steps.md`.
