# Spark Logs Info

This folder documents what Spark observability data is currently available in ClickHouse, how to query it from ClickStack, and what is still only available as raw Spark event-log JSON.

## Current Coverage

The Go loader ingests Spark event log files from MinIO into ClickHouse. For every event-log line it can parse, it stores the full raw JSON in `spark_raw_events`. This means the raw event-log coverage is intentionally broad: if Spark wrote the event into the event log and the loader read that event-log file, the original JSON should be available in ClickHouse.

The loader also normalizes a smaller subset into first-class tables:

- `spark_sql_executions`: SQL execution start events and their initial physical plan text.
- `spark_sql_execution_ends`: SQL execution end events and error text when present.
- `spark_stages`: completed stage-level metadata.
- `spark_tasks`: completed task-level metrics.
- `spark_eventlog_files`: ingested file metadata and idempotency state.

## Important Distinction

The physical plan is attached to a SQL execution, not to each individual task.

```text
Spark application
  -> SQL execution
      -> physical plan
      -> jobs
          -> stages
              -> tasks
```

Tasks contain execution metrics for work that ran inside stages. They do not each contain a full physical plan. To understand what really happened, query both the SQL plan data and task/stage metrics.

## Best Entry Points

Use these documents in order:

1. [ClickStack Guide](clickstack-guide.md)
2. [Current ClickHouse Tables](current-tables.md)
3. [Spark Event Log Coverage](spark-event-log-coverage.md)
4. [Exploration Roadmap](exploration-roadmap.md)

Project-level next steps live in `../next-steps.md`.
