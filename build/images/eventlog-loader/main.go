package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type rawRow struct {
	EventUID    string
	AppID       string
	EventType   string
	EventTimeMS uint64
	Bucket      string
	ObjectKey   string
	LineNo      uint64
	Raw         string
}

type sqlStartRow struct {
	AppID        string
	ExecutionID  uint64
	Description  string
	Details      string
	PhysicalPlan string
	StartTimeMS  uint64
	EventUID     string
}

type sqlEndRow struct {
	AppID        string
	ExecutionID  uint64
	EndTimeMS    uint64
	ErrorMessage string
	EventUID     string
}

type stageRow struct {
	AppID            string
	StageID          uint64
	StageAttemptID   uint64
	StageName        string
	NumTasks         uint64
	SubmissionTimeMS uint64
	CompletionTimeMS uint64
	EventUID         string
}

type taskRow struct {
	AppID               string
	StageID             uint64
	StageAttemptID      uint64
	TaskID              uint64
	TaskIndex           uint64
	TaskAttempt         uint64
	ExecutorID          string
	Host                string
	LaunchTimeMS        uint64
	FinishTimeMS        uint64
	DurationMS          uint64
	TaskType            string
	Successful          uint8
	Reason              string
	ExecutorRunTimeMS   uint64
	ExecutorCPUTimeNS   uint64
	PeakExecutionMemory uint64
	InputBytes          uint64
	InputRecords        uint64
	OutputBytes         uint64
	OutputRecords       uint64
	ShuffleReadBytes    uint64
	ShuffleWriteBytes   uint64
	EventUID            string
}

type ingestResult struct {
	RawRows      []rawRow
	SQLStarts    []sqlStartRow
	SQLEnds      []sqlEndRow
	Stages       []stageRow
	Tasks        []taskRow
	LineCount    uint64
	SkippedLines uint64
}

var appIDRE = regexp.MustCompile(`app-[0-9]+-[0-9]+`)

func main() {
	ctx := context.Background()
	logger := log.New(os.Stdout, "eventlog-loader ", log.LstdFlags|log.LUTC)

	ch, err := connectClickHouse(ctx)
	if err != nil {
		logger.Fatalf("connect clickhouse: %v", err)
	}
	if err := ensureSchema(ctx, ch); err != nil {
		logger.Fatalf("ensure schema: %v", err)
	}

	mc, err := connectMinIO()
	if err != nil {
		logger.Fatalf("connect minio: %v", err)
	}

	once := envBool("LOADER_ONCE", false)
	interval := time.Duration(envInt("LOADER_INTERVAL_SECONDS", 10)) * time.Second
	for {
		if err := ingestOnce(ctx, logger, ch, mc); err != nil {
			logger.Printf("ingest error: %v", err)
		}
		if once {
			return
		}
		time.Sleep(interval)
	}
}

func connectClickHouse(ctx context.Context) (driver.Conn, error) {
	addr := env("CLICKHOUSE_ADDR", "clickhouse:9000")
	db := env("CLICKHOUSE_DB", "spark_observability")
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: strings.Split(addr, ","),
		Auth: clickhouse.Auth{
			Database: db,
			Username: env("CLICKHOUSE_USER", "spv0"),
			Password: env("CLICKHOUSE_PASSWORD", "spv0clickhouse123"),
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}
	var lastErr error
	for i := 0; i < 60; i++ {
		if err := conn.Ping(ctx); err == nil {
			return conn, nil
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func connectMinIO() (*minio.Client, error) {
	return minio.New(env("MINIO_ENDPOINT", "minio:9000"), &minio.Options{
		Creds:  credentials.NewStaticV4(env("MINIO_ACCESS_KEY", "spv0minio"), env("MINIO_SECRET_KEY", "spv0minio123"), ""),
		Secure: envBool("MINIO_USE_SSL", false),
	})
}

func ensureSchema(ctx context.Context, conn driver.Conn) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS spark_eventlog_files (bucket String, object_key String, etag String, size UInt64, last_modified DateTime64(3, 'UTC'), line_count UInt64, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (bucket, object_key, etag)`,
		`CREATE TABLE IF NOT EXISTS spark_raw_events (event_uid String, app_id String, event_type LowCardinality(String), event_time_ms UInt64, bucket String, object_key String, line_no UInt64, raw String, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (app_id, event_type, event_uid)`,
		`CREATE TABLE IF NOT EXISTS spark_sql_executions (app_id String, execution_id UInt64, description String, details String, physical_plan String, start_time_ms UInt64, event_uid String, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (app_id, execution_id)`,
		`CREATE TABLE IF NOT EXISTS spark_sql_execution_ends (app_id String, execution_id UInt64, end_time_ms UInt64, error_message String, event_uid String, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (app_id, execution_id, event_uid)`,
		`CREATE TABLE IF NOT EXISTS spark_stages (app_id String, stage_id UInt64, stage_attempt_id UInt64, stage_name String, num_tasks UInt64, submission_time_ms UInt64, completion_time_ms UInt64, event_uid String, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (app_id, stage_id, stage_attempt_id)`,
		`CREATE TABLE IF NOT EXISTS spark_tasks (app_id String, stage_id UInt64, stage_attempt_id UInt64, task_id UInt64, task_index UInt64, task_attempt UInt64, executor_id String, host String, launch_time_ms UInt64, finish_time_ms UInt64, duration_ms UInt64, task_type LowCardinality(String), successful UInt8, reason String, executor_run_time_ms UInt64, executor_cpu_time_ns UInt64, peak_execution_memory UInt64, input_bytes UInt64, input_records UInt64, output_bytes UInt64, output_records UInt64, shuffle_read_bytes UInt64, shuffle_write_bytes UInt64, event_uid String, ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = ReplacingMergeTree(ingested_at) ORDER BY (app_id, stage_id, stage_attempt_id, task_id, event_uid)`,
	}
	for _, stmt := range statements {
		if err := conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func ingestOnce(ctx context.Context, logger *log.Logger, conn driver.Conn, mc *minio.Client) error {
	bucket := env("MINIO_BUCKET", "spark-logs")
	prefix := env("MINIO_PREFIX", "events/")
	objects := mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	for obj := range objects {
		if obj.Err != nil {
			return obj.Err
		}
		if !isEventLogObject(obj.Key) {
			continue
		}
		ingested, err := alreadyIngested(ctx, conn, bucket, obj.Key, obj.ETag)
		if err != nil {
			return err
		}
		if ingested {
			continue
		}
		result, err := readEventLog(ctx, mc, bucket, obj.Key)
		if err != nil {
			logger.Printf("skip %s: %v", obj.Key, err)
			continue
		}
		if err := insertResult(ctx, conn, result); err != nil {
			return fmt.Errorf("insert %s: %w", obj.Key, err)
		}
		if err := markFile(ctx, conn, bucket, obj, result.LineCount); err != nil {
			return err
		}
		logger.Printf("ingested object=%s lines=%d raw=%d sql=%d stages=%d tasks=%d skipped=%d", obj.Key, result.LineCount, len(result.RawRows), len(result.SQLStarts), len(result.Stages), len(result.Tasks), result.SkippedLines)
	}
	return nil
}

func isEventLogObject(key string) bool {
	if strings.HasSuffix(key, "/") || strings.Contains(key, "appstatus_") {
		return false
	}
	base := key[strings.LastIndex(key, "/")+1:]
	return strings.HasPrefix(base, "events_") || strings.Contains(base, "eventlog")
}

func alreadyIngested(ctx context.Context, conn driver.Conn, bucket, key, etag string) (bool, error) {
	var count uint64
	err := conn.QueryRow(ctx, "SELECT count() FROM spark_eventlog_files WHERE bucket = ? AND object_key = ? AND etag = ?", bucket, key, etag).Scan(&count)
	return count > 0, err
}

func readEventLog(ctx context.Context, mc *minio.Client, bucket, key string) (ingestResult, error) {
	obj, err := mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return ingestResult{}, err
	}
	defer obj.Close()

	var reader io.Reader = obj
	var zstdReader *zstd.Decoder
	if strings.HasSuffix(key, ".zstd") {
		zstdReader, err = zstd.NewReader(obj)
		if err != nil {
			return ingestResult{}, err
		}
		defer zstdReader.Close()
		reader = zstdReader
	}

	appID := appIDFromKey(key)
	result := ingestResult{}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		result.LineCount++
		if strings.TrimSpace(line) == "" {
			continue
		}
		var event map[string]any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&event); err != nil {
			result.SkippedLines++
			continue
		}
		if id := stringAt(event, "App ID"); id != "" {
			appID = id
		}
		eventType := stringAt(event, "Event")
		uid := eventUID(bucket, key, result.LineCount, line)
		result.RawRows = append(result.RawRows, rawRow{EventUID: uid, AppID: appID, EventType: eventType, EventTimeMS: eventTimeMillis(eventType, event), Bucket: bucket, ObjectKey: key, LineNo: result.LineCount, Raw: line})
		switch {
		case strings.HasSuffix(eventType, "SparkListenerSQLExecutionStart"):
			result.SQLStarts = append(result.SQLStarts, sqlStartRow{AppID: appID, ExecutionID: uintAt(event, "executionId"), Description: stringAt(event, "description"), Details: stringAt(event, "details"), PhysicalPlan: stringAt(event, "physicalPlanDescription"), StartTimeMS: uintAt(event, "time"), EventUID: uid})
		case strings.HasSuffix(eventType, "SparkListenerSQLExecutionEnd"):
			result.SQLEnds = append(result.SQLEnds, sqlEndRow{AppID: appID, ExecutionID: uintAt(event, "executionId"), EndTimeMS: uintAt(event, "time"), ErrorMessage: valueString(event["errorMessage"]), EventUID: uid})
		case eventType == "SparkListenerStageCompleted":
			if row, ok := stageFromEvent(appID, uid, event); ok {
				result.Stages = append(result.Stages, row)
			}
		case eventType == "SparkListenerTaskEnd":
			if row, ok := taskFromEvent(appID, uid, event); ok {
				result.Tasks = append(result.Tasks, row)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return ingestResult{}, err
	}
	return result, nil
}

func insertResult(ctx context.Context, conn driver.Conn, result ingestResult) error {
	if len(result.RawRows) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_raw_events (event_uid, app_id, event_type, event_time_ms, bucket, object_key, line_no, raw)")
		if err != nil {
			return err
		}
		for _, row := range result.RawRows {
			if err := batch.Append(row.EventUID, row.AppID, row.EventType, row.EventTimeMS, row.Bucket, row.ObjectKey, row.LineNo, row.Raw); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if len(result.SQLStarts) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_sql_executions (app_id, execution_id, description, details, physical_plan, start_time_ms, event_uid)")
		if err != nil {
			return err
		}
		for _, row := range result.SQLStarts {
			if err := batch.Append(row.AppID, row.ExecutionID, row.Description, row.Details, row.PhysicalPlan, row.StartTimeMS, row.EventUID); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if len(result.SQLEnds) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_sql_execution_ends (app_id, execution_id, end_time_ms, error_message, event_uid)")
		if err != nil {
			return err
		}
		for _, row := range result.SQLEnds {
			if err := batch.Append(row.AppID, row.ExecutionID, row.EndTimeMS, row.ErrorMessage, row.EventUID); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if len(result.Stages) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_stages (app_id, stage_id, stage_attempt_id, stage_name, num_tasks, submission_time_ms, completion_time_ms, event_uid)")
		if err != nil {
			return err
		}
		for _, row := range result.Stages {
			if err := batch.Append(row.AppID, row.StageID, row.StageAttemptID, row.StageName, row.NumTasks, row.SubmissionTimeMS, row.CompletionTimeMS, row.EventUID); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if len(result.Tasks) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_tasks (app_id, stage_id, stage_attempt_id, task_id, task_index, task_attempt, executor_id, host, launch_time_ms, finish_time_ms, duration_ms, task_type, successful, reason, executor_run_time_ms, executor_cpu_time_ns, peak_execution_memory, input_bytes, input_records, output_bytes, output_records, shuffle_read_bytes, shuffle_write_bytes, event_uid)")
		if err != nil {
			return err
		}
		for _, row := range result.Tasks {
			if err := batch.Append(row.AppID, row.StageID, row.StageAttemptID, row.TaskID, row.TaskIndex, row.TaskAttempt, row.ExecutorID, row.Host, row.LaunchTimeMS, row.FinishTimeMS, row.DurationMS, row.TaskType, row.Successful, row.Reason, row.ExecutorRunTimeMS, row.ExecutorCPUTimeNS, row.PeakExecutionMemory, row.InputBytes, row.InputRecords, row.OutputBytes, row.OutputRecords, row.ShuffleReadBytes, row.ShuffleWriteBytes, row.EventUID); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	return nil
}

func markFile(ctx context.Context, conn driver.Conn, bucket string, obj minio.ObjectInfo, lineCount uint64) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO spark_eventlog_files (bucket, object_key, etag, size, last_modified, line_count)")
	if err != nil {
		return err
	}
	if err := batch.Append(bucket, obj.Key, obj.ETag, uint64(maxInt64(obj.Size, 0)), obj.LastModified.UTC(), lineCount); err != nil {
		return err
	}
	return batch.Send()
}

func stageFromEvent(appID, uid string, event map[string]any) (stageRow, bool) {
	info := mapAt(event, "Stage Info")
	if info == nil {
		return stageRow{}, false
	}
	return stageRow{AppID: appID, StageID: uintAt(info, "Stage ID"), StageAttemptID: uintAt(info, "Stage Attempt ID"), StageName: stringAt(info, "Stage Name"), NumTasks: uintAt(info, "Number of Tasks"), SubmissionTimeMS: uintAt(info, "Submission Time"), CompletionTimeMS: uintAt(info, "Completion Time"), EventUID: uid}, true
}

func taskFromEvent(appID, uid string, event map[string]any) (taskRow, bool) {
	info := mapAt(event, "Task Info")
	metrics := mapAt(event, "Task Metrics")
	if info == nil {
		return taskRow{}, false
	}
	launch := uintAt(info, "Launch Time")
	finish := uintAt(info, "Finish Time")
	input := mapAt(metrics, "Input Metrics")
	output := mapAt(metrics, "Output Metrics")
	shuffleRead := mapAt(metrics, "Shuffle Read Metrics")
	shuffleWrite := mapAt(metrics, "Shuffle Write Metrics")
	successful := uint8(0)
	if boolAt(info, "Successful") || strings.Contains(valueString(event["Task End Reason"]), "Success") {
		successful = 1
	}
	return taskRow{
		AppID:               appID,
		StageID:             uintAt(event, "Stage ID"),
		StageAttemptID:      uintAt(event, "Stage Attempt ID"),
		TaskID:              uintAt(info, "Task ID"),
		TaskIndex:           uintAt(info, "Index"),
		TaskAttempt:         uintAt(info, "Attempt"),
		ExecutorID:          stringAt(info, "Executor ID"),
		Host:                stringAt(info, "Host"),
		LaunchTimeMS:        launch,
		FinishTimeMS:        finish,
		DurationMS:          durationMillis(launch, finish),
		TaskType:            stringAt(event, "Task Type"),
		Successful:          successful,
		Reason:              valueString(event["Task End Reason"]),
		ExecutorRunTimeMS:   uintAt(metrics, "Executor Run Time"),
		ExecutorCPUTimeNS:   uintAt(metrics, "Executor CPU Time"),
		PeakExecutionMemory: uintAt(metrics, "Peak Execution Memory"),
		InputBytes:          uintAt(input, "Bytes Read"),
		InputRecords:        uintAt(input, "Records Read"),
		OutputBytes:         uintAt(output, "Bytes Written"),
		OutputRecords:       uintAt(output, "Records Written"),
		ShuffleReadBytes:    uintAt(shuffleRead, "Local Bytes Read") + uintAt(shuffleRead, "Remote Bytes Read") + uintAt(shuffleRead, "Remote Bytes Read To Disk"),
		ShuffleWriteBytes:   uintAt(shuffleWrite, "Shuffle Bytes Written"),
		EventUID:            uid,
	}, true
}

func eventTimeMillis(eventType string, event map[string]any) uint64 {
	switch {
	case strings.HasSuffix(eventType, "SparkListenerSQLExecutionStart") || strings.HasSuffix(eventType, "SparkListenerSQLExecutionEnd"):
		return uintAt(event, "time")
	case eventType == "SparkListenerApplicationStart" || eventType == "SparkListenerApplicationEnd":
		return uintAt(event, "Timestamp")
	case eventType == "SparkListenerStageCompleted":
		info := mapAt(event, "Stage Info")
		if t := uintAt(info, "Completion Time"); t > 0 {
			return t
		}
		return uintAt(info, "Submission Time")
	case eventType == "SparkListenerTaskEnd":
		return uintAt(mapAt(event, "Task Info"), "Finish Time")
	default:
		return 0
	}
}

func eventUID(bucket, key string, lineNo uint64, raw string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n%d\n%s", bucket, key, lineNo, raw)))
	return hex.EncodeToString(sum[:])
}

func appIDFromKey(key string) string {
	return appIDRE.FindString(key)
}

func mapAt(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	if child, ok := m[key].(map[string]any); ok {
		return child
	}
	return nil
}

func stringAt(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	return valueString(m[key])
}

func valueString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		bytes, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprintf("%v", x)
		}
		return string(bytes)
	}
}

func uintAt(m map[string]any, key string) uint64 {
	if m == nil {
		return 0
	}
	return toUint(m[key])
}

func toUint(v any) uint64 {
	switch x := v.(type) {
	case nil:
		return 0
	case json.Number:
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return u
		}
		if f, err := strconv.ParseFloat(x.String(), 64); err == nil && f > 0 {
			return uint64(f)
		}
	case float64:
		if x > 0 {
			return uint64(x)
		}
	case int:
		if x > 0 {
			return uint64(x)
		}
	case int64:
		if x > 0 {
			return uint64(x)
		}
	case uint64:
		return x
	}
	return 0
}

func boolAt(m map[string]any, key string) bool {
	if m == nil {
		return false
	}
	b, _ := m[key].(bool)
	return b
}

func durationMillis(start, end uint64) uint64 {
	if end > start {
		return end - start
	}
	return 0
}

func maxInt64(a int64, min int64) int64 {
	if a < min {
		return min
	}
	return a
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
