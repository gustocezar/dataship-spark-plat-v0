#!/usr/bin/env python3
"""
Gerador de plano (v4) — adiciona evento ApexSyntheticProvenance (scenario_hash) para cadeia de custodia.
"""
import sys
import json
import time
import hashlib
import yaml


def compute_scenario_hash(scenario_path: str) -> str:
    with open(scenario_path, "rb") as f:
        h = hashlib.sha256(f.read()).hexdigest()[:16]
    return f"sha256:{h}"


def synthesize_events(config, scenario_hash):
    sid = config["scenario_id"]
    code = config["code_generator"]
    sig = config["plan_generator"]["expected_signals"]
    line = code.get("anti_pattern_line", 0)
    rows = code["data"]["orders"]["rows"]
    hot_share = code["data"]["orders"]["hot_share"]
    parts = int(code["spark_config"]["spark.sql.shuffle.partitions"])
    stage = sig.get("hot_stage", 4)
    app_id = f"app-{int(time.time())}-0001"
    t0 = int(time.time() * 1000)

    declared_hot = sig.get("hot_partition", {}).get("single_task_shuffle_read_records")
    hot_records = int(declared_hot) if declared_hot else int(rows * hot_share)
    cold_total = max(rows - hot_records, parts - 1)
    cold_each = cold_total // (parts - 1) if (parts - 1) > 0 else 0

    events = [
        # Evento de proveniência (obrigatório como primeiro)
        {
            "Event": "ApexSyntheticProvenance",
            "scenario_id": sid,
            "scenario_hash": scenario_hash,
            "generator_version": "v4"
        },
        {
            "Event": "SparkListenerApplicationStart",
            "App Name": sid, "App ID": app_id, "Timestamp": t0, "User": "apex"
        },
        {
            "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 1, "description": f"save at job.py:{line}",
            "physicalPlanDescription":
                f"== Physical Plan ==\n*(5) {sig['join_operator']} [customer_id], [customer_id], Inner\n"
                f":- Exchange hashpartitioning(customer_id, {parts})\n"
                f"+- Exchange hashpartitioning(customer_id, {parts})",
            "time": t0
        },
        {
            "Event": "SparkListenerStageSubmitted",
            "Stage Info": {
                "Stage ID": stage,
                "Stage Name": f"{sig['join_operator']} at job.py:{line}",
                "Number of Tasks": parts
            }
        },
    ]
    for tid in range(parts):
        recs = hot_records if tid == 0 else cold_each + (tid * 3)
        events.append({
            "Event": "SparkListenerTaskEnd",
            "Stage ID": stage, "Stage Attempt ID": 0,
            "Task Type": "ShuffleMapTask", "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Task ID": tid, "Index": tid, "Attempt": 0,
                          "Launch Time": t0, "Finish Time": t0 + recs // 100, "Failed": False},
            "Task Metrics": {"Executor Run Time": recs // 100, "JVM GC Time": 0,
                             "Memory Bytes Spilled": 0,
                             "Shuffle Read Metrics": {"Total Records Read": recs,
                                                      "Remote Bytes Read": recs * 64,
                                                      "Fetch Wait Time": 0}},
        })
    events.append({
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {"Stage ID": stage, "Number of Tasks": parts, "Completion Time": t0 + 2000}
    })
    events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": t0 + 2500})
    return events


def generate_plan(scenario_path, output_path):
    config = yaml.safe_load(open(scenario_path))
    scenario_hash = compute_scenario_hash(scenario_path)
    events = synthesize_events(config, scenario_hash)
    with open(output_path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"✅ {output_path} gerado: {len(events)} eventos, scenario_hash {scenario_hash} embutido.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: plan_generator.py <scenario.yaml> <output.ndjson>")
        sys.exit(1)
    generate_plan(sys.argv[1], sys.argv[2])