#!/usr/bin/env python3
"""
Gerador de plano (v2). Le o scenario.yaml e emite um event log SINTETICO sem executar Spark.

Correcoes:
- Defeito #2: usa os nomes de campo REAIS do Spark (JsonProtocol). Eventos de scheduler usam
  chaves "Capitalizado Com Espaco" ("Event", "Stage ID", "Task Metrics" -> "Shuffle Read
  Metrics" -> "Total Records Read"); eventos de SQL usam camelCase (executionId,
  physicalPlanDescription). Um Watcher escrito contra ISTO tambem le log real.
- Defeito #3: a distribuicao de tasks e DERIVADA dos dados (rows, hot_share, particoes),
  entao existe uma cauda real -- da pra detectar skew. A task quente concentra hot_share das
  linhas; as demais dividem o resto. Isso faz o "30x" do nome do cenario virar sinal medivel.
- A callSite carrega a MESMA linha do contrato, entao log <-> codigo correspondem.
"""
import yaml
import sys
import json
import time


def synthesize_events(config):
    sid = config["scenario_id"]
    code = config["code_generator"]
    sig = config["plan_generator"]["expected_signals"]
    line = code["anti_pattern_line"]
    rows = code["data"]["orders"]["rows"]
    hot_share = code["data"]["orders"]["hot_share"]
    parts = int(code["spark_config"]["spark.sql.shuffle.partitions"])
    stage = sig["hot_stage"]
    app_id = f"app-{int(time.time())}-0001"
    t0 = int(time.time() * 1000)

    # Distribuicao derivada: 1 particao quente (a do hot_key) + (parts-1) frias.
    hot_records = int(sig.get('hot_partition',{}).get('single_task_shuffle_read_records', int(rows*hot_share)))
    cold_total = rows - hot_records                         # ~40000
    cold_each = cold_total // (parts - 1)                   # ~5714

    events = []
    events.append({"Event": "SparkListenerApplicationStart", "App Name": sid,
                   "App ID": app_id, "Timestamp": t0, "User": "apex"})

    # Evento SQL: camelCase, com o plano fisico textual contendo o operador de join.
    events.append({
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 1,
        "description": f"save at job.py:{line}",        # callSite -> mesma linha do contrato
        "physicalPlanDescription":
            f"== Physical Plan ==\n*(5) {sig['join_operator']} [customer_id], [customer_id], Inner\n"
            f":- *(2) Sort [customer_id ASC]\n:  +- Exchange hashpartitioning(customer_id, {parts})\n"
            f"+- *(4) Sort [customer_id ASC]\n   +- Exchange hashpartitioning(customer_id, {parts})",
        "time": t0,
    })
    events.append({"Event": "SparkListenerStageSubmitted",
                   "Stage Info": {"Stage ID": stage, "Stage Name": f"{sig['join_operator']} at job.py:{line}",
                                  "Number of Tasks": parts}})

    # Uma task por particao. taskId 0 = quente; resto = frias (com leve jitter deterministico).
    for tid in range(parts):
        recs = hot_records if tid == 0 else cold_each + (tid * 3)
        events.append({
            "Event": "SparkListenerTaskEnd",
            "Stage ID": stage,
            "Stage Attempt ID": 0,
            "Task Type": "ShuffleMapTask",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Task ID": tid, "Index": tid, "Attempt": 0,
                          "Launch Time": t0, "Finish Time": t0 + recs // 100, "Failed": False},
            "Task Metrics": {
                "Executor Run Time": recs // 100,
                "JVM GC Time": 0,
                "Memory Bytes Spilled": 0,
                "Shuffle Read Metrics": {
                    "Total Records Read": recs,
                    "Remote Bytes Read": recs * 64,
                    "Fetch Wait Time": 0,
                },
            },
        })

    events.append({"Event": "SparkListenerStageCompleted",
                   "Stage Info": {"Stage ID": stage, "Number of Tasks": parts,
                                  "Completion Time": t0 + 2000}})
    events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": t0 + 2500})
    return events


def generate_plan(scenario_path, output_path):
    with open(scenario_path) as f:
        config = yaml.safe_load(f)
    events = synthesize_events(config)
    with open(output_path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"✅ {output_path} gerado: {len(events)} eventos, schema fiel ao Spark, distribuicao derivada.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: plan_generator.py <scenario.yaml> <output.ndjson>")
        sys.exit(1)
    generate_plan(sys.argv[1], sys.argv[2])
