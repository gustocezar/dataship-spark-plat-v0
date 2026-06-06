#!/usr/bin/env python3
"""
Skew Watcher (Pod A1) -- a peca que FECHA o loop e que faltava no pacote.

Le um event log do Spark (real OU sintetico -- mesmo schema, esse e o ponto), reconstroi a
distribuicao de 'Total Records Read' por task no stage quente, e detecta skew olhando a CAUDA
(task mais pesada / mediana), nao a media. Emite um Finding no formato do watcher.contract e
valida contra o bloco 'acceptance' do scenario.

Saida: imprime o Finding e sai com codigo 0 se o anti-pattern declarado foi detectado e o
Finding satisfaz o acceptance; senao sai != 0 (vira gate de CI).
"""
import json
import sys
import yaml
import statistics


def read_events(log_path):
    with open(log_path) as f:
        return [json.loads(line) for line in f if line.strip()]


def analyze(events):
    # Operador de join: do plano fisico textual do evento SQL.
    join_op = None
    for e in events:
        if e.get("Event", "").endswith("SparkListenerSQLExecutionStart"):
            plan = e.get("physicalPlanDescription", "")
            for op in ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin"):
                if op in plan:
                    join_op = op
                    break
    # Distribuicao de registros lidos por task (chaves REAIS do Spark).
    per_task = []
    for e in events:
        if e.get("Event") == "SparkListenerTaskEnd":
            recs = (e.get("Task Metrics", {})
                     .get("Shuffle Read Metrics", {})
                     .get("Total Records Read"))
            if recs is not None:
                per_task.append((e["Task Info"]["Task ID"], recs))
    return join_op, per_task


def build_finding(scenario, join_op, per_task):
    sig = scenario["plan_generator"]["expected_signals"]
    counts = sorted((r for _, r in per_task), reverse=True)
    hottest = max(per_task, key=lambda x: x[1])
    cold = [r for r in counts if r != hottest[1] and r > 0] or [1]
    median_cold = statistics.median(cold)
    skew_ratio = round(hottest[1] / median_cold, 1) if median_cold else 0

    is_skew = (join_op == sig["join_operator"]) and (skew_ratio >= sig["skew_ratio_min"])
    finding = {
        "watcher": "shuffle_skew",
        "severity": "high" if is_skew else "low",
        "confidence": round(min(0.99, skew_ratio / (skew_ratio + 3)), 2),  # da evidencia, nao auto-avaliado
        "evidence": [
            f"join operator: {join_op}",
            f"task mais pesada (id {hottest[0]}) leu {hottest[1]} registros",
            f"mediana das demais: {int(median_cold)} -> skew ratio {skew_ratio}x",
        ],
        "root_cause": f"data skew na chave de join customer_id ({join_op}): "
                      f"1 particao concentra {skew_ratio}x a mediana",
        "recommendations": [
            "habilitar spark.sql.adaptive.skewJoin.enabled",
            "broadcast o lado customers (dimensao pequena)",
            "salgar a chave customer_id",
        ],
    }
    return finding, is_skew


def check_acceptance(finding, scenario):
    acc = scenario["acceptance"]
    blob = (finding["root_cause"] + " " + " ".join(finding["evidence"])).lower()
    missing = [t for t in acc["root_cause_includes"] if t.lower() not in blob]
    enough_recs = len(finding["recommendations"]) >= acc.get("min_recommendations", 1)
    return (not missing) and enough_recs, missing, enough_recs


def main(scenario_path, log_path):
    scenario = yaml.safe_load(open(scenario_path))
    join_op, per_task = analyze(read_events(log_path))
    finding, is_skew = build_finding(scenario, join_op, per_task)

    print(json.dumps(finding, indent=2, ensure_ascii=False))
    ok, missing, enough = check_acceptance(finding, scenario)

    print("\n--- ACCEPTANCE ---")
    if not is_skew:
        print("❌ Watcher NAO detectou o anti-pattern declarado."); sys.exit(1)
    if not ok:
        print(f"❌ Finding nao satisfaz acceptance. Faltou root_cause: {missing}; recs suficientes: {enough}")
        sys.exit(1)
    print("✅ Anti-pattern detectado E Finding satisfaz o acceptance do scenario. GATE VERDE.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: skew_watcher.py <scenario.yaml> <event-log.ndjson>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
