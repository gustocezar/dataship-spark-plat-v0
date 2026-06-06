#!/usr/bin/env bash
# Vertical slice do Apex: contrato -> codigo + log fiel -> deteccao -> acceptance.
# Roda local (laptop) ou em CI. Sem Spark: a geracao e a deteccao sao deterministicas.
set -e
S="${1:-scenarios/skew_on_join_30x.yaml}"
echo "▶ 1/3 gerar codigo (com guard de linha)";   python3 generators/code_generator.py "$S" job_demo.py
echo "▶ 2/3 gerar log sintetico fiel ao Spark";    python3 generators/plan_generator.py "$S" synthetic_log.ndjson
echo "▶ 3/3 Watcher detecta skew + valida acceptance"; python3 watchers/skew_watcher.py "$S" synthetic_log.ndjson
echo "🟢 SLICE COMPLETO"
