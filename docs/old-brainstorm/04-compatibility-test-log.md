# Compatibility Test Log

Data: 2026-05-23

## Escopo

Validacao simples do caminho classico:

- base image oficial `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu`;
- imagem local `spark-plat-v0-compat:4.1.2-java17` construida via Compose;
- Spark standalone master + 1 worker + History Server;
- event logs em `./logs/spark-events`;
- jobs submetidos de dentro do container master com `spark-submit`;
- History Server expondo SQL executions, plan descriptions, stages e task metrics.

## Resultado

| Item | Versao/config | Resultado |
| --- | --- | --- |
| Spark base image | `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` | OK |
| Spark runtime | `Spark 4.1.2`, Java `17.0.19` | OK |
| Compose build | `spark-plat-v0-compat:4.1.2-java17` | OK |
| History Server | `file:/opt/spark/logs/spark-events`, porta host `28080` | OK |
| SQL smoke | `apps/smoke_sql.py` | OK |
| Delta smoke | `apps/smoke_delta.py` + `io.delta:delta-spark_4.1_2.13:4.2.0` | OK |

## Evidencias

SQL smoke:

- app id: `app-20260523151959-0000`;
- app name: `compat-smoke-sql`;
- resultado: 10 buckets com 1000 linhas cada;
- History API `/api/v1/applications/app-20260523151959-0000/sql` retornou `planDescription`, `nodes`, `edges`, metricas por operador e `successJobIds`;
- History API `/stages` e `/stages/0/0/taskList` retornou stage/task metrics, incluindo `executorRunTime`, `inputMetrics`, `shuffleReadMetrics` e `shuffleWriteMetrics`.

Delta smoke:

- app id: `app-20260523152526-0001`;
- app name: `compat-smoke-delta`;
- artifact usado: `io.delta:delta-spark_4.1_2.13:4.2.0`;
- criou tabela local em `file:/opt/spark/data/delta_smoke`;
- resultado: 10 buckets com 10 linhas cada;
- History API `/api/v1/applications/app-20260523152526-0001/sql` retornou execucoes para `SaveIntoDataSourceCommand`, processamento de log Delta e SQL final sobre `PreparedDeltaFileIndex`;
- plano final do SQL Delta mostrou `Scan parquet`, `ColumnarToRow`, `HashAggregate`, `Exchange`, `AQEShuffleRead` e `Sort`.

## Comandos usados

```bash
docker compose build spark-master
docker compose up -d spark-master spark-worker spark-history
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/apps/smoke_sql.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_4.1_2.13:4.2.0 \
  /opt/spark/apps/smoke_delta.py
```

## Decisoes

- A base mais segura para a v0 e a imagem oficial upstream `apache/spark`, nao Bitnami, porque o requisito atual e usar imagens oficiais sem risco operacional extra.
- Delta 4.2.0 deve usar o artifact com sufixo de Spark: `delta-spark_4.1_2.13`.
- Para o compose final, preferir bakear os jars Delta na imagem em vez de depender de `--packages` em cada submit. O teste com `--packages` foi adequado apenas para validar compatibilidade rapidamente.
- O History Server atende o requisito de plano SQL, stage e task no primeiro momento. Listener customizado e carga no ClickHouse ficam para a etapa posterior.

## Pendencias para a proxima fase

- Transformar o compat-lab em estrutura principal do projeto.
- Criar `.env` para quantidade de workers, cores, memoria e paths locais.
- Adicionar script `bin/submit` para encapsular `docker compose exec ... spark-submit`.
- Bakear dependencies Delta na imagem.
- Definir se os event logs ficam em filesystem local no v0 ou se ja entram no MinIO na fase seguinte.
