# Modelo De Observabilidade

## O que queremos enxergar

Nivel minimo:

- aplicacao Spark;
- SQL execution;
- plano fisico e plano adaptativo quando existir;
- jobs gerados por cada SQL execution;
- stages;
- tasks;
- metricas por operador SQL quando disponiveis;
- metricas de task: runtime, CPU, input, output, shuffle, spill, GC;
- configuracoes relevantes da execucao.

## Fontes de dados

1. Spark History Server UI
   - Primeiro destino humano.
   - URLs como `/history/<app-id>/SQL/execution/?id=<execution-id>`.

2. Spark History Server REST API
   - Fonte inicial para automacao.
   - Endpoints importantes:
     - `/api/v1/applications`
     - `/api/v1/applications/<app-id>/sql`
     - `/api/v1/applications/<app-id>/sql/<execution-id>`
     - `/api/v1/applications/<app-id>/jobs`
     - `/api/v1/applications/<app-id>/stages?details=true`
     - `/api/v1/applications/<app-id>/stages/<stage-id>/<attempt-id>/taskList`

3. Event log bruto
   - Fonte de verdade persistida.
   - Contem eventos JSON como:
     - `SparkListenerSQLExecutionStart`
     - `SparkListenerSQLAdaptiveExecutionUpdate`
     - `SparkListenerStageCompleted`
     - `SparkListenerTaskEnd`
   - Deve ser mantido em volume local e depois, se necessario, copiado para MinIO.

4. `sparkmeasure`
   - Util para relatorios dentro dos jobs de demonstracao.
   - Bom para validar rapidamente stage/task aggregates.
   - Nao deve ser a unica fonte da plataforma, porque exige instrumentar o codigo
     da aplicacao.

## Estrategia v0

Primeiro, nao criar listener customizado.

Motivos:

- Spark ja emite event log suficiente para SQL, stage e task diagnostics.
- History Server e REST API ja conseguem reconstruir boa parte da UI.
- Listener customizado aumenta acoplamento com APIs internas do Spark.
- A prioridade e validar Spark 4.1.2 + Delta 4.2.0 + Compose + History + Connect.

Depois, se precisarmos escrever direto no ClickHouse:

- criar um parser de event logs;
- normalizar os eventos em tabelas;
- carregar ClickHouse em batch apos cada app terminar;
- avaliar listener customizado apenas se a latencia em tempo real for requisito.

## Futuro ClickHouse

Tabelas candidatas:

- `spark_applications`
- `spark_sql_executions`
- `spark_sql_plan_nodes`
- `spark_jobs`
- `spark_stages`
- `spark_tasks`
- `spark_task_metrics`
- `spark_executor_metrics`
- `spark_environment`

Chaves importantes:

- `app_id`
- `attempt_id`
- `execution_id`
- `job_id`
- `stage_id`
- `stage_attempt_id`
- `task_id`
- `accumulator_id`

## Demos necessarias

Para provar o comportamento:

- `demo_sql_observability.py`
  - gera dados locais;
  - executa join sort-merge;
  - executa broadcast join;
  - chama `explain("formatted")`;
  - produz event log com SQL execution.

- `demo_spill_shuffle.py`
  - usa poucos workers/memoria;
  - forca shuffle;
  - demonstra spill se possivel.

- `demo_delta_observability.py`
  - cria tabela Delta;
  - append;
  - SQL read;
  - `DESCRIBE HISTORY`;
  - valida event log + Delta.

## Validacao esperada

Depois de `spark-submit` ou execucao via Connect:

```bash
curl http://localhost:18080/api/v1/applications
curl http://localhost:18080/api/v1/applications/<app-id>/sql
curl "http://localhost:18080/api/v1/applications/<app-id>/stages?details=true"
```

O criterio de sucesso da v0 e:

- app aparece no History Server;
- endpoint `/sql` lista execucoes;
- cada execution tem `planDescription`/plan nodes;
- stages e tasks aparecem com metricas;
- Delta roda com Spark 4.1.x sem conflito de jars.

