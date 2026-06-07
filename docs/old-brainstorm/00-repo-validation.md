# Validacao Dos Repos De Referencia

## Repos analisados

- `forge/dataship/master-spark-plumbers`
- `forge/dataship/spark-frm-mec`

Ambos usam o mesmo conceito central: Spark standalone em Docker Compose, workers
montando os mesmos volumes do master, jobs enviados via `spark-submit` dentro do
container `spark-master`, e Spark History Server lendo os event logs do mesmo
diretorio montado.

## `master-spark-plumbers`

O repo antigo e o mais rico para observabilidade de execucao Spark.

Pontos encontrados:

- `docker-compose.yml` define `spark-master`, 3 workers e `spark-history-server`.
- O master e os workers montam `APP_SRC_PATH`, `APP_STORAGE_PATH`, `APP_LOG_PATH`
  e `APP_METRICS_PATH`.
- `build/config/spark/spark-defaults.conf` habilita:
  - `spark.eventLog.enabled true`
  - `spark.eventLog.dir file:/opt/bitnami/spark/logs/events`
  - `spark.history.fs.logDirectory file:/opt/bitnami/spark/logs/events`
  - `spark.history.provider org.apache.spark.deploy.history.FsHistoryProvider`
- O History Server usa `SPARK_HISTORY_FS_LOG_DIRECTORY` apontando para o mesmo
  volume de event logs.
- Os scripts de exemplo documentam URLs do History Server como
  `/history/<app-id>/SQL/execution/?id=<execution-id>`.
- Muitos jobs usam `sparkmeasure.StageMetrics`; alguns importam `TaskMetrics`.
- Os event logs em `logs/` contem eventos reais:
  - `SparkListenerSQLExecutionStart`
  - `physicalPlanDescription`
  - `sparkPlanInfo`
  - `SparkListenerStageCompleted`
  - `SparkListenerTaskEnd`
  - metricas de shuffle, spill, input/output e executor runtime.

Validacao importante: o nivel "SQL task" nao vem de um unico objeto pronto. Ele
e reconstruido pela UI/API do Spark combinando SQL execution, jobs, stages e task
metrics no event log. Para uma plataforma propria, precisamos persistir o event
log bruto e/ou extrair essas relacoes depois.

## `spark-frm-mec`

O segundo repo preserva a estrutura de build/Compose de forma mais organizada,
mas tem menos material de diagnostico avancado.

Pontos encontrados:

- `build/docker-compose.yml` e equivalente ao primeiro, com master, 3 workers e
  History Server.
- Tem `.env` local com os quatro caminhos principais de montagem.
- `build/readme.md` documenta o fluxo:
  - criar `.env`;
  - criar diretorios `src`, `storage`, `logs`, `metrics`;
  - buildar imagens Spark e History;
  - subir o cluster;
  - verificar containers;
  - acessar Master UI e History Server.
- Usa os mesmos conceitos de event log e History Server.
- O conteudo de aulas e scripts cobre `spark-submit`, `explain()` e Delta, mas a
  parte de observabilidade por event log e menos explorada que no repo antigo.

## Conclusao

O reaproveitamento e conceitual:

- manter um Compose com master, workers e History Server;
- manter `.env` para caminhos e recursos;
- manter um comando padrao de `spark-submit`;
- montar o diretorio de event logs em todos os containers Spark;
- usar History Server e REST API como fonte de verdade inicial;
- adicionar, nesta versao nova, Spark Connect como interface para agentes e
  exploracao interativa.

Nao devemos copiar:

- imagens antigas baseadas em Spark 3.5;
- jars Scala 2.12;
- `openjdk:11`;
- credenciais ou endpoints hardcoded dos scripts antigos;
- caminhos absolutos locais do `.env` antigo.

