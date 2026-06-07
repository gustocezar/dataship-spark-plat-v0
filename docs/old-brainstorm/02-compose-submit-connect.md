# Compose, Submit E Spark Connect

## Modelo operacional desejado

A fase 1 deve ser classica e reprodutivel:

1. `docker compose build` para gerar a imagem local de runtime.
2. `docker compose up` para subir master, workers e History Server.
3. `spark-submit` dentro do container para evitar Java/Spark instalado no host.
4. Event logs persistidos em volume local configuravel e lidos pelo History Server.

Spark Connect fica como trilha futura para agentes, notebooks e exploracao SQL
remota. Ele nao deve bloquear a primeira entrega, porque o objetivo imediato e
reproduzir o fluxo dos repos antigos com versoes atuais e observabilidade no
History Server.

## Compose base

Servicos planejados para a primeira versao:

- `spark-master`
  - Spark standalone master.
  - Porta `8080` para Master UI.
  - Monta `./apps`, `./data`, `./logs/spark-events`, `./metrics`.

- `spark-worker`
  - Servico escalavel via Compose:
    - `docker compose up --scale spark-worker=3`
  - Usa `SPARK_WORKER_CORES` e `SPARK_WORKER_MEMORY` vindos do `.env`.
  - Monta os mesmos volumes do master.

- `spark-history`
  - Roda o History Server.
  - Porta `18080`.
  - Le `spark.history.fs.logDirectory` apontando para o mesmo volume de event logs.

Servicos futuros:

- `spark-connect`
  - Roda `start-connect-server.sh --master spark://spark-master:7077`.
  - Porta padrao Spark Connect: `15002`.
  - Usa os mesmos `spark-defaults.conf` e packages do runtime.
  - Exposto para clientes/agentes como `sc://localhost:15002`.
- `minio`
  - armazenamento S3 local para dados e tabelas Delta.
- `clickhouse`
  - destino analitico para event logs normalizados e metricas de execucao.
- `eventlog-loader`
  - job ou servico pequeno que le event logs e carrega ClickHouse.

## `.env` esperado

Variaveis iniciais:

```dotenv
SPARK_VERSION=4.1.2
SPARK_IMAGE=apache/spark:4.1.2-scala2.13-java17-python3-ubuntu
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_CONNECT_URL=sc://spark-connect:15002

SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=3g
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=2g

APP_SRC_PATH=./apps
APP_DATA_PATH=./data
APP_EVENTLOG_PATH=./logs/spark-events
APP_METRICS_PATH=./metrics

DELTA_PACKAGE=io.delta:delta-spark_4.1_2.13:4.2.0
```

## Submit classico e ainda necessario

O padrao de submit deve ficar em script, por exemplo `bin/submit`, para esconder
o comando longo:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name demo-sql-observability \
  --packages io.delta:delta-spark_4.1_2.13:4.2.0 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:/opt/spark/logs/spark-events \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/apps/demo_sql_observability.py
```

Notas:

- O `spark-submit` deve rodar dentro do container para evitar Java/Spark local.
- Em modo standalone local, `deploy-mode client` e suficiente e simples.
- O driver roda no container que executa o submit; por isso o submit a partir do
  master e o caminho mais previsivel.
- Todos os paths usados pelo job precisam existir dentro dos containers.

## Spark Connect como trilha futura

Spark Connect pode ser tratado depois como caminho moderno para workloads
SQL/DataFrame, mas com uma diferenca importante: ele nao envia um arquivo Python
para o cluster manager como `spark-submit`. Ele roda um cliente fora do driver,
abre uma sessao remota e envia planos logicos via gRPC para o Spark Connect
Server, que executa no cluster.

A unidade operacional muda:

- `spark-submit`: submete uma aplicacao Spark empacotada para o cluster manager.
- `connect-submit`: executa um cliente Python/Scala que se conecta ao servidor
  Spark Connect e dispara SQL/DataFrame actions.

Esse `connect-submit` pode ser nosso wrapper moderno para agentes em uma fase
posterior.

Uso esperado em Python:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .appName("agent-session")
    .getOrCreate()
)

spark.sql("SELECT 1 AS ok").show()
```

Wrapper conceitual:

```bash
docker compose run --rm spark-client python /workspace/apps/demo_connect_sql.py
```

Com ambiente:

```dotenv
SPARK_REMOTE=sc://spark-connect:15002
```

E no codigo:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo-connect-sql").getOrCreate()
spark.sql("SELECT 1 AS ok").show()
```

Se `SPARK_REMOTE` estiver definido no cliente, `getOrCreate()` cria uma sessao
Spark Connect. Alternativamente, usamos explicitamente
`SparkSession.builder.remote("sc://spark-connect:15002")`.

Pontos fortes:

- bom para agentes, notebooks e exploracao;
- nao exige Java local se o cliente roda em container;
- cliente pode ter dependencias proprias sem contaminar o driver;
- facilita APIs de plataforma: um backend pode abrir sessoes e executar SQL;
- evita `docker compose exec spark-master spark-submit ...` para interacoes
  pequenas ou agenticas.

Limites:

- nao substitui totalmente `spark-submit` para jobs batch versionados;
- APIs baseadas em `SparkContext`, RDD e acesso direto a `_jvm`/`_jdf` nao sao o
  alvo do Spark Connect;
- bibliotecas Python executadas como UDF ainda precisam de cuidado de empacotamento;
- autenticacao nao vem pronta no Spark Connect OSS; para expor fora do localhost,
  precisa proxy/autenticacao na frente;
- a fronteira de observabilidade pode mudar: em vez de um app id novo por arquivo
  submetido, podemos ver sessoes/execucoes dentro do app do Spark Connect Server.
  Isso precisa ser validado nos smoke tests.

## Recomendacao

Construir a v0 em duas ondas:

1. Primeiro: `spark-submit` para demos reprodutiveis, jobs de carga, jobs longos
   e testes de regressao.
2. Depois: `connect-submit` para interface de agentes, SQL/DataFrame remoto e
   execucao interativa.

Na pratica, a primeira entrega deve ter:

- `docker-compose.yml`;
- `conf/spark-defaults.conf`;
- `bin/submit`;
- `apps/demo_sql_observability.py`;
- `apps/demo_delta_observability.py`.
