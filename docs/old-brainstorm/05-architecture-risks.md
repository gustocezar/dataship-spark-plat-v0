# Architecture Risks

Data: 2026-05-23

## MinIO e imagens oficiais

- Para desenvolvimento local, MinIO em Compose e aceitavel e simplifica validar S3A sem depender de cloud.
- Usar imagens oficiais e pinadas. No compat-lab a escolha inicial e `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1` e `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z-cpuv1`.
- Risco: o ecossistema MinIO mudou distribuicao/comercializacao em 2025/2026. Antes de promover para base duradoura, revisar status das imagens oficiais, CVEs e alternativa S3-compatible se necessario.
- Credenciais do compat-lab sao somente defaults locais. Nao reutilizar em ambientes compartilhados.

## Delta em S3/MinIO

- Delta em `s3a://` e o caminho certo para simular um data lake object-store.
- Para o nosso fluxo single-cluster/single-driver, o modo S3 default do Delta e suficiente para smoke tests e desenvolvimento.
- Risco importante: writes concorrentes no mesmo Delta table a partir de multiplos drivers/clusters exigem coordenacao extra de LogStore. Sem isso, pode haver conflito ou perda de consistencia.
- Decisao para v0: permitir um writer por tabela nos demos e documentar isso explicitamente.

## Spark S3A

- Spark precisa do Hadoop AWS connector no classpath: `org.apache.hadoop:hadoop-aws` na mesma familia da versao Hadoop usada pelo Spark.
- Para Spark 4.1.2 no teste, a versao candidata e Hadoop AWS `3.4.2`.
- Risco: mismatch entre `hadoop-aws`, Hadoop client runtime e AWS SDK gera erro em runtime, nao em build.
- Decisao para v0: primeiro validar com `--packages`; depois bakear os jars na imagem Spark para submits reprodutiveis.

## Event logs em S3

- Etapa inicial deve manter event logs em filesystem local para isolar a validacao Delta/MinIO.
- Depois, validar `spark.eventLog.dir=s3a://spark-logs/events` e `spark.history.fs.logDirectory=s3a://spark-logs/events` como teste separado.
- Risco: History Server tambem precisa dos jars S3A e credenciais. Se os jars so forem passados no `spark-submit`, o History Server nao consegue ler logs em S3.

## ClickHouse futuro

- ClickHouse deve receber dados normalizados dos event logs, nao os arquivos brutos como fonte primaria de consulta humana.
- Risco: o schema de eventos Spark pode mudar entre versoes. O loader precisa versionar parsing por `SparkListener` event type e guardar payload bruto para auditoria.
