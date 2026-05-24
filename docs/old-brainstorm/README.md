# Spark Platform v0 Brainstorm

Data: 2026-05-23

Esta pasta registra a validacao conceitual feita sobre os repos de referencia em
`forge/dataship/master-spark-plumbers` e `forge/dataship/spark-frm-mec`.

O objetivo desta fase nao e copiar a implementacao antiga. O que vale reaproveitar
e o desenho operacional:

- subir um cluster Spark standalone com Docker Compose;
- submeter jobs para o master;
- persistir event logs em volume local configuravel;
- expor Spark History Server para UI e API REST;
- inspecionar SQL executions, jobs, stages e tasks;
- manter exemplos que gerem cenarios comparaveis de shuffle, broadcast, spill,
  particionamento e Delta.

Documentos:

- `00-repo-validation.md`: o que foi encontrado nos dois repos antigos.
- `01-target-versions.md`: versoes atuais e compatibilidade de Spark/Delta.
- `02-compose-submit-connect.md`: desenho operacional para Compose, submit e Spark Connect.
- `03-observability-model.md`: como capturar plano SQL, stage/task metrics e futuro ClickHouse.
- `04-compatibility-test-log.md`: resultados dos smoke tests com imagem oficial Apache Spark.
- `05-architecture-risks.md`: riscos e decisoes para MinIO, S3A, Delta, History e ClickHouse.
