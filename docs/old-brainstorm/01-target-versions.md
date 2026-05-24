# Versoes Alvo E Compatibilidade

## Estado em 2026-05-23

Spark mais novo estavel:

- Apache Spark 4.1.2, lancado em 2026-05-21 segundo a pagina oficial de
  downloads do Spark.
- A pagina de documentacao atual tambem identifica a documentacao como Spark
  4.1.2.
- Spark 4 usa Scala 2.13; suporte a Scala 2.12 foi removido no Spark 4.0.0.
- Spark 4.1.2 roda em Java 17/21, Scala 2.13 e Python 3.10+.

Delta Lake:

- Delta Lake 4.2.0 foi publicado em 2026-04-16/17.
- O release note do Delta informa que Delta Spark 4.2.0 e construido para Spark
  4.1.0 e Spark 4.0.1.
- Para Spark 4.1, o artefato recomendado e `io.delta:delta-spark_4.1_2.13:4.2.0`.
- Tambem existe `delta-spark==4.2.0` para Python.

## Decisao inicial recomendada

Usar:

- imagem base: `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` ou tag curta
  equivalente `apache/spark:4.1.2`;
- Spark: `4.1.2`;
- Java: dentro do container, Java 17 ou 21;
- Python: dentro do container, Python 3.10+;
- Scala ABI: `2.13`;
- Delta: `4.2.0`;
- pacote Maven principal: `io.delta:delta-spark_4.1_2.13:4.2.0`.

Java local nao e requisito para o usuario. O host precisa de Docker/Compose. Java
so precisa existir dentro da imagem Spark, e por isso a imagem escolhida deve ser
compativel com Spark 4.

## Risco Delta 4.2.0 com Spark 4.1.2

Delta 4.2.0 declara build contra Spark 4.1.0. Como Spark 4.1.2 e patch release
da mesma linha 4.1, a combinacao e a melhor aposta atual para "mais novo
possivel", mas deve ser validada com smoke tests:

- criar tabela Delta local;
- append;
- SQL read;
- update/delete/merge se quisermos exercitar Delta completo;
- event log gerado e visivel no History Server;
- endpoint `/api/v1/applications/<app-id>/sql` retornando plano e metricas.

## Fontes oficiais consultadas

- Apache Spark downloads: https://spark.apache.org/downloads.html
- Apache Spark 4.1.2 docs: https://spark.apache.org/docs/latest/
- Spark monitoring/history server: https://spark.apache.org/docs/latest/monitoring.html
- Spark Connect overview: https://spark.apache.org/docs/latest/spark-connect-overview.html
- Delta Lake 4.2 release: https://delta.io/blog/2026-04-17-delta-4-2-released/
- Delta GitHub releases: https://github.com/delta-io/delta/releases

