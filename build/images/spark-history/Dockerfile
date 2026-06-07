ARG SPARK_RUNTIME_IMAGE=spark-plat-v0-spark:4.1.2
FROM ${SPARK_RUNTIME_IMAGE}

USER root
COPY entrypoint.sh /opt/spark/bin/spv0-history-entrypoint.sh
RUN chmod 755 /opt/spark/bin/spv0-history-entrypoint.sh

USER spark

ENTRYPOINT ["/opt/spark/bin/spv0-history-entrypoint.sh"]
