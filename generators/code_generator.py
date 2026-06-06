#!/usr/bin/env python3
"""
Gerador de codigo (v2). Le o scenario.yaml e emite um job PySpark com o bug.

Correcao do defeito #1: a linha do anti-pattern nao e mais uma ficcao no YAML.
O gerador CONSTROI o job de forma deterministica para que o join caia exatamente
na linha declarada no contrato, e ASSERTA isso antes de gravar. Se alguem mexer no
template e a linha mudar, o gerador FALHA -- contrato e codigo nunca divergem em
silencio. E o principio "eval como teste" aplicado ao proprio gerador.
"""
import yaml
import sys


def build_job_source(config):
    sid = config["scenario_id"]
    cfg = config["code_generator"]
    data = cfg["data"]
    conf = cfg.get("spark_config", {})
    conf_lines = "".join(f'    .config("{k}", "{v}")\n' for k, v in conf.items())

    # O cabecalho e construido para que a linha do JOIN seja deterministica.
    header = f'''# Auto-gerado por code_generator v2 -- scenario: {sid}
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when

spark = (SparkSession.builder.appName("{sid}")
{conf_lines}    .getOrCreate())

orders = spark.range({data['orders']['rows']}).select(
    (rand(42) * {data['orders']['distinct_keys']}).cast('int').alias('customer_id'),
    col('id').alias('order_id'))
orders = orders.withColumn('customer_id',
    when(rand(13) < {data['orders']['hot_share']}, {data['orders']['hot_key']}).otherwise(col('customer_id')))
customers = spark.range({data['customers']['rows']}).select(
    col('id').alias('customer_id'), col('id').alias('customer_name'))
'''
    # A proxima linha apos o header e o join. Calculamos seu numero.
    join_line_no = len(header.splitlines()) + 1
    body = '''result = orders.join(customers.hint("shuffle_merge"), "customer_id", "inner")   # <<< ANTI-PATTERN: skew join
result.write.mode("overwrite").parquet("/tmp/apex_output")
spark.stop()
'''
    return header + body, join_line_no


def generate_job(scenario_path, output_path):
    with open(scenario_path) as f:
        config = yaml.safe_load(f)

    declared = config["code_generator"]["anti_pattern_line"]
    source, actual = build_job_source(config)

    # GUARD: o contrato e o codigo TEM que concordar sobre a linha.
    if actual != declared:
        raise SystemExit(
            f"❌ DIVERGENCIA DE CONTRATO: o anti-pattern caiu na linha {actual}, "
            f"mas o scenario.yaml declara {declared}. Ajuste anti_pattern_line para {actual} "
            f"(ou corrija o template). Contrato e codigo nao podem divergir."
        )

    with open(output_path, "w") as f:
        f.write(source)
    print(f"✅ {output_path} gerado. Anti-pattern na linha {actual} (== contrato). Guard OK.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: code_generator.py <scenario.yaml> <output_job.py>")
        sys.exit(1)
    generate_job(sys.argv[1], sys.argv[2])
