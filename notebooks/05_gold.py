# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Camada Gold
# MAGIC
# MAGIC Consome a Silver e entrega os datasets analíticos para o dashboard (Looker Studio)
# MAGIC e para exploração/ML:
# MAGIC
# MAGIC - **indicador_municipio** — meta × realizado por município/rede/ano (+ gap, atingiu_meta)
# MAGIC - **evolucao_municipio** — série temporal (realizado + trajetória de metas até 2030)
# MAGIC - **indicador_uf** — meta × realizado por UF (resultados oficiais estaduais)
# MAGIC - **indicador_stream_recente** — última medição do streaming por município (near-real-time)
# MAGIC
# MAGIC **Pré-requisitos:** Silver já gravada (notebook 03).

# COMMAND ----------

import os
import sys

_raiz = os.getcwd()
while _raiz != "/" and not os.path.isdir(os.path.join(_raiz, "config")):
    _raiz = os.path.dirname(_raiz)
sys.path.insert(0, _raiz)
sys.path.append("..")

# limpa cache de módulos do repo (para um git pull recente valer sem reiniciar o Python)
for _m in [m for m in list(sys.modules) if m == "config" or m.startswith(("config.", "src."))]:
    del sys.modules[_m]

from config import settings
from src.transformations import gold

SILVER = settings.PATHS["silver"]
GOLD = settings.PATHS["gold"]

def ler_silver(nome):
    return spark.read.format("delta").load(f"{SILVER}/{nome}")

print("Silver:", SILVER)
print("Gold:", GOLD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Construção das tabelas Gold

# COMMAND ----------

municipio      = ler_silver("municipio")
uf             = ler_silver("uf")
meta_municipio = ler_silver("meta_municipio")
meta_uf        = ler_silver("meta_uf")
indic_stream   = ler_silver("indicador_stream")

g_municipio = gold.indicador_municipio(municipio, meta_municipio)
g_evolucao  = gold.evolucao_municipio(municipio, meta_municipio)
g_uf        = gold.indicador_uf(uf, meta_uf)
g_stream    = gold.indicador_stream_recente(indic_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gravação da Gold (Delta, particionado por ano quando aplicável)

# COMMAND ----------

def gravar_gold(df, nome, particao=None):
    destino = f"{GOLD}/{nome}"
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if particao and particao in df.columns:
        w = w.partitionBy(particao)
    w.save(destino)
    return destino

for nome, df, part in [
    ("indicador_municipio", g_municipio, "ano"),
    ("evolucao_municipio",  g_evolucao,  "ano_ref"),
    ("indicador_uf",        g_uf,        "ano"),
    ("indicador_stream_recente", g_stream, None),
]:
    print(f"  {nome:26s} -> {gravar_gold(df, nome, part)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação / amostras

# COMMAND ----------

from pyspark.sql import functions as F

# meta x realizado do ano mais recente que tem meta (2024), rede Municipal
print("indicador_municipio (2024, Municipal, com meta):")
display(
    g_municipio.filter((F.col("ano") == 2024) & (F.col("meta_ano").isNotNull()))
    .select("id_municipio", "sigla_uf", "rede_desc", "taxa_realizada", "meta_ano", "gap_pp", "atingiu_meta")
    .orderBy(F.desc("gap_pp"))
    .limit(15)
)

# COMMAND ----------

# resumo: % de municípios que atingiram a meta em 2024 (rede Municipal)
print("Atingimento da meta 2024 (Municipal):")
display(
    g_municipio.filter((F.col("ano") == 2024) & (F.col("meta_ano").isNotNull()))
    .groupBy("atingiu_meta").agg(F.count("*").alias("qtd_municipios"))
)

# COMMAND ----------

# evolução de um município (linha realizado + metas)
print("evolucao_municipio (exemplo de trajetória):")
display(g_evolucao.orderBy("id_municipio", "tipo", "ano_ref").limit(20))

# COMMAND ----------

print("indicador_uf (meta x realizado por UF):")
display(g_uf.filter(F.col("meta_ano").isNotNull()).orderBy("sigla_uf", "ano").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Opcional) Export para o BigQuery
# MAGIC
# MAGIC Publica as tabelas Gold no BigQuery para o Looker Studio consumir. Requer:
# MAGIC 1. um **dataset** BigQuery (ex.: `alfabetizacao_gold`) já criado no projeto;
# MAGIC 2. a service account com papel **BigQuery Data Editor** nesse dataset;
# MAGIC 3. um **bucket GCS** temporário para o conector (`temporaryGcsBucket`).
# MAGIC
# MAGIC Deixe `EXPORTAR_BQ = True` só quando esses pré-requisitos estiverem prontos.

# COMMAND ----------

EXPORTAR_BQ = False
BQ_DATASET = "alfabetizacao_gold"
GCS_BUCKET_TEMP = "SEU_BUCKET_TEMP"  # ajustar

if EXPORTAR_BQ:
    for nome in ["indicador_municipio", "evolucao_municipio", "indicador_uf", "indicador_stream_recente"]:
        (spark.read.format("delta").load(f"{GOLD}/{nome}")
            .write.format("bigquery")
            .option("table", f"{settings.BILLING_PROJECT_ID}.{BQ_DATASET}.{nome}")
            .option("temporaryGcsBucket", GCS_BUCKET_TEMP)
            .mode("overwrite").save())
        print("exportado:", nome)
else:
    print("Export para BigQuery desativado (EXPORTAR_BQ=False).")
