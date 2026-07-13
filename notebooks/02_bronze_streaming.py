# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingestão Streaming → Bronze
# MAGIC
# MAGIC Produtor grava eventos NDJSON na landing → Structured Streaming consome (com checkpoint) → Bronze.
# MAGIC Só usa o Volume `alfabetizacao` (sem BigQuery/credencial).

# COMMAND ----------

import os
import sys

# raiz do repo no sys.path
_raiz = os.getcwd()
while _raiz != "/" and not os.path.isdir(os.path.join(_raiz, "config")):
    _raiz = os.path.dirname(_raiz)
sys.path.insert(0, _raiz)
sys.path.append("..")

from config import settings
from src.ingestion.streaming_producer import produzir_eventos
from src.ingestion.streaming_bronze import iniciar_stream_bronze

LANDING = settings.PATHS["streaming_in"]
CKPT = settings.PATHS["streaming_ckpt"]
DESTINO = settings.PATHS["bronze_indicador"]
FONTE = settings.TOPICO_STREAMING

print("Landing (eventos):", LANDING)
print("Checkpoint:", CKPT)
print("Destino Bronze:", DESTINO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Opcional) Reset
# MAGIC Zera landing, checkpoint e destino para rodar do zero. Comente para acumular eventos.

# COMMAND ----------

for _p in (LANDING, CKPT, DESTINO):
    try:
        dbutils.fs.rm(_p, recurse=True)
    except Exception:
        pass
print("Diretórios de streaming zerados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Produtor — gera eventos na landing

# COMMAND ----------

arquivos = produzir_eventos(
    destino_dir=LANDING,
    n_lotes=5,
    eventos_por_lote=20,
    intervalo_seg=2.0,
    ano=2024,
)
print(f"\n{len(arquivos)} arquivos de eventos gerados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consumidor — Structured Streaming → Bronze
# MAGIC `continuo=False` (availableNow) processa e encerra; `continuo=True` roda contínuo.

# COMMAND ----------

query = iniciar_stream_bronze(
    spark=spark,
    origem=LANDING,
    destino=DESTINO,
    checkpoint=CKPT,
    fonte=FONTE,
    continuo=False,
)
query.awaitTermination()
print("Streaming concluído.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação da Bronze (streaming)

# COMMAND ----------

from pyspark.sql import functions as F

df_stream = spark.read.format("delta").load(DESTINO)
print("Total de eventos ingeridos:", df_stream.count())
df_stream.printSchema()

# COMMAND ----------

# distribuição por UF, para conferência rápida
display(
    df_stream.groupBy("sigla_uf")
    .agg(
        F.count("*").alias("eventos"),
        F.round(F.avg("taxa_alfabetizacao"), 1).alias("taxa_media"),
    )
    .orderBy(F.desc("eventos"))
)

# COMMAND ----------

display(df_stream.limit(10))
