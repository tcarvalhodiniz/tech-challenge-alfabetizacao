# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingestão Streaming → Bronze
# MAGIC
# MAGIC Simula a chegada de medições do indicador em tempo quase-real e as ingere na
# MAGIC camada **Bronze** via **Spark Structured Streaming**, completando a pipeline
# MAGIC híbrida (batch + streaming) exigida pelo desafio.
# MAGIC
# MAGIC **Fluxo:** o produtor grava lotes de eventos NDJSON na landing do Volume →
# MAGIC o Structured Streaming lê incrementalmente (com checkpoint) → grava na Bronze em Delta.
# MAGIC
# MAGIC **Pré-requisitos:** apenas o Volume `workspace.default.alfabetizacao`.
# MAGIC Diferente do batch, **não usa BigQuery nem credenciais** — os eventos são
# MAGIC gerados localmente.

# COMMAND ----------

import os
import sys

# Torna os módulos do repositório importáveis (mesma lógica robusta do notebook batch).
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
# MAGIC ## (Opcional) Reset para uma demonstração limpa
# MAGIC
# MAGIC Zera a landing, o checkpoint e a tabela de destino para poder rodar o notebook
# MAGIC do início quantas vezes quiser. Comente esta célula se quiser **acumular** eventos
# MAGIC entre execuções.

# COMMAND ----------

for _p in (LANDING, CKPT, DESTINO):
    try:
        dbutils.fs.rm(_p, recurse=True)
    except Exception:
        pass  # diretório ainda não existe — nada a remover
print("Diretórios de streaming zerados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Produtor — gera eventos na landing
# MAGIC
# MAGIC 5 lotes de 20 eventos, com 2s entre lotes, simulando a chegada gradual.

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
# MAGIC
# MAGIC `availableNow=True`: processa todos os eventos já disponíveis e encerra
# MAGIC (bom pra validar no notebook). Pra simular o fluxo contínuo, troque para
# MAGIC `continuo=True` e deixe a célula rodando enquanto o produtor gera mais lotes.

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
