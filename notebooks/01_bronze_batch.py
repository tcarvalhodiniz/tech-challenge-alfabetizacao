# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Batch → Bronze
# MAGIC
# MAGIC Lê as 7 tabelas do dataset `basedosdados.br_inep_avaliacao_alfabetizacao` (as 6 entidades + o dicionário)
# MAGIC e grava na Bronze em Delta. Pré-req: Volume `alfabetizacao`, chave da service account no Volume e `BILLING_PROJECT_ID` no settings.

# COMMAND ----------

# MAGIC %pip install basedosdados "numpy<2"
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

# raiz do repo no sys.path (sobe até achar config/)
_raiz = os.getcwd()
while _raiz != "/" and not os.path.isdir(os.path.join(_raiz, "config")):
    _raiz = os.path.dirname(_raiz)
sys.path.insert(0, _raiz)
sys.path.append("..")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Volumes/workspace/default/alfabetizacao/tech-challenge-fiap-501217-170d3c25c0e2.json"
)

from config import settings
from src.ingestion.batch_basedosdados import ingerir_batch

BILLING_PROJECT_ID = settings.BILLING_PROJECT_ID
BRONZE = settings.PATHS["bronze"]

print("Destino Bronze:", BRONZE)
print("Dataset:", f"{settings.BD_PROJECT}.{settings.DATASET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão das tabelas
# MAGIC `alunos` (microdados) fica com `LIMIT` em dev; em produção, sem limite.

# COMMAND ----------

LIMITES = {"alunos": 100_000}  # None = tabela inteira

resultados = []
for table_id in settings.TABELAS_BATCH:
    print(f"→ Ingerindo {table_id} ...")
    res = ingerir_batch(
        spark=spark,
        dataset_id=settings.DATASET_ID,
        table_id=table_id,
        billing_project_id=BILLING_PROJECT_ID,
        bronze_base=BRONZE,
        limite=LIMITES.get(table_id),
    )
    resultados.append(res)
    print(f"  {res['linhas']:,} linhas → {res['destino']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

display(spark.createDataFrame(resultados))

# COMMAND ----------

# amostra para conferir schema e metadados
df_municipio = spark.read.format("delta").load(f"{BRONZE}/municipio")
df_municipio.printSchema()
display(df_municipio.limit(10))
