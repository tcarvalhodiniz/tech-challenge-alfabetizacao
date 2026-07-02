# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Batch → Bronze
# MAGIC
# MAGIC Lê as tabelas do dataset `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery)
# MAGIC e grava na camada **Bronze** em Delta, sem regras de negócio — apenas com
# MAGIC metadados de ingestão e particionamento por data.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC 1. Criar o Volume `workspace.default.alfabetizacao` (Catalog → Create → Volume).
# MAGIC 2. Um projeto GCP com BigQuery habilitado (billing) e credenciais disponíveis no
# MAGIC    cluster (`GOOGLE_APPLICATION_CREDENTIALS` apontando para a chave da service account).
# MAGIC 3. Ajustar `BILLING_PROJECT_ID` abaixo.

# COMMAND ----------

# MAGIC %pip install basedosdados
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

# Torna os módulos do repositório importáveis (repo adicionado como Git folder no Databricks).
sys.path.append("/Workspace/Repos/tech-challenge-alfabetizacao")
sys.path.append("..")

# Credencial da service account, enviada para o Volume `alfabetizacao`.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Volumes/workspace/default/alfabetizacao/tech-challenge-fiap-501217-170d3c25c0e2.json"
)

from config import settings
from src.ingestion.batch_basedosdados import ingerir_batch

BILLING_PROJECT_ID = settings.BILLING_PROJECT_ID  # ajuste em config/settings.py ou variável de ambiente
BRONZE = settings.PATHS["bronze"]

print("Destino Bronze:", BRONZE)
print("Dataset:", f"{settings.BD_PROJECT}.{settings.DATASET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão das tabelas
# MAGIC
# MAGIC `alunos` são microdados (volumoso) — em desenvolvimento usamos um `LIMIT` para
# MAGIC agilizar; em produção, remover o limite.

# COMMAND ----------

# limite por tabela (None = tabela inteira). Ajuste conforme o ambiente.
LIMITES = {"alunos": 100_000}

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
# MAGIC ## Validação da Bronze
# MAGIC
# MAGIC Confirma que cada tabela foi gravada, com os metadados de ingestão.

# COMMAND ----------

display(spark.createDataFrame(resultados))

# COMMAND ----------

# amostra de uma tabela para conferência do schema e dos metadados
df_municipio = spark.read.format("delta").load(f"{BRONZE}/municipio")
df_municipio.printSchema()
display(df_municipio.limit(10))
