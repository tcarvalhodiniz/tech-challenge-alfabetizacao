# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Qualidade + Quarentena
# MAGIC
# MAGIC Regras (nulo/duplicidade em chave, percentual e ano fora de faixa) separam aprovados de quarentena, com o motivo.
# MAGIC Pré-req: Silver gravada (03).

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
from src.quality import regras

SILVER = settings.PATHS["silver"]
QUARENTENA = settings.PATHS["quarentena"]

def ler_silver(nome):
    return spark.read.format("delta").load(f"{SILVER}/{nome}")

print("Silver:", SILVER)
print("Quarentena:", QUARENTENA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alvos da avaliação
# MAGIC (a integrada `indicador_municipio` não entra: seus nulos de meta são esperados)

# COMMAND ----------

# (tabela, chaves, colunas_percentuais)
ALVOS = [
    ("municipio",        ["id_municipio", "ano", "rede", "serie"], ["taxa_alfabetizacao"]),
    ("uf",               ["sigla_uf", "ano", "rede", "serie"],     ["taxa_alfabetizacao"]),
    ("meta_municipio",   ["id_municipio", "ano", "rede", "ano_meta"], ["valor_meta"]),
    ("indicador_stream", ["evento_id"],                            ["taxa_alfabetizacao"]),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Avaliação + gravação da quarentena

# COMMAND ----------

resumos = []
quarentenas = {}
for nome, chaves, pct in ALVOS:
    df = ler_silver(nome)
    aprovados, quarentena = regras.avaliar_qualidade(df, chaves, pct, settings.LIMITES)

    # grava a quarentena (mesmo vazia)
    (quarentena.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(f"{QUARENTENA}/{nome}"))

    quarentenas[nome] = quarentena
    resumos.append(regras.resumo_qualidade(nome, aprovados, quarentena))
    print(f"  {nome:18s} avaliada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de qualidade

# COMMAND ----------

display(spark.createDataFrame(resumos).select("tabela", "total", "aprovados", "quarentena", "pct_qualidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Motivos de quarentena (quando houver)

# COMMAND ----------

from pyspark.sql import functions as F

for nome, q in quarentenas.items():
    n = q.count()
    if n:
        print(f"### {nome}: {n} registro(s) em quarentena")
        display(q.groupBy("motivo_quarentena").agg(F.count("*").alias("qtd")).orderBy(F.desc("qtd")))
    else:
        print(f"### {nome}: 0 registros em quarentena ✓")
