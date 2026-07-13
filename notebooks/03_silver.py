# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Silver
# MAGIC
# MAGIC Limpa e padroniza cada base, decodifica `rede`/`serie`, despivota as metas e integra município × meta.
# MAGIC Pré-req: Bronze gravada (01 e 02).

# COMMAND ----------

import os
import sys

_raiz = os.getcwd()
while _raiz != "/" and not os.path.isdir(os.path.join(_raiz, "config")):
    _raiz = os.path.dirname(_raiz)
sys.path.insert(0, _raiz)
sys.path.append("..")

# limpa cache dos módulos do repo (git pull vale sem reiniciar o Python)
for _m in [m for m in list(sys.modules) if m == "config" or m.startswith(("config.", "src."))]:
    del sys.modules[_m]

from config import settings
from src.transformations import silver_clean as clean
from src.transformations import silver_metas as metas
from src.transformations import silver_alunos as sa
from src.transformations import silver_integracao as integra

BRONZE = settings.PATHS["bronze"]
SILVER = settings.PATHS["silver"]
BRONZE_INDICADOR = settings.PATHS["bronze_indicador"]

def ler_bronze(nome):
    return spark.read.format("delta").load(f"{BRONZE}/{nome}")

print("Bronze:", BRONZE)
print("Silver:", SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Limpeza, padronização e decodificação

# COMMAND ----------

dicionario = ler_bronze("dicionario")

def preparar(nome):
    df = clean.limpar(ler_bronze(nome))
    df = clean.decodificar(df, dicionario, "rede")
    df = clean.decodificar(df, dicionario, "serie")
    return clean.preencher_categoria_nula(df, ["rede_desc", "serie_desc"])

uf        = preparar("uf")
municipio = preparar("municipio")
alunos    = preparar("alunos")

# metas: limpa e despivota (`rede` já vem como texto, não decodifica)
def preparar_meta(nome):
    df = clean.limpar(ler_bronze(nome))
    return metas.despivotar_metas(df, settings.COLUNAS_META)

meta_brasil    = preparar_meta("meta_alfabetizacao_brasil")
meta_uf        = preparar_meta("meta_alfabetizacao_uf")
meta_municipio = preparar_meta("meta_alfabetizacao_municipio")

# streaming: limpa e deduplica por evento_id
indicador_stream = clean.limpar(
    spark.read.format("delta").load(BRONZE_INDICADOR)
).dropDuplicates(["evento_id"])

print("municipio:", municipio.count(), "| indicador_stream:", indicador_stream.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregação dos alunos (taxa recalculada, corte 743)

# COMMAND ----------

taxa_alunos_calc = sa.agregar_taxa_alunos(alunos, settings.PONTO_CORTE_SAEB)
display(taxa_alunos_calc.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integração — indicador por município (realizado × meta)

# COMMAND ----------

indicador_municipio = integra.integrar_indicador_municipio(municipio, meta_municipio)
print("indicador_municipio =", indicador_municipio.count(), "linhas")
indicador_municipio.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação da Silver

# COMMAND ----------

def gravar_silver(df, nome, particao="ano"):
    destino = f"{SILVER}/{nome}"
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if particao in df.columns:
        w = w.partitionBy(particao)
    w.save(destino)
    return destino

tabelas = {
    "uf": uf,
    "municipio": municipio,
    "alunos": alunos,
    "indicador_stream": indicador_stream,
    "meta_brasil": meta_brasil,
    "meta_uf": meta_uf,
    "meta_municipio": meta_municipio,
    "taxa_alunos_calc": taxa_alunos_calc,
    "indicador_municipio": indicador_municipio,
}
for nome, df in tabelas.items():
    destino = gravar_silver(df, nome)
    print(f"  {nome:22s} -> {destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação

# COMMAND ----------

from pyspark.sql import functions as F

# chaves nulas na tabela integrada (não deveria haver)
display(
    indicador_municipio.select(
        F.sum(F.col("id_municipio").isNull().cast("int")).alias("id_municipio_nulo"),
        F.sum(F.col("ano").isNull().cast("int")).alias("ano_nulo"),
        F.sum(F.col("sigla_uf").isNull().cast("int")).alias("sigla_uf_nula"),
    )
)

# COMMAND ----------

# amostra: realizado x meta por município
display(
    indicador_municipio.select(
        "id_municipio", "sigla_uf", "ano", "rede", "rede_desc",
        "taxa_alfabetizacao", "ano_meta", "meta_taxa_alfabetizacao",
    ).limit(15)
)
