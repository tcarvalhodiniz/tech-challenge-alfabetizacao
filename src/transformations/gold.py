"""
Gold — datasets analíticos prontos para BI/ML.

Consome a Silver e entrega tabelas denormalizadas/agregadas para o dashboard:
meta × realizado por município, evolução temporal rumo a 2030, meta × realizado por
UF e a medição mais recente vinda do streaming.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.transformations.silver_integracao import adicionar_sigla_uf


def indicador_municipio(municipio: DataFrame, meta_muni_long: DataFrame) -> DataFrame:
    """
    Meta × realizado por município/rede/ano.

    O realizado (taxa do ano de avaliação) é alinhado com a meta daquele mesmo ano
    (`valor_meta` onde `ano_meta == ano`). `gap_pp` = meta − realizado (em pontos
    percentuais); `atingiu_meta` = realizado ≥ meta. Municípios/anos sem meta ficam
    com meta/gap nulos (ex.: 2023 não tem meta; meta municipal só existe p/ Municipal).
    """
    base = adicionar_sigla_uf(
        municipio.select(
            "id_municipio", "ano", "rede", "rede_desc", "serie",
            F.col("taxa_alfabetizacao").alias("taxa_realizada"),
        )
    )
    # a trajetória de metas é fixa e vem repetida por ano-fonte (2023/2024) — dedup por
    # (município, ano-alvo, rede) para não duplicar o realizado no join.
    metas = meta_muni_long.select(
        F.col("id_municipio").alias("_id"),
        F.col("ano_meta").alias("_ano"),
        F.col("rede").alias("_rede"),
        F.col("valor_meta").alias("meta_ano"),
    ).dropDuplicates(["_id", "_ano", "_rede"])
    cond = (
        (base["id_municipio"] == metas["_id"])
        & (base["ano"] == metas["_ano"])
        & (base["rede_desc"] == metas["_rede"])
    )
    return (
        base.join(metas, cond, "left").drop("_id", "_ano", "_rede")
        .withColumn("gap_pp", F.round(F.col("meta_ano") - F.col("taxa_realizada"), 1))
        .withColumn(
            "atingiu_meta",
            F.when(F.col("meta_ano").isNotNull(), F.col("taxa_realizada") >= F.col("meta_ano")),
        )
    )


def evolucao_municipio(municipio: DataFrame, meta_muni_long: DataFrame) -> DataFrame:
    """
    Série temporal por município/rede: o realizado de cada ano avaliado + a trajetória
    de metas até 2030, empilhados (`tipo` = 'realizado' | 'meta'). Alimenta o gráfico
    de linha "onde está × para onde precisa ir".
    """
    realizado = adicionar_sigla_uf(municipio).select(
        "id_municipio", "sigla_uf", "rede_desc",
        F.col("ano").alias("ano_ref"),
        F.col("taxa_alfabetizacao").alias("valor"),
        F.lit("realizado").alias("tipo"),
    )
    meta = adicionar_sigla_uf(meta_muni_long).select(
        "id_municipio", "sigla_uf",
        F.col("rede").alias("rede_desc"),
        F.col("ano_meta").alias("ano_ref"),
        F.col("valor_meta").alias("valor"),
        F.lit("meta").alias("tipo"),
    ).dropDuplicates(["id_municipio", "rede_desc", "ano_ref"])
    return realizado.unionByName(meta)


def indicador_uf(uf: DataFrame, meta_uf_long: DataFrame) -> DataFrame:
    """
    Meta × realizado por UF/rede/ano, usando os resultados oficiais por UF.

    A meta estadual vem com `rede = "Pública"`. Assumimos que corresponde à
    "Pública (Estadual e Municipal)" (código 5) — a rede pública de ensino básico.
    >>> PREMISSA A CONFIRMAR: "Pública" (meta) == "Pública (Estadual e Municipal)". <<<
    """
    realizado = uf.select(
        "sigla_uf", "ano", "rede", "rede_desc",
        F.col("taxa_alfabetizacao").alias("taxa_realizada"),
    )
    metas = meta_uf_long.select(
        F.col("sigla_uf").alias("_uf"),
        F.col("ano_meta").alias("_ano"),
        F.col("valor_meta").alias("meta_ano"),
    ).dropDuplicates(["_uf", "_ano"])
    cond = (
        (realizado["sigla_uf"] == metas["_uf"])
        & (realizado["ano"] == metas["_ano"])
        & (realizado["rede_desc"] == F.lit("Pública (Estadual e Municipal)"))
    )
    return (
        realizado.join(metas, cond, "left").drop("_uf", "_ano")
        .withColumn("gap_pp", F.round(F.col("meta_ano") - F.col("taxa_realizada"), 1))
    )


def indicador_stream_recente(indicador_stream: DataFrame) -> DataFrame:
    """
    Última medição do indicador vinda do streaming, por município (near-real-time).
    Usa `timestamp_evento` para pegar o evento mais recente de cada município — é a
    reconciliação batch × streaming acontecendo na Gold.
    """
    w = Window.partitionBy("id_municipio").orderBy(F.col("timestamp_evento").desc())
    return (
        indicador_stream.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
