"""Gold — datasets analíticos (meta × realizado, evolução, UF, near-real-time) para BI/ML."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.transformations.silver_integracao import adicionar_sigla_uf


def indicador_municipio(municipio: DataFrame, meta_muni_long: DataFrame) -> DataFrame:
    """Meta × realizado por município/rede/ano, com gap_pp e atingiu_meta."""
    base = adicionar_sigla_uf(
        municipio.select(
            "id_municipio", "ano", "rede", "rede_desc", "serie",
            F.col("taxa_alfabetizacao").alias("taxa_realizada"),
        )
    )
    # dedup: a trajetória de metas vem repetida por ano-fonte (2023/2024)
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
    """Série temporal por município/rede: realizado + metas até 2030 (tipo = 'realizado'|'meta')."""
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
    Meta × realizado por UF (resultados oficiais estaduais). A meta estadual vem com
    `rede = "Pública"`, casada aqui com a rede "Pública (Estadual e Municipal)".
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
    """Última medição do streaming por município (pelo timestamp)."""
    w = Window.partitionBy("id_municipio").orderBy(F.col("timestamp_evento").desc())
    return (
        indicador_stream.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
