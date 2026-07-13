"""Silver — recalcula a taxa de alfabetização dos microdados (proficiência ≥ 743, ponderada) por município."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agregar_taxa_alunos(alunos: DataFrame, ponto_corte: float) -> DataFrame:
    """Taxa ponderada por município/ano/rede: peso dos alunos com proficiência ≥ corte / peso total."""
    peso = F.coalesce(F.col("peso_aluno").cast("double"), F.lit(1.0))
    alfabetizado = (F.col("proficiencia") >= F.lit(ponto_corte)).cast("double")

    return (
        alunos
        .filter(F.col("proficiencia").isNotNull())
        .groupBy("id_municipio", "ano", "rede")
        .agg(
            F.round(100 * F.sum(peso * alfabetizado) / F.sum(peso), 1).alias("taxa_alfabetizacao_calc"),
            F.round(F.sum(peso), 1).alias("peso_total"),
            F.count("*").alias("qtd_alunos"),
        )
    )
