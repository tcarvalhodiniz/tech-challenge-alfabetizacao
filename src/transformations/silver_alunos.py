"""
Silver — agregação dos microdados de alunos.

Recalcula a taxa de alfabetização a partir da proficiência individual vs o ponto de
corte do Saeb (743), ponderada pelo peso amostral, por município/ano/rede. Serve de
cross-check da taxa oficial na Gold.

Obs.: em desenvolvimento os microdados vêm com `LIMIT`, então isso é uma amostra —
com a base inteira (produção) a taxa recalculada tende a bater com a oficial.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def agregar_taxa_alunos(alunos: DataFrame, ponto_corte: float) -> DataFrame:
    """
    Agrega os alunos por município/ano/rede e calcula a taxa de alfabetização
    ponderada: soma dos pesos dos alunos com `proficiencia >= ponto_corte` sobre a
    soma total dos pesos. Considera apenas alunos com proficiência registrada.
    """
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
