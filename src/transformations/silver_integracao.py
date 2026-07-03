"""
Silver — integração das bases.

Junta o resultado por município (realizado) com as metas municipais despivotadas e
acrescenta a UF, formando a visão integrada município × ano × rede que alimenta a
Gold. A UF é derivada dos 2 primeiros dígitos do `id_municipio` (código IBGE), sem
precisar de uma fonte extra.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Código IBGE da UF (2 primeiros dígitos do id_municipio) -> sigla
UF_POR_CODIGO = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


def adicionar_sigla_uf(df: DataFrame) -> DataFrame:
    """Deriva `sigla_uf` a partir dos 2 primeiros dígitos do `id_municipio`."""
    mapa = F.create_map([F.lit(x) for kv in UF_POR_CODIGO.items() for x in kv])
    return df.withColumn("sigla_uf", mapa[F.substring(F.col("id_municipio"), 1, 2)])


def integrar_indicador_municipio(municipio: DataFrame, meta_muni_long: DataFrame) -> DataFrame:
    """
    Integra o realizado (tabela `municipio`) com as metas municipais despivotadas.

    Join por `id_municipio` + `ano` + `rede` (a meta não tem `serie`; ela vale para
    o município/ano/rede). O resultado é uma linha por município × ano × rede × serie
    × ano-meta, com o realizado e a meta lado a lado, e a `sigla_uf`.
    """
    metas = meta_muni_long.select(
        "id_municipio", "ano", "rede",
        F.col("ano_meta"),
        F.col("valor_meta").alias("meta_taxa_alfabetizacao"),
    )
    integrado = municipio.join(metas, on=["id_municipio", "ano", "rede"], how="left")
    return adicionar_sigla_uf(integrado)
