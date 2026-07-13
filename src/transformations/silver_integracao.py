"""Silver — integração: junta município (realizado) × metas municipais e deriva a UF do id_municipio."""

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
    Junta o realizado (`municipio`) com as metas municipais por id_municipio + ano + rede.
    O join usa `rede_desc` (código decodificado) == `rede` da meta (que vem como texto).
    """
    metas = meta_muni_long.select(
        F.col("id_municipio").alias("_meta_id_municipio"),
        F.col("ano").alias("_meta_ano"),
        F.col("rede").alias("_meta_rede_desc"),
        F.col("ano_meta"),
        F.col("valor_meta").alias("meta_taxa_alfabetizacao"),
    )
    condicao = (
        (municipio["id_municipio"] == metas["_meta_id_municipio"])
        & (municipio["ano"] == metas["_meta_ano"])
        & (municipio["rede_desc"] == metas["_meta_rede_desc"])
    )
    integrado = municipio.join(metas, condicao, "left").drop(
        "_meta_id_municipio", "_meta_ano", "_meta_rede_desc"
    )
    return adicionar_sigla_uf(integrado)
