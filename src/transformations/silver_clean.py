"""Silver — limpeza, normalização de chaves e decodificação de categorias."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_METADADOS_BRONZE = ["data_ingestao", "data_ingestao_dt", "fonte", "tipo_ingestao"]


def normalizar_chaves(df: DataFrame) -> DataFrame:
    """Padroniza as chaves (id_municipio 7 díg, sigla_uf maiúscula, ano int, rede/serie trim)."""
    cols = df.columns
    if "id_municipio" in cols:
        df = df.withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
    if "sigla_uf" in cols:
        df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf").cast("string"))))
    if "ano" in cols:
        df = df.withColumn("ano", F.col("ano").cast("int"))
    for cat in ("rede", "serie"):
        if cat in cols:
            df = df.withColumn(cat, F.trim(F.col(cat).cast("string")))
    return df


def descartar_metadados(df: DataFrame) -> DataFrame:
    """Remove os metadados de ingestão herdados da Bronze."""
    return df.drop(*[m for m in _METADADOS_BRONZE if m in df.columns])


def preencher_categoria_nula(df: DataFrame, colunas: list[str], rotulo: str = "Não informado") -> DataFrame:
    """Preenche dimensões categóricas nulas com um rótulo genérico (só categorias, não medidas)."""
    for c in colunas:
        if c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit(rotulo)))
    return df


def decodificar(df: DataFrame, dicionario: DataFrame, coluna: str, id_tabela: str | None = None) -> DataFrame:
    """Traduz os códigos de uma coluna categórica para `<coluna>_desc` via `dicionario` (mantém o código)."""
    if coluna not in df.columns:
        return df

    dic = dicionario.filter(F.col("nome_coluna") == coluna)
    if id_tabela:
        dic = dic.filter(F.col("id_tabela") == id_tabela)

    de_para = (
        dic.select(
            F.trim(F.col("chave").cast("string")).alias("_chave"),
            F.col("valor").alias(f"{coluna}_desc"),
        )
        .dropDuplicates(["_chave"])
    )
    return df.join(de_para, df[coluna] == de_para["_chave"], "left").drop("_chave")


def limpar(df: DataFrame) -> DataFrame:
    """Limpeza padrão Bronze → Silver: normaliza chaves e tira metadados."""
    return descartar_metadados(normalizar_chaves(df))
