"""
Silver — limpeza e padronização.

Deixa cada tabela da Bronze consistente antes da integração: corrige tipos,
normaliza as chaves que ligam as bases (id_municipio, sigla_uf, ano, rede, serie)
e, quando pedido, traduz os códigos categóricos usando a tabela `dicionario`.
Os joins entre as bases ficam no `silver_integracao`.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# metadados que a Bronze acrescenta e que não seguem para a Silver
_METADADOS_BRONZE = ["data_ingestao", "data_ingestao_dt", "fonte", "tipo_ingestao"]


def normalizar_chaves(df: DataFrame) -> DataFrame:
    """
    Padroniza as colunas-chave presentes na tabela, pra que os joins casem:
    - id_municipio: string de 7 dígitos com zero à esquerda
    - sigla_uf: 2 letras maiúsculas, sem espaços
    - ano: inteiro
    - rede / serie: string sem espaços nas pontas
    """
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
    """
    Preenche dimensões categóricas nulas com um rótulo genérico.

    Vale só para colunas de categoria (ex.: `rede_desc`, `serie_desc`) — mantém a
    dimensão usável no BI, sem "perder" linhas ao agrupar. Medidas numéricas NÃO devem
    passar por aqui (null numérico é ausência de medição, não uma categoria).
    """
    for c in colunas:
        if c in df.columns:
            df = df.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit(rotulo)))
    return df


def decodificar(df: DataFrame, dicionario: DataFrame, coluna: str, id_tabela: str | None = None) -> DataFrame:
    """
    Traduz os códigos de uma coluna categórica (ex.: `rede`, `serie`) para a
    descrição legível usando a tabela `dicionario`. Cria `<coluna>_desc` e mantém
    o código original (nada se perde se não houver correspondência).

    Se `id_tabela` for informado, restringe o de-para àquela tabela; senão usa o
    de-para da coluna em qualquer tabela (os códigos de `rede`/`serie` são os mesmos).
    """
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
    """Pipeline padrão de limpeza Bronze → Silver: normaliza chaves e tira metadados."""
    return descartar_metadados(normalizar_chaves(df))
