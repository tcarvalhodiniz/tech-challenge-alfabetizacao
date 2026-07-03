"""
Silver — despivot das metas (formato largo → longo).

As tabelas `meta_alfabetizacao_*` trazem uma coluna por ano-alvo
(`meta_alfabetizacao_2024` … `meta_alfabetizacao_2030`). Aqui essas colunas viram
linhas (`ano_meta`, `valor_meta`), o que permite comparar meta × realizado ano a
ano na Gold. As demais colunas (chaves e o realizado) são preservadas.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def despivotar_metas(df: DataFrame, colunas_meta: list[str]) -> DataFrame:
    """
    Converte as colunas `meta_alfabetizacao_<ano>` em linhas (ano_meta, valor_meta).

    `colunas_meta` é a lista esperada (ex.: settings.COLUNAS_META); só entram as que
    existem de fato na tabela. Linhas sem valor de meta são descartadas.
    """
    metas = [c for c in colunas_meta if c in df.columns]
    if not metas:
        return df

    demais = [c for c in df.columns if c not in metas]
    # monta os pares "ano-alvo", valor para o stack (ex.: '2024', `meta_alfabetizacao_2024`)
    pares = ", ".join(f"'{c.split('_')[-1]}', `{c}`" for c in metas)
    stack = F.expr(f"stack({len(metas)}, {pares}) as (ano_meta, valor_meta)")

    return (
        df.select(*demais, stack)
        .withColumn("ano_meta", F.col("ano_meta").cast("int"))
        .filter(F.col("valor_meta").isNotNull())
    )
