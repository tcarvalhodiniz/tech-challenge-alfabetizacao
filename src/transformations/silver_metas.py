"""Silver — despivot das metas: colunas meta_alfabetizacao_<ano> viram linhas (ano_meta, valor_meta)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def despivotar_metas(df: DataFrame, colunas_meta: list[str]) -> DataFrame:
    """Colunas `meta_alfabetizacao_<ano>` → linhas (ano_meta, valor_meta); descarta metas nulas."""
    metas = [c for c in colunas_meta if c in df.columns]
    if not metas:
        return df

    demais = [c for c in df.columns if c not in metas]
    # pares "ano-alvo", valor para o stack
    pares = ", ".join(f"'{c.split('_')[-1]}', `{c}`" for c in metas)
    stack = F.expr(f"stack({len(metas)}, {pares}) as (ano_meta, valor_meta)")

    return (
        df.select(*demais, stack)
        .withColumn("ano_meta", F.col("ano_meta").cast("int"))
        .filter(F.col("valor_meta").isNotNull())
    )
