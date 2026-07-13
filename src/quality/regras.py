"""Qualidade — regras (nulo/duplicidade em chave, faixas) que separam aprovados de quarentena, com o motivo."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def avaliar_qualidade(
    df: DataFrame,
    chaves: list[str],
    colunas_pct: list[str] | None = None,
    limites: dict | None = None,
):
    """
    Separa `df` em (aprovados, quarentena) pelas regras. Reprovados ganham `motivo_quarentena`.
    `chaves`: chave primária; `colunas_pct`: colunas em [0,100]; `limites`: faixas (settings.LIMITES).
    """
    colunas_pct = colunas_pct or []
    limites = limites or {}
    chaves = [k for k in chaves if k in df.columns]

    checagens = []

    # 1) nulo em chave
    for k in chaves:
        checagens.append(F.when(F.col(k).isNull(), F.lit(f"chave_nula:{k}")))

    # 2) chave duplicada (todas as linhas do grupo repetido são marcadas)
    if chaves:
        w = Window.partitionBy(*chaves)
        df = df.withColumn("_dup", F.count(F.lit(1)).over(w))
        checagens.append(F.when(F.col("_dup") > 1, F.lit("chave_duplicada")))

    # 3) percentual fora da faixa (só valores não nulos)
    lo_p, hi_p = limites.get("indicador_pct", (0.0, 100.0))
    for c in colunas_pct:
        if c in df.columns:
            fora = F.col(c).isNotNull() & ((F.col(c) < lo_p) | (F.col(c) > hi_p))
            checagens.append(F.when(fora, F.lit(f"{c}_fora_da_faixa")))

    # 4) ano fora da faixa
    if "ano" in df.columns and "ano" in limites:
        lo_a, hi_a = limites["ano"]
        fora = F.col("ano").isNotNull() & ((F.col("ano") < lo_a) | (F.col("ano") > hi_a))
        checagens.append(F.when(fora, F.lit("ano_fora_da_faixa")))

    # motivos aplicáveis (remove os nulls)
    motivos = F.filter(F.array(*checagens), lambda x: x.isNotNull())
    df = df.withColumn("_motivos", motivos)

    descartar = ["_motivos"] + (["_dup"] if chaves else [])
    aprovados = df.filter(F.size("_motivos") == 0).drop(*descartar)
    quarentena = (
        df.filter(F.size("_motivos") > 0)
        .withColumn("motivo_quarentena", F.concat_ws("; ", F.col("_motivos")))
        .drop(*descartar)
    )
    return aprovados, quarentena


def resumo_qualidade(nome: str, aprovados: DataFrame, quarentena: DataFrame) -> dict:
    """Resumo de uma tabela avaliada: totais e % de registros aprovados."""
    ap = aprovados.count()
    qt = quarentena.count()
    total = ap + qt
    return {
        "tabela": nome,
        "total": total,
        "aprovados": ap,
        "quarentena": qt,
        "pct_qualidade": round(100 * ap / total, 2) if total else None,
    }
