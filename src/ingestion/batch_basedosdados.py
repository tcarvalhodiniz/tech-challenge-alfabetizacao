"""
Ingestão Batch → Bronze.

Lê as tabelas do dataset `basedosdados.br_inep_avaliacao_alfabetizacao` (BigQuery)
e grava na camada Bronze em Delta, sem transformação de negócio — apenas com
metadados de ingestão e particionamento por data.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ler_tabela_bd(dataset_id: str, table_id: str, billing_project_id: str, limite: int | None = None):
    """
    Lê uma tabela da Base dos Dados (BigQuery) como pandas DataFrame.

    `limite` é útil em desenvolvimento para não baixar microdados inteiros (ex.: `alunos`).
    """
    import basedosdados as bd

    query = f"SELECT * FROM `basedosdados.{dataset_id}.{table_id}`"
    if limite:
        query += f" LIMIT {limite}"
    return bd.read_sql(query=query, billing_project_id=billing_project_id)


def gravar_bronze(spark: SparkSession, pdf, fonte: str, destino: str) -> int:
    """
    Converte o pandas DataFrame para Spark, adiciona metadados de ingestão e grava
    em Delta particionado por data de ingestão. Retorna o número de linhas gravadas.
    """
    sdf: DataFrame = (
        spark.createDataFrame(pdf)
        .withColumn("data_ingestao", F.current_timestamp())
        .withColumn("data_ingestao_dt", F.current_date())
        .withColumn("fonte", F.lit(fonte))
        .withColumn("tipo_ingestao", F.lit("batch"))
    )
    (
        sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("data_ingestao_dt")
        .save(destino)
    )
    return sdf.count()


def ingerir_batch(spark, dataset_id, table_id, billing_project_id, bronze_base, limite=None) -> dict:
    """Orquestra a ingestão de uma tabela: lê da fonte e grava na Bronze."""
    pdf = ler_tabela_bd(dataset_id, table_id, billing_project_id, limite)
    destino = f"{bronze_base}/{table_id}"
    linhas = gravar_bronze(spark, pdf, table_id, destino)
    return {"tabela": table_id, "linhas": linhas, "destino": destino}
