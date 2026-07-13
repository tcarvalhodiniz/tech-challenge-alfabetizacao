"""Ingestão Batch → Bronze: lê as tabelas do BigQuery e grava em Delta com metadados de ingestão."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ler_tabela_bd(dataset_id: str, table_id: str, billing_project_id: str, limite: int | None = None):
    """
    Lê uma tabela da Base dos Dados (BigQuery) como pandas DataFrame, via cliente oficial
    do BigQuery (o `basedosdados.read_sql` força login interativo, inviável no servidor).
    """
    from google.cloud import bigquery

    query = f"SELECT * FROM `basedosdados.{dataset_id}.{table_id}`"
    if limite:
        query += f" LIMIT {limite}"

    client = bigquery.Client(project=billing_project_id)
    # download via REST (evita exigir permissão do BigQuery Storage)
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def gravar_bronze(spark: SparkSession, pdf, fonte: str, destino: str) -> int:
    """Grava em Delta (particionado por data de ingestão) com metadados; retorna nº de linhas."""
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
