"""Ingestão Streaming → Bronze: consome os eventos NDJSON da landing (com checkpoint) e grava em Delta."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def schema_eventos() -> T.StructType:
    """Schema explícito dos eventos (evita inferência em streaming)."""
    return T.StructType(
        [
            T.StructField("evento_id", T.StringType()),
            T.StructField("id_municipio", T.StringType()),
            T.StructField("sigla_uf", T.StringType()),
            T.StructField("ano", T.IntegerType()),
            T.StructField("rede", T.StringType()),
            T.StructField("serie", T.StringType()),
            T.StructField("taxa_alfabetizacao", T.DoubleType()),
            T.StructField("timestamp_evento", T.StringType()),
        ]
    )


def iniciar_stream_bronze(
    spark: SparkSession,
    origem: str,
    destino: str,
    checkpoint: str,
    fonte: str,
    continuo: bool = False,
):
    """
    Lê os eventos NDJSON de `origem` e grava em Delta (`destino`) com checkpoint.
    `continuo=False`: availableNow (processa e encerra). `continuo=True`: micro-batches contínuos.
    """
    leitura = (
        spark.readStream
        .schema(schema_eventos())
        .option("maxFilesPerTrigger", 1)  # um arquivo por micro-batch
        .json(origem)
    )

    enriquecido = (
        leitura
        .withColumn("timestamp_evento", F.to_timestamp("timestamp_evento"))
        .withColumn("data_ingestao", F.current_timestamp())
        .withColumn("data_ingestao_dt", F.current_date())
        .withColumn("fonte", F.lit(fonte))
        .withColumn("tipo_ingestao", F.lit("streaming"))
    )

    escrita = (
        enriquecido.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("data_ingestao_dt")
    )
    escrita = (
        escrita.trigger(processingTime="10 seconds")
        if continuo
        else escrita.trigger(availableNow=True)
    )
    return escrita.start(destino)
