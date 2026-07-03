"""
Ingestão Streaming → Bronze (Spark Structured Streaming).

Consome os eventos de medição do indicador que o produtor deposita na pasta de
landing (arquivos NDJSON) e grava, de forma incremental e com checkpoint, na
camada Bronze em Delta — sem regras de negócio, apenas com metadados de ingestão.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def schema_eventos() -> T.StructType:
    """
    Schema explícito dos eventos.

    Em streaming a inferência de schema sai cara e imprevisível, então o contrato
    dos eventos fica fixado aqui.
    """
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
    Inicia o job de Structured Streaming: lê os eventos NDJSON de `origem` e grava
    em Delta em `destino`, com checkpoint em `checkpoint`. Retorna a `StreamingQuery`.

    - `continuo=False` (padrão): `trigger(availableNow=True)` — processa tudo o que já
      chegou e encerra sozinho (bom pra rodar e validar no notebook).
    - `continuo=True`: micro-batches a cada 10s, roda indefinidamente (demonstra o
      near-real-time; interrompa a célula para parar).

    O particionamento por `data_ingestao_dt` e o formato Delta seguem o mesmo padrão
    da ingestão batch, mantendo a Bronze homogênea entre as duas fontes.
    """
    leitura = (
        spark.readStream
        .schema(schema_eventos())
        .option("maxFilesPerTrigger", 1)  # um arquivo por micro-batch = chegada gradual
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
