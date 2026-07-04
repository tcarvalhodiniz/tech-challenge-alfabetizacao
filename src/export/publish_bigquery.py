"""
Publica as tabelas Gold no BigQuery.

Roda o pipeline (Bronze no BigQuery → Silver → Gold) usando os mesmos módulos de
transformação do projeto e carrega as tabelas Gold num dataset do BigQuery, de onde
o Looker Studio consome. É a via portátil de export — o código de transformação é o
mesmo que roda no Databricks (só muda a origem/destino), alternativa ao conector
`spark-bigquery` do notebook `05_gold`.

Uso:
    GOOGLE_APPLICATION_CREDENTIALS=chave.json python -m src.export.publish_bigquery
"""

import os
import sys

_RAIZ = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _RAIZ)

from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import settings
from src.transformations import silver_clean as clean
from src.transformations import silver_metas as metas
from src.transformations import gold
from src.ingestion.streaming_producer import gerar_evento

BQ_DATASET = "alfabetizacao_gold"
BQ_LOCATION = "US"


def _ler_bronze_bq(spark, client, tabela):
    """Lê uma tabela da Base dos Dados (BigQuery) como Spark DataFrame."""
    query = f"SELECT * FROM `basedosdados.{settings.DATASET_ID}.{tabela}`"
    pdf = client.query(query).to_dataframe(create_bqstorage_client=False)
    return spark.createDataFrame(pdf)


def construir_gold(spark, client):
    """Reproduz Silver + Gold a partir da Bronze no BigQuery. Retorna {nome: df}."""
    dicionario = _ler_bronze_bq(spark, client, "dicionario")

    def preparar(nome):
        df = clean.limpar(_ler_bronze_bq(spark, client, nome))
        df = clean.decodificar(df, dicionario, "rede")
        df = clean.decodificar(df, dicionario, "serie")
        return clean.preencher_categoria_nula(df, ["rede_desc", "serie_desc"])

    municipio = preparar("municipio")
    uf = preparar("uf")
    meta_muni = metas.despivotar_metas(
        clean.limpar(_ler_bronze_bq(spark, client, "meta_alfabetizacao_municipio")),
        settings.COLUNAS_META,
    )
    meta_uf = metas.despivotar_metas(
        clean.limpar(_ler_bronze_bq(spark, client, "meta_alfabetizacao_uf")),
        settings.COLUNAS_META,
    )

    # streaming sintético → Silver indicador_stream (mesma métrica, near-real-time)
    eventos = [gerar_evento(2024) for _ in range(200)]
    stream = clean.limpar(spark.createDataFrame(eventos)).dropDuplicates(["evento_id"])
    stream = stream.withColumn("timestamp_evento", F.to_timestamp("timestamp_evento"))

    return {
        "indicador_municipio": gold.indicador_municipio(municipio, meta_muni),
        "evolucao_municipio": gold.evolucao_municipio(municipio, meta_muni),
        "indicador_uf": gold.indicador_uf(uf, meta_uf),
        "indicador_stream_recente": gold.indicador_stream_recente(stream),
    }


def main():
    projeto = settings.BILLING_PROJECT_ID
    client = bigquery.Client(project=projeto)
    spark = (
        SparkSession.builder.master("local[*]").appName("publish_gold_bq").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tabelas = construir_gold(spark, client)

    dataset = bigquery.Dataset(f"{projeto}.{BQ_DATASET}")
    dataset.location = BQ_LOCATION
    client.create_dataset(dataset, exists_ok=True)
    print(f"dataset pronto: {projeto}.{BQ_DATASET}")

    for nome, df in tabelas.items():
        pdf = df.toPandas()
        destino = f"{projeto}.{BQ_DATASET}.{nome}"
        job = client.load_table_from_dataframe(
            pdf,
            destino,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job.result()
        print(f"  {nome:26s} -> {destino} ({len(pdf):,} linhas)")

    spark.stop()
    print("Publicação concluída.")


if __name__ == "__main__":
    main()
