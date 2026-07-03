# Arquitetura da Solução

Pipeline híbrida (Batch + Streaming) em arquitetura **Lakehouse**, organizada nas
camadas medalhão **Bronze → Silver → Gold**. O desenvolvimento e a execução do
Spark acontecem no **Databricks Free Edition** (Delta Lake + Structured Streaming), e
a nuvem de destino é o **GCP** (fonte e camada analítica no BigQuery, dashboard no
Looker Studio).

## Diagrama da pipeline

```mermaid
flowchart TB
    subgraph Fontes["Fontes de dados"]
        BD["Base dos Dados / BigQuery<br/>UF - Municipio - Metas - Alunos"]
        EV["Eventos do indicador<br/>novas medicoes / metas"]
    end

    subgraph Ingestao["Ingestao hibrida"]
        BATCH["Ingestao Batch<br/>PySpark"]
        PROD["Produtor Python"]
        PS["Landing de eventos<br/>Pub/Sub em producao"]
        STREAM["Structured Streaming"]
        PROD --> PS --> STREAM
    end

    subgraph Lakehouse["Lakehouse - Arquitetura Medalhao (Delta Lake)"]
        BRONZE["BRONZE<br/>bruto + metadados<br/>particionado por data de ingestao"]
        SILVER["SILVER<br/>limpeza - tipos - qualidade<br/>normalizacao de chaves<br/>integracao das 6 bases"]
        GOLD["GOLD<br/>indicador por municipio<br/>meta x realizado<br/>evolucao temporal"]
        QUAR["Quarentena<br/>registros invalidos"]
    end

    subgraph Consumo["Consumo - GCP"]
        BQ["BigQuery<br/>camada analitica"]
        LOOKER["Looker Studio<br/>dashboard"]
    end

    BD --> BATCH --> BRONZE
    EV --> PROD
    STREAM --> BRONZE
    BRONZE --> SILVER
    SILVER -. invalidos .-> QUAR
    SILVER --> GOLD
    GOLD --> BQ --> LOOKER
```

## Camadas da arquitetura

### Bronze — dados brutos
- Ingestão fiel das fontes, **sem regras de negócio**.
- Enriquecimento apenas com **metadados de ingestão** (timestamp, fonte, tipo batch/streaming).
- Particionamento por **data de ingestão**; histórico completo preservado para auditoria
  e reprocessamento.

### Silver — dados tratados e integrados
- Limpeza, tratamento de valores ausentes e **padronização de tipos e nomes**.
- **Validação de qualidade** (duplicidade, nulos em chaves, faixas válidas, consistência
  entre tabelas); registros reprovados vão para a **quarentena**.
- **Normalização de chaves** (`id_municipio`, `sigla_uf`, `ano`) e **integração das 6 bases**.

### Gold — camada analítica
- Datasets prontos para consumo: **indicador por município**, **meta × realizado** e
  **evolução temporal** do indicador.
- Publicada em **Delta** (Databricks) e exportada para o **BigQuery** (consumo por BI/ML).

## Mapeamento para serviços GCP (produção)

| Função | Databricks (dev) | GCP (produção) |
|--------|------------------|----------------|
| Fonte batch | leitura via `basedosdados` | BigQuery (datasets públicos) |
| Ingestão streaming | Structured Streaming | Pub/Sub + Dataflow/Spark |
| Armazenamento das camadas | Volumes / Delta | Cloud Storage (Parquet particionado) |
| Camada analítica | Delta | BigQuery |
| Orquestração | Jobs Databricks | Cloud Composer (Airflow) |
| Visualização | — | Looker Studio |
