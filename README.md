# Pipeline Híbrido para Análise da Alfabetização no Brasil
> **Pós-Graduação em AI Scientist – FIAP** · Tech Challenge – Fase 2

Pipeline de dados híbrida (**Batch + Streaming**) em nuvem que integra as fontes do
**Indicador Criança Alfabetizada**, seguindo a **Arquitetura Medalhão
(Bronze → Silver → Gold)**, com foco em qualidade, escalabilidade e eficiência de
custos (FinOps).

📊 **Dashboard (Looker Studio):** https://datastudio.google.com/reporting/98ab3664-b93d-46d5-a597-1cc1dbb26f27

---

## 1. Contexto do problema

A alfabetização na infância é um dos pilares do desenvolvimento educacional, social e
econômico do país. O **Compromisso Nacional Criança Alfabetizada** mobiliza União,
estados, Distrito Federal e municípios para garantir que toda criança esteja
alfabetizada até o final do **2º ano do ensino fundamental**.

Em 2023, a **Pesquisa Alfabetiza Brasil** (INEP) definiu o **ponto de corte de 743
pontos** na escala de proficiência do **Saeb** — nível a partir do qual uma criança é
considerada alfabetizada. Desse parâmetro nasceu o **Indicador Criança Alfabetizada**,
que expressa o percentual de estudantes que atingem esse patamar. A meta nacional é
que, **até 2030**, todas as crianças estejam alfabetizadas ao final do 2º ano.

Compreender os fatores que influenciam esse processo exige integrar diferentes fontes
de dados públicos — metas nacionais/estaduais/municipais, dados territoriais,
microdados educacionais e indicadores de desempenho — para subsidiar **políticas
públicas baseadas em evidências**.

## 2. Desafio educacional e uso do indicador

Atuando como um time de engenharia de dados de uma organização pública de análise
educacional, o objetivo é integrar as fontes do indicador de alfabetização em uma
camada analítica confiável que permita responder perguntas como:

- Qual o percentual de crianças alfabetizadas **por município** e por UF?
- Quão distante cada território está da **meta** definida (meta × realizado)?
- Como o indicador **evolui ao longo do tempo** rumo à meta de 2030?
- Onde estão as maiores **desigualdades educacionais** entre regiões?

A camada Gold alimenta dashboards, análises estatísticas e, futuramente,
modelos de Machine Learning (ver seção *Aplicação em IA*).

**Fonte de dados:** [Indicador Criança Alfabetizada – Base dos Dados](https://basedosdados.org/).

## 3. Arquitetura proposta

Arquitetura com ingestão híbrida e camadas medalhão:

| Camada | Papel | Formato |
|--------|-------|---------|
| **Bronze** | Dados brutos ingeridos das fontes, sem transformação, histórico completo + metadados de ingestão | Delta/Parquet |
| **Silver** | Limpeza, tratamento de nulos, padronização de tipos/nomes, validação de consistência, **normalização de chaves e integração das 6 bases** | Delta/Parquet |
| **Gold** | Datasets analíticos: indicador por município, metas × resultados, evolução temporal | Delta + BigQuery |

**Fontes integradas (6):** UF · Meta Alfabetização Brasil · Meta Alfabetização por UF ·
Meta Alfabetização por Município · Município · Dados de alunos.

**Ingestão híbrida:**
- **Batch** — dados históricos de metas, municípios e agregados nacionais (Base dos Dados).
- **Streaming** — eventos quase-real-time (novas medições do indicador) via produtor
  Python e Structured Streaming.

## 4. Descrição da arquitetura da solução

> _Desenvolvimento e execução do Spark: **Databricks Free Edition** (Delta Lake +
> Structured Streaming). Nuvem de destino: **GCP**._

- **Fonte:** Base dos Dados (BigQuery público), acessada via pacote `basedosdados`.
- **Bronze:** notebooks PySpark gravam os dados brutos particionados por data de ingestão.
- **Streaming:** produtor Python grava eventos (NDJSON) numa landing; o Structured
  Streaming consome incrementalmente, com checkpoint, e grava na Bronze de streaming.
  Em produção na nuvem esse transporte vira **Pub/Sub**.
- **Silver:** limpeza + regras de qualidade + join das bases por `id_municipio` / `sigla_uf` / `ano`.
- **Gold:** tabelas analíticas em Delta, exportadas para **BigQuery**.
- **Consumo:** [dashboard no **Looker Studio**](https://datastudio.google.com/reporting/98ab3664-b93d-46d5-a597-1cc1dbb26f27) sobre o BigQuery.

## 5. Arquitetura

Detalhes e mapeamento para serviços GCP em [`docs/arquitetura.md`](docs/arquitetura.md).


## 6. Fluxo de dados

**Extração** (Base dos Dados + eventos) → **Bronze** (bruto + metadados) → **Silver**
(limpeza, qualidade, normalização de chaves e integração das 6 bases; inválidos vão para
quarentena) → **Gold** (indicador por município, meta × realizado, evolução temporal) →
**Consumo** (BigQuery + Looker Studio).

Fluxo completo, com as transformações de cada camada, em
[`docs/fluxo-de-dados.md`](docs/fluxo-de-dados.md).


## 7. Tecnologias utilizadas

| Componente | Ferramenta | Justificativa |
|-----------|-----------|---------------|
| Fonte | Base dos Dados (BigQuery) | Datasets educacionais estruturados, nativos do BigQuery |
| Processamento | Apache Spark / PySpark (Databricks) | Processamento distribuído, padrão de mercado |
| Camadas | Delta Lake | Transações ACID, time travel e schema enforcement sobre o data lake |
| Streaming | Pub/Sub + Structured Streaming | Ingestão desacoplada de eventos em tempo quase-real |
| Warehouse analítico | BigQuery | Serverless, pay-per-scan — melhor eficiência de custos (FinOps) |
| Armazenamento | Parquet particionado (GCS) | Formato colunar comprimido, leitura seletiva (columnar pruning) |
| Visualização | Looker Studio | Dashboard gratuito integrado ao BigQuery |

## 8. Decisões arquiteturais (trade-offs)

- **Batch vs Streaming** — metas e cadastros mudam pouco, então entram por **batch**; as
  medições novas do indicador pedem baixa latência, então entram por **streaming**. Daí o
  desenho ser híbrido, com os dois.
- **Data Lake vs Data Warehouse** — a opção foi um **Lakehouse**: os dados brutos e tratados
  ficam em Delta no lake (flexível e barato) e a Gold vai também pro **BigQuery** (governança
  e performance pra BI).
- **Custo vs Performance** — Parquet + particionamento reduzem I/O; BigQuery cobra por
  dados lidos, então otimização de queries e partições impacta diretamente o custo.

## 9. FinOps

Delta/Parquet (colunar + comprimido) em todas as camadas, particionamento por `ano` e
projeção de colunas para ler só o necessário. O custo foi medido de verdade com o *dry-run*
do BigQuery: a **ingestão completa processa ≈ 272 MB → US$ 0,0017 por execução** (0,027% do
1 TB/mês grátis). Detalhes, otimizações e a tabela de custo por fonte em
[`docs/finops.md`](docs/finops.md).

## 10. Aplicação em IA

Como a Gold já entrega o indicador por município, a comparação meta × realizado e a
série temporal, ela funciona como uma base de *features* pronta para alguns usos:

**a) Prever quais municípios estão em risco de não bater a meta de 2030.**
Um modelo de classificação usaria como features a `taxa_realizada`
atual, o `gap_pp`, a tendência vinda de `evolucao_municipio`, a UF/região e a rede. A saída —
"no caminho" vs "em risco" — permite **priorizar política pública** e agir antes, onde é mais
necessário.

**b) Projetar a taxa de alfabetização em 2030 por município.**
Uma regressão (ou um modelo de série temporal) sobre o histórico realizado + a trajetória de
metas estima a taxa futura e mostra o esforço necessário para fechar o gap.

**c) Alertas near-real-time.**
A tabela `indicador_stream_recente`, alimentada pela ingestão de streaming, permitiria
disparar **alertas automáticos** quando uma nova medição apontasse queda relevante do
indicador — sem esperar o ciclo batch.

Em produção, esses modelos seriam treinados fora da Gold,
consumindo-a como *feature store*, e devolveriam os scores para o próprio BigQuery, fechando
o ciclo **dados → modelo → decisão**.

## 11. Estrutura do repositório

```
tech-challenge-alfabetizacao/
├── config/            # configuração central (caminhos, fontes, regras de qualidade)
├── notebooks/         # notebooks Databricks (bronze, streaming, silver, gold)
├── src/
│   ├── ingestion/        # ingestão batch (Base dos Dados) + produtor de streaming
│   ├── transformations/  # lógica de Silver e Gold
│   ├── quality/          # regras de qualidade + quarentena
│   └── export/           # publicação da Gold no BigQuery
├── docs/
│   └── evidencias/    # prints da pipeline (bronze, silver, gold, BigQuery, dashboard)
└── data/              # camadas locais para dev (não versionado)
```

📸 Prints da execução completa (Databricks + BigQuery + Looker Studio) em
[`docs/evidencias/`](docs/evidencias/).

## 12. Como executar

**Pré-requisitos**
- Projeto GCP com BigQuery habilitado e uma *service account* (papéis BigQuery User + Job
  User); a chave JSON fica fora do repositório.
- Databricks Free Edition com o Volume `workspace.default.alfabetizacao` e a chave da service
  account enviada para o Volume.

**Pipeline — notebooks Databricks, nesta ordem**
1. `01_bronze_batch` — ingestão batch das 7 fontes (Base dos Dados/BigQuery) → Bronze.
2. `02_bronze_streaming` — produtor de eventos + Structured Streaming → Bronze.
3. `03_silver` — limpeza, normalização de chaves, decodificação e integração → Silver.
4. `04_quality` — regras de qualidade + quarentena.
5. `05_gold` — datasets analíticos → Gold.


---

## 👤 Autor

**Thiago Corrêa Carvalho Diniz** — RM 371212

Pós-Graduação AI Scientist — FIAP
