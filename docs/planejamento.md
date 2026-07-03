# Planejamento do Projeto

Documento de acompanhamento do Tech Challenge – Fase 2 (pipeline de alfabetização).

## Objetivo

Pipeline híbrida (Batch + Streaming) em nuvem, arquitetura Medalhão
(Bronze → Silver → Gold), integrando as 6 fontes do Indicador Criança Alfabetizada
(Base dos Dados), com qualidade de dados e FinOps.

## Decisões tomadas

- **Nuvem de destino:** GCP (fonte no BigQuery via `basedosdados`, Gold no BigQuery,
  dashboard no Looker Studio).
- **Ambiente de desenvolvimento/execução:** Databricks Free Edition (PySpark + Delta
  Lake + Structured Streaming).
- **Camadas:** Delta Lake (Bronze/Silver/Gold), Parquet particionado.
- **Escopo:** foco nos itens obrigatórios. **Fora do escopo (opcionais):** fontes
  externas (Censo/IBGE/FUNDEB), observabilidade avançada e treino de modelo de ML.

## Fontes (6 entidades exigidas)

UF · Meta Alfabetização Brasil · Meta Alfabetização por UF · Meta Alfabetização por
Município · Município · Dados de alunos.

## Roadmap e status

| # | Etapa | Status |
|---|-------|--------|
| 1 | Estrutura do repositório + Git | ✅ Concluído |
| 2 | Diagrama da arquitetura + fluxo de dados | ✅ Concluído |
| 3 | Ingestão Batch → Bronze (Base dos Dados) | ⬜ A fazer |
| 4 | Ingestão Streaming → Bronze (produtor + Structured Streaming) | ⬜ A fazer |
| 5 | Camada Silver (limpeza + integração das bases) | ⬜ A fazer |
| 6 | Regras de qualidade + quarentena | ⬜ A fazer |
| 7 | Camada Gold + export BigQuery | ⬜ A fazer |
| 8 | FinOps (particionamento + estimativa de custo) | ⬜ A fazer |
| 9 | README final + seção Aplicação em IA | ⬜ A fazer |
| 10 | Dashboard (Looker Studio) + roteiro do vídeo | ⬜ A fazer |

## Fluxo de trabalho (Git)

- `main` estável (recebe PRs) · `develop` integração · `feature/*` por etapa.
- Commits incrementais, um por avanço real de cada camada.
