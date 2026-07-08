# Evidências de Execução

Prints que comprovam a pipeline rodando na nuvem — **Databricks Free Edition**
(PySpark + Delta Lake + Structured Streaming), com a camada **Gold no BigQuery** e o
dashboard no **Looker Studio**.

Salve cada print com o nome indicado abaixo, nesta pasta.

| Arquivo | O que o print deve mostrar |
|---------|-----------------------------|
| `01-bronze-batch.png` | Notebook `01_bronze_batch` — tabela de contagens das 7 fontes na Bronze (uf=145, municipio=23.995, meta_municipio=10.704, alunos=100.000, dicionario=27…) |
| `02-bronze-streaming.png` | Notebook `02_bronze_streaming` — validação do streaming: total de eventos (~100) e a distribuição por UF |
| `03-silver.png` | Notebook `03_silver` — validação: chaves nulas **0 / 0 / 0** e a amostra `taxa_realizada` × `meta_taxa_alfabetizacao` |
| `04-qualidade.png` | Notebook `04_quality` — relatório de qualidade (aprovados / quarentena por tabela) |
| `05-gold.png` | Notebook `05_gold` — meta × realizado por município (com `gap_pp`, `atingiu_meta`) |
| `06-bigquery.png` | Console do **BigQuery** — dataset `alfabetizacao_gold` com as 4 tabelas publicadas |
| `07-dashboard.png` | **Looker Studio** — o dashboard público do indicador |

> Não é preciso manter a infraestrutura no ar após a entrega: estes prints + o vídeo
> executivo (≤5 min) são a evidência de que a arquitetura funcionou de ponta a ponta.
