# FinOps — eficiência de custo e desempenho

Como a pipeline mantém o custo baixo e o desempenho bom em cada camada. Os números de
custo abaixo são reais, medidos com o *dry-run* do BigQuery (calcula os bytes que uma
query processaria, sem executar nem cobrar).

## 1. Formato e armazenamento

- **Delta Lake (sobre Parquet)** em todas as camadas. O Parquet é **colunar e comprimido**
  (Snappy por padrão), então uma query lê só as colunas que precisa, e não a linha inteira.
- Colunar + compressão já derrubam o volume lido em relação a um CSV/JSON equivalente.
- O Delta ainda dá transações ACID, *time travel* e *schema enforcement* — sem custo extra
  de storage relevante.

## 2. Particionamento

| Camada | Partição | Motivo |
|--------|----------|--------|
| Bronze | `data_ingestao_dt` | isola cada carga; permite reprocessar/expurgar por data |
| Silver | `ano` | a maioria das análises filtra por ano |
| Gold | `ano` / `ano_ref` | dashboards consultam um ano por vez |

O ganho é o **partition pruning**: uma consulta com `WHERE ano = 2024` lê apenas a partição
de 2024, ignorando o resto dos arquivos no disco.

## 3. Otimizações aplicadas

- **Column pruning** — como o formato é colunar, selecionar só as colunas necessárias reduz
  o scan. Medido na tabela `alunos`: `SELECT *` lê **268 MB**; lendo só
  `id_municipio, ano, rede, proficiencia, peso_aluno` cai para **131 MB** (**−51%**).
- **`LIMIT` não economiza no BigQuery** — o `SELECT * ... LIMIT 100000` que usamos em
  desenvolvimento processa os mesmos 268 MB (o `LIMIT` é aplicado *depois* do scan). Ou seja,
  ele acelera a transferência e o processamento no Databricks, mas **não** reduz o custo do BQ.
  Para cortar custo de verdade, o caminho é column pruning e partição, não `LIMIT`.
- **Delta OPTIMIZE / Z-ORDER** — em produção, compactar arquivos pequenos e ordenar por
  `id_municipio` melhora o *data skipping* nas leituras da Silver/Gold.
- **Cache/broadcast** — as tabelas de dimensão (`dicionario`, metas) são pequenas e entram como
  *broadcast join*, evitando *shuffle*.

## 4. Estimativa de custo (BigQuery on-demand)

Preço on-demand: **US$ 6,25 por TB** escaneado, com **1 TB/mês grátis**.

| Tabela | Scan (`SELECT *`) |
|--------|-------------------|
| `uf` | 0,01 MB |
| `municipio` | 1,83 MB |
| `meta_alfabetizacao_brasil` | 0,00 MB |
| `meta_alfabetizacao_uf` | 0,01 MB |
| `meta_alfabetizacao_municipio` | 1,15 MB |
| `alunos` | 268,55 MB |
| `dicionario` | 0,00 MB |
| **Total por ingestão completa** | **≈ 272 MB (0,27 GB)** |

- **Custo por execução completa da ingestão:** **US$ 0,0017** (menos de dois décimos de centavo).
- Isso é **0,027%** do 1 TB grátis mensal — daria para rodar a ingestão inteira **~3.700 vezes
  por mês** ainda dentro do free tier.
- **Storage:** as camadas somam poucas dezenas de MB (Delta comprimido); no free tier de 10 GB
  do BigQuery e nos Volumes do Databricks, o custo de armazenamento é praticamente nulo.

## 5. Boas práticas e próximos passos (escala)

O dataset é pequeno, então o custo hoje é desprezível. As alavancas abaixo é que manteriam o
custo sob controle se o volume crescesse (ex.: microdados de todos os anos e redes):

- Ingerir `alunos` com **projeção de colunas** em vez de `SELECT *` (−51% já comprovado).
- **Particionar as tabelas Gold no BigQuery** por `ano` para o dashboard escanear menos.
- Agendar **OPTIMIZE** periódico no Delta e política de **retenção/vacuum** na Bronze.
- Materializar na Gold só o necessário para o BI (o Looker paga scan por consulta).
