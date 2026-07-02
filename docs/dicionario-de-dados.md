# Dicionário de Dados

**Dataset:** `basedosdados.br_inep_avaliacao_alfabetizacao` (INEP · cobertura 2023–2024)

Fonte: [Base dos Dados — Avaliação da Alfabetização](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72).
O próprio dataset traz a tabela `dicionario`, que mapeia os códigos das colunas
categóricas (ex.: `rede`, `serie`) para suas descrições.

## Mapeamento fontes → entidades do desafio

| Entidade (enunciado) | Tabela (`table_id`) | Grão |
|----------------------|---------------------|------|
| Dados de alunos | `alunos` | aluno × ano |
| Meta Alfabetização Brasil | `meta_alfabetizacao_brasil` | ano × rede |
| Meta Alfabetização por UF | `meta_alfabetizacao_uf` | UF × ano × rede |
| Meta Alfabetização por Município | `meta_alfabetizacao_municipio` | município × ano × rede |
| Município | `municipio` | município × ano × rede × série |
| UF | `uf` | UF × ano × rede × série |
| Dicionário | `dicionario` | — |

**Chaves de integração:** `id_municipio` (7 dígitos), `sigla_uf`, `ano`, `rede`, `serie`.
**Indicador:** `taxa_alfabetizacao` (% de alunos avaliados considerados alfabetizados).
**Corte Saeb:** 743 pontos (usado a partir da `proficiencia` na tabela `alunos`).

---

## `alunos` — microdados (proficiência)

| Coluna | Descrição |
|--------|-----------|
| `id_aluno` | Código do aluno |
| `id_escola` | Máscara do código da escola (fictício) |
| `id_municipio` | ID município de 7 dígitos |
| `ano` | Ano de aplicação da avaliação estadual |
| `serie` | Ano escolar |
| `rede` | Dependência administrativa da escola |
| `caderno` | Código do caderno da prova de LP |
| `presenca` | Indicador de presença na prova |
| `preenchimento_caderno` | Indicador de preenchimento da prova |
| `peso_aluno` | Peso do aluno na prova de LP |
| `proficiencia` | Proficiência em LP na escala equalizada com o SAEB |
| `alfabetizado` | Indica se o aluno é considerado alfabetizado |

## `municipio` / `uf` — resultados por território

| Coluna | Descrição |
|--------|-----------|
| `id_municipio` / `sigla_uf` | Chave do território |
| `ano` | Ano de aplicação |
| `rede` | Rede de ensino |
| `serie` | Ano escolar |
| `media_portugues` | Média ponderada em LP equalizada com o SAEB |
| `proporcao_aluno_nivel_0..8` | % de alunos por nível de desempenho em LP |
| `taxa_alfabetizacao` | **% de alunos considerados alfabetizados (indicador)** |

## `meta_alfabetizacao_brasil` / `_uf` / `_municipio` — metas

| Coluna | Descrição |
|--------|-----------|
| `ano` | Ano da avaliação |
| `sigla_uf` / `id_municipio` | Chave do território (quando aplicável) |
| `rede` | Rede de ensino |
| `taxa_alfabetizacao` | Taxa de alfabetização realizada |
| `percentual_participacao` | % de participação no território |
| `nivel_alfabetizacao` | Nível de alfabetização (só em `_municipio`) |
| `meta_alfabetizacao_2024` … `meta_alfabetizacao_2030` | Meta de alfabetização por ano-alvo (formato **largo**) |

> As metas estão em formato largo (uma coluna por ano-alvo). Na Silver, essas colunas
> são despivotadas para o formato longo (`ano_meta`, `valor_meta`), permitindo comparar
> **meta × realizado** ano a ano na Gold.

## `dicionario` — de-para de categorias

| Coluna | Descrição |
|--------|-----------|
| `id_tabela` | Nome da tabela |
| `nome_coluna` | Nome da coluna categórica |
| `chave` | Código/identificador da categoria |
| `valor` | Descrição correspondente à chave |
| `cobertura_temporal` | Cobertura temporal |
