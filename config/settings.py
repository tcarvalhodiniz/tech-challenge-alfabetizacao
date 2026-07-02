"""
Configuração central do pipeline de alfabetização.

Um único lugar para caminhos, parâmetros de qualidade e identificadores das
fontes. Trocando AMBIENTE de "databricks" para "gcp" o pipeline aponta das
Volumes do Databricks para os buckets do GCS, sem alterar a lógica.
"""

import os

# "databricks" (dev/demo em Delta) | "gcp" (camadas em GCS/BigQuery) | "local"
AMBIENTE = os.getenv("AMBIENTE", "databricks")

# ---------------------------------------------------------------------------
# Caminhos das camadas (Arquitetura Medalhão)
# ---------------------------------------------------------------------------
_BASES = {
    "databricks": "/Volumes/workspace/default/alfabetizacao",
    "gcp": "gs://tech-challenge-alfabetizacao",   # ajustar para o bucket real
    "local": "./data",
}
BASE_PATH = _BASES[AMBIENTE]

PATHS = {
    "bronze": f"{BASE_PATH}/bronze",
    "silver": f"{BASE_PATH}/silver",
    "gold": f"{BASE_PATH}/gold",
    "quarentena": f"{BASE_PATH}/quarentena",
    "streaming_in": f"{BASE_PATH}/streaming/eventos",       # landing dos eventos
    "streaming_ckpt": f"{BASE_PATH}/streaming/_checkpoints",  # checkpoints do Spark
}

# ---------------------------------------------------------------------------
# Fontes (Base dos Dados / BigQuery)
#   Dataset: basedosdados.br_inep_avaliacao_alfabetizacao
#   As 6 entidades exigidas pelo enunciado + o dicionário de dados.
# ---------------------------------------------------------------------------
BILLING_PROJECT_ID = os.getenv("GCP_PROJECT", "seu-projeto-gcp")  # projeto GCP p/ billing
BD_PROJECT = "basedosdados"
DATASET_ID = "br_inep_avaliacao_alfabetizacao"

# table_id na Base dos Dados -> entidade do desafio
TABELAS_BATCH = {
    "uf":                          "UF (resultados por estado)",
    "municipio":                   "Município (resultados por município)",
    "meta_alfabetizacao_brasil":   "Meta Alfabetização Brasil",
    "meta_alfabetizacao_uf":       "Meta Alfabetização por UF",
    "meta_alfabetizacao_municipio":"Meta Alfabetização por Município",
    "alunos":                      "Dados de alunos (microdados)",
    "dicionario":                  "Dicionário de dados",
}

# A "medição do indicador" também chega em tempo quase-real via streaming.
TOPICO_STREAMING = "indicador-alfabetizacao"

# ---------------------------------------------------------------------------
# Semântica de negócio (usada na Silver/Gold)
# ---------------------------------------------------------------------------
# Ponto de corte de proficiência do Saeb (Alfabetiza Brasil 2023)
PONTO_CORTE_SAEB = 743
# Coluna que representa o indicador (% de alunos alfabetizados)
COLUNA_INDICADOR = "taxa_alfabetizacao"
# Metas vêm em formato largo — colunas de meta por ano-alvo
COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]

# ---------------------------------------------------------------------------
# Regras de qualidade de dados (por camada)
# ---------------------------------------------------------------------------
# chave que identifica unicamente uma linha (grão inclui rede/serie quando existem)
CHAVES_PRIMARIAS = {
    "uf":                          ["sigla_uf", "ano", "rede", "serie"],
    "municipio":                   ["id_municipio", "ano", "rede", "serie"],
    "meta_alfabetizacao_brasil":   ["ano", "rede"],
    "meta_alfabetizacao_uf":       ["sigla_uf", "ano", "rede"],
    "meta_alfabetizacao_municipio":["id_municipio", "ano", "rede"],
    "alunos":                      ["id_aluno", "ano"],
}

# Faixas válidas para validação de consistência
LIMITES = {
    "indicador_pct": (0.0, 100.0),   # % de crianças alfabetizadas
    "ano": (2019, 2030),
    "proficiencia_saeb": (0.0, 1000.0),
}
