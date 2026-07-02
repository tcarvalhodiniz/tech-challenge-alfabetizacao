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
#   Os dataset_id/table_id abaixo são confirmados na etapa de ingestão.
#   Mapeiam as 6 entidades exigidas pelo enunciado.
# ---------------------------------------------------------------------------
BILLING_PROJECT_ID = os.getenv("GCP_PROJECT", "seu-projeto-gcp")

FONTES = {
    "uf":                  {"tipo": "batch", "tabela": "br_bd_diretorios_brasil.uf"},
    "municipio":           {"tipo": "batch", "tabela": "br_bd_diretorios_brasil.municipio"},
    "meta_brasil":         {"tipo": "batch", "tabela": "<dataset>.meta_alfabetizacao_brasil"},
    "meta_uf":             {"tipo": "batch", "tabela": "<dataset>.meta_alfabetizacao_uf"},
    "meta_municipio":      {"tipo": "batch", "tabela": "<dataset>.meta_alfabetizacao_municipio"},
    "alunos":             {"tipo": "batch", "tabela": "<dataset>.dados_alunos"},
    # A "medição do indicador" também chega em tempo quase-real via streaming.
    "indicador_evento":    {"tipo": "streaming", "topico": "indicador-alfabetizacao"},
}

# Parâmetro de negócio: ponto de corte de proficiência do Saeb (Alfabetiza Brasil 2023)
PONTO_CORTE_SAEB = 743

# ---------------------------------------------------------------------------
# Regras de qualidade de dados (por camada)
# ---------------------------------------------------------------------------
CHAVES_PRIMARIAS = {
    "municipio": ["id_municipio"],
    "uf": ["sigla_uf"],
    "meta_municipio": ["id_municipio", "ano"],
    "meta_uf": ["sigla_uf", "ano"],
    "alunos": ["id_aluno", "ano"],
}

# Faixas válidas para validação de consistência
LIMITES = {
    "indicador_pct": (0.0, 100.0),   # % de crianças alfabetizadas
    "ano": (2019, 2030),
    "proficiencia_saeb": (0.0, 1000.0),
}
