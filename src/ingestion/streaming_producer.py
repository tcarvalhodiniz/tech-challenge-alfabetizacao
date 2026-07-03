"""
Produtor de eventos de streaming → landing da Bronze.

Simula a chegada, em tempo quase-real, de medições do indicador de alfabetização
(uma `taxa_alfabetizacao` reportada por município). Cada lote de eventos é gravado
como um arquivo JSON (NDJSON) na pasta de landing; o job de Structured Streaming
(`streaming_bronze.py`) consome esses arquivos incrementalmente.

É Python puro (sem Spark) de propósito, pra rodar como um "produtor" à parte —
inclusive em paralelo ao consumidor, como uma fila/tópico de verdade faria.
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime

# Amostra de municípios reais (código IBGE de 7 dígitos + UF) para dar realismo aos
# eventos. Em produção, essa medição chegaria de um tópico Pub/Sub / fila de eventos.
MUNICIPIOS_AMOSTRA = [
    ("3550308", "SP"),  # São Paulo
    ("3304557", "RJ"),  # Rio de Janeiro
    ("3106200", "MG"),  # Belo Horizonte
    ("2927408", "BA"),  # Salvador
    ("2304400", "CE"),  # Fortaleza
    ("4106902", "PR"),  # Curitiba
    ("2611606", "PE"),  # Recife
    ("4314902", "RS"),  # Porto Alegre
    ("1302603", "AM"),  # Manaus
    ("5300108", "DF"),  # Brasília
    ("5208707", "GO"),  # Goiânia
    ("2111300", "MA"),  # São Luís
]

REDES = ["estadual", "municipal", "privada"]
SERIES = ["2"]  # 2º ano do Ensino Fundamental (foco do Alfabetiza Brasil)


def gerar_evento(ano: int) -> dict:
    """Cria um evento sintético de medição do indicador para um município."""
    id_municipio, sigla_uf = random.choice(MUNICIPIOS_AMOSTRA)
    return {
        "evento_id": str(uuid.uuid4()),
        "id_municipio": id_municipio,
        "sigla_uf": sigla_uf,
        "ano": ano,
        "rede": random.choice(REDES),
        "serie": random.choice(SERIES),
        # taxa plausível (%) com uma casa decimal
        "taxa_alfabetizacao": round(random.uniform(45.0, 95.0), 1),
        "timestamp_evento": datetime.now().isoformat(timespec="seconds"),
    }


def escrever_lote(eventos: list[dict], destino_dir: str) -> str:
    """Grava um lote de eventos como um arquivo NDJSON na pasta de landing."""
    os.makedirs(destino_dir, exist_ok=True)
    nome = f"eventos_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
    caminho = os.path.join(destino_dir, nome)
    with open(caminho, "w", encoding="utf-8") as f:
        for ev in eventos:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    return caminho


def produzir_eventos(
    destino_dir: str,
    n_lotes: int = 5,
    eventos_por_lote: int = 20,
    intervalo_seg: float = 2.0,
    ano: int = 2024,
) -> list[str]:
    """
    Produz `n_lotes` arquivos, cada um com `eventos_por_lote` eventos, aguardando
    `intervalo_seg` entre lotes para simular a chegada gradual (streaming).

    Retorna a lista de caminhos dos arquivos gerados.
    """
    arquivos: list[str] = []
    for i in range(n_lotes):
        lote = [gerar_evento(ano) for _ in range(eventos_por_lote)]
        caminho = escrever_lote(lote, destino_dir)
        arquivos.append(caminho)
        print(f"[produtor] lote {i + 1}/{n_lotes} → {len(lote)} eventos → {caminho}")
        if i < n_lotes - 1:
            time.sleep(intervalo_seg)
    return arquivos
