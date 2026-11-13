"""Configuração central do pipeline. Alterou ambiente? Mude aqui (ou via variável de ambiente)."""

import os

TIMEZONE = "America/Sao_Paulo"

# Fonte de dados (FakeStore representa a API de catálogo)
FAKESTORE_BASE_URL = os.getenv("FAKESTORE_BASE_URL", "https://fakestoreapi.com").rstrip("/")
PRODUTOS_ENDPOINT = f"{FAKESTORE_BASE_URL}/products"
HTTP_TIMEOUT = (5, 30)  # (connect, read) em segundos

# Airflow
POSTGRES_CONN_ID = "postgres_analytics"
POOL_ECOMMERCE = "ecommerce_pool"

# Tabelas de destino
TABELA_SNAPSHOT = "metricas_categoria_snapshot"
TABELA_HISTORICO = "metricas_categoria_historico"
