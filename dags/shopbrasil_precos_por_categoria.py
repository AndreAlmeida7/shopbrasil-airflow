"""
### ShopBrasil - Panorama de preços por categoria

Substitui o script em cron que alimentava o painel de pricing.
Roda todo dia às **06:00 (America/Sao_Paulo)**, coleta os produtos da API de catálogo
(FakeStore), calcula preço médio/mínimo/máximo e quantidade por categoria e grava no
PostgreSQL analítico.

**Topologias**
- *Linear* (ingestao): `buscar_produtos -> validar_produtos -> agrupar_por_categoria`
- *Fan-out* (analise): `calcular_metricas.expand(...)`, uma task por categoria, limitada pelo pool `ecommerce_pool`
- *Fan-in* (analise): `consolidar_metricas` junta o resultado de todas as categorias

**Persistência**
- `metricas_categoria_snapshot`: idempotente (upsert por `data_referencia` + `categoria`)
- `metricas_categoria_historico`: append, para acompanhar a evolução dos preços
"""

from __future__ import annotations

import logging
from collections import defaultdict
from contextlib import closing
from datetime import timedelta
from statistics import mean

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from shopbrasil import callbacks, sql
from shopbrasil.config import (
    HTTP_TIMEOUT,
    POOL_ECOMMERCE,
    POSTGRES_CONN_ID,
    PRODUTOS_ENDPOINT,
    TIMEZONE,
)
from shopbrasil.validar_produtos_operator import ValidarProdutosOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "time-dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # Qualquer task que falhar de vez dispara alerta
    "on_failure_callback": callbacks.ao_falhar,
}


# --------------------------------------------------------------------------- #
# Ingestão
# --------------------------------------------------------------------------- #
@task(
    task_display_name="Buscar Produtos",
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,  # 30s, ~1min, ~2min, ~4min... até o teto abaixo
    max_retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(minutes=2),
    on_success_callback=callbacks.ao_sucesso,
    on_retry_callback=callbacks.ao_retentar,
    on_failure_callback=callbacks.ao_falhar,
)
def buscar_produtos() -> list[dict]:
    """Busca o catálogo. Qualquer erro de rede/HTTP/payload vira exceção para acionar o retry."""
    try:
        resposta = requests.get(
            PRODUTOS_ENDPOINT,
            timeout=HTTP_TIMEOUT,
            headers={"Accept": "application/json", "User-Agent": "shopbrasil-airflow/1.0"},
        )
        resposta.raise_for_status()
        produtos = resposta.json()
        if not isinstance(produtos, list) or not produtos:
            raise ValueError(f"Payload inesperado da API: {type(produtos).__name__} vazio ou inválido")
    except (requests.RequestException, ValueError) as erro:
        log.error("Falha ao buscar produtos em %s: %s", PRODUTOS_ENDPOINT, erro)
        raise AirflowException(f"API de catálogo indisponível: {erro}") from erro

    log.info("%s produtos recebidos da API.", len(produtos))
    # XCom leve: só os campos que o pipeline usa
    return [
        {k: p.get(k) for k in ("id", "title", "price", "category")} if isinstance(p, dict) else p
        for p in produtos
    ]


@task
def agrupar_por_categoria(produtos: list[dict]) -> list[dict]:
    """Agrupa os preços por categoria. Cada item vira uma task mapeada no fan-out."""
    precos_por_categoria: dict[str, list[float]] = defaultdict(list)
    for produto in produtos:
        precos_por_categoria[produto["category"]].append(produto["price"])

    lotes = [{"categoria": cat, "precos": precos} for cat, precos in sorted(precos_por_categoria.items())]
    log.info("%s categorias encontradas: %s", len(lotes), [l["categoria"] for l in lotes])
    return lotes


# --------------------------------------------------------------------------- #
# Análise
# --------------------------------------------------------------------------- #
@task(pool=POOL_ECOMMERCE, map_index_template="{{ categoria }}")
def calcular_metricas(lote: dict) -> dict:
    """Métricas de uma categoria (roda em paralelo, no máximo 2 por vez pelo pool)."""
    categoria, precos = lote["categoria"], lote["precos"]
    get_current_context()["categoria"] = categoria  # rótulo da task mapeada na UI

    return {
        "categoria": categoria,
        "preco_medio": round(mean(precos), 2),
        "preco_minimo": round(min(precos), 2),
        "preco_maximo": round(max(precos), 2),
        "qtd_produtos": len(precos),
    }


@task
def consolidar_metricas(metricas: list[dict]) -> list[dict]:
    """Fan-in: junta o resultado de todas as categorias numa lista única e ordenada."""
    consolidadas = sorted(metricas, key=lambda m: m["categoria"])
    for m in consolidadas:
        log.info(
            "%-20s | qtd=%3s | médio=%9.2f | mín=%9.2f | máx=%9.2f",
            m["categoria"], m["qtd_produtos"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"],
        )
    return consolidadas


# --------------------------------------------------------------------------- #
# Persistência
# --------------------------------------------------------------------------- #
def _data_referencia(context) -> str:
    """Dia a que o painel se refere: fim do intervalo da run, no fuso de Brasília.

    É determinístico por run, então reprocessar a mesma run gera a mesma chave.
    """
    return context["data_interval_end"].in_timezone(TIMEZONE).to_date_string()


def _linhas(metricas: list[dict], context) -> list[dict]:
    data_ref = _data_referencia(context)
    return [{**m, "data_referencia": data_ref, "run_id": context["run_id"]} for m in metricas]


@task
def criar_tabelas() -> None:
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql.DDL)


@task(sla=timedelta(minutes=30))  # painel precisa estar pronto até 06:30
def salvar_snapshot(metricas: list[dict]) -> int:
    """Grava o snapshot do dia de forma idempotente (upsert + limpeza na mesma transação)."""
    context = get_current_context()
    linhas = _linhas(metricas, context)
    data_ref = linhas[0]["data_referencia"]

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with closing(hook.get_conn()) as conn:
        with conn:  # commit no sucesso, rollback em qualquer erro
            with conn.cursor() as cur:
                cur.executemany(sql.UPSERT_SNAPSHOT, linhas)
                cur.execute(
                    sql.REMOVER_CATEGORIAS_OBSOLETAS,
                    {"data_referencia": data_ref, "categorias": [l["categoria"] for l in linhas]},
                )
    log.info("Snapshot de %s gravado: %s categorias.", data_ref, len(linhas))
    return len(linhas)


@task
def salvar_historico(metricas: list[dict]) -> int:
    """Append-only: cada execução vira uma nova foto, para acompanhar a evolução dos preços."""
    linhas = _linhas(metricas, get_current_context())

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with closing(hook.get_conn()) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql.INSERT_HISTORICO, linhas)
    log.info("Histórico: %s linhas adicionadas.", len(linhas))
    return len(linhas)


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Panorama diário de preços por categoria para o time de pricing",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=callbacks.sla_perdido,
    doc_md=__doc__,
    tags=["shopbrasil", "pricing", "ecommerce"],
)
def shopbrasil_precos_por_categoria():
    with TaskGroup("ingestao", tooltip="Coleta e valida o catálogo (linear)"):
        produtos_brutos = buscar_produtos()
        produtos_validos = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos_brutos,
            retries=0,
        ).output
        lotes = agrupar_por_categoria(produtos_validos)

    with TaskGroup("analise", tooltip="Métricas por categoria (fan-out) e consolidação (fan-in)"):
        metricas = calcular_metricas.expand(lote=lotes)
        metricas_consolidadas = consolidar_metricas(metricas)

    with TaskGroup("persistencia", tooltip="Grava snapshot idempotente e histórico append"):
        tabelas = criar_tabelas()
        metricas_consolidadas >> tabelas >> [salvar_snapshot(metricas_consolidadas), salvar_historico(metricas_consolidadas)]


shopbrasil_precos_por_categoria()
