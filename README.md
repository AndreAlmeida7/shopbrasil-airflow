# ShopBrasil · Pipeline de preços por categoria (Apache Airflow)

Pipeline que substitui o script em cron que alimentava o painel de pricing da ShopBrasil.
Todo dia às **06:00 (horário de Brasília)** ele coleta o catálogo da API ([FakeStore](https://fakestoreapi.com/docs)),
calcula **preço médio, mínimo, máximo e quantidade de produtos por categoria** e grava o resultado no **PostgreSQL** analítico.

## Arquitetura

```
┌──────────────────────── ingestao (linear) ─────────────────────────┐
│ buscar_produtos ──► validar_produtos ──► agrupar_por_categoria     │
└────────────────────────────────────────────────┬───────────────────┘
                                                 │ fan-out (1 task por categoria, pool 2 slots)
┌──────────────────────── analise ───────────────▼───────────────────┐
│ calcular_metricas[electronics] ┐                                   │
│ calcular_metricas[jewelery]    ├──► consolidar_metricas  (fan-in)  │
│ calcular_metricas[...]         ┘                                   │
└────────────────────────────────────────────────┬───────────────────┘
┌──────────────────────── persistencia ──────────▼───────────────────┐
│ criar_tabelas ──► salvar_snapshot   (upsert, idempotente)          │
│               └─► salvar_historico  (append, evolução de preços)   │
└────────────────────────────────────────────────────────────────────┘
```

```
.
├── docker-compose.yaml                    # Airflow 2.10.5 (LocalExecutor) + Postgres metadados + Postgres analítico
├── dags/
│   └── shopbrasil_precos_por_categoria.py # a DAG (TaskFlow API)
└── plugins/shopbrasil/
    ├── config.py                          # constantes (endpoint, conn_id, pool, tabelas)
    ├── callbacks.py                       # sucesso / retry / falha / SLA (alerta simulado)
    ├── sql.py                             # DDL, upsert e insert
    └── validar_produtos_operator.py       # ValidarProdutosOperator(BaseOperator)
```

## Como rodar

Pré-requisito: Docker Desktop (ou Docker Engine + Compose v2) com pelo menos 4 GB de RAM.

```bash
git clone <url-deste-repo> && cd shopbrasil-airflow
cp .env.example .env          # opcional: sem .env os mesmos defaults são usados

docker compose up airflow-init   # migra o banco, cria o usuário admin e o pool ecommerce_pool
docker compose up -d             # sobe webserver + scheduler + bancos
```

1. Acesse **http://localhost:8080** (usuário `airflow`, senha `airflow`).
2. Despause a DAG `shopbrasil_precos_por_categoria` e clique em **Trigger DAG**.
3. Acompanhe na aba **Graph**. As tasks mapeadas aparecem com o nome da categoria.

Para derrubar tudo: `docker compose down` (ou `docker compose down -v` para apagar também os dados).

## Validando o resultado

A base analítica fica exposta em `localhost:5433` (usuário, senha e banco `shopbrasil`).
Pode usar DBeaver ou direto pelo container:

```bash
docker compose exec postgres-analytics psql -U shopbrasil -d shopbrasil
```

```sql
-- Painel do dia
SELECT * FROM metricas_categoria_snapshot ORDER BY data_referencia DESC, categoria;

-- Evolução de preços
SELECT data_referencia, categoria, preco_medio, coletado_em
FROM metricas_categoria_historico ORDER BY categoria, coletado_em;
```

**Teste de idempotência:** depois da primeira execução, vá em *Grid*, selecione a run e use **Clear** para reprocessá-la.
A contagem de `metricas_categoria_snapshot` continua igual (uma linha por categoria por dia), enquanto
`metricas_categoria_historico` ganha novas linhas, como esperado em modo append.

```sql
SELECT 'snapshot' AS tabela, count(*) FROM metricas_categoria_snapshot
UNION ALL
SELECT 'historico', count(*) FROM metricas_categoria_historico;
```

**Teste de resiliência:** em `.env`, troque `FAKESTORE_BASE_URL` por uma URL inválida (ex.: `https://fakestoreapi.invalid`),
rode `docker compose up -d` e dispare a DAG. `Buscar Produtos` entra em *up_for_retry* com espera crescente,
e os logs mostram os callbacks de retry e, ao esgotar as tentativas, o alerta simulado.

## Requisitos atendidos

| Requisito | Implementação |
|---|---|
| TaskFlow API | `@dag` / `@task`. As dependências saem das chamadas de função; o `>>` só é usado para `criar_tabelas`, que não troca dados |
| XComs pequenos | `buscar_produtos` devolve só `id`, `title`, `price` e `category`. A análise recebe `{categoria, precos[]}` |
| Linear / fan-out / fan-in | `ingestao` (linear), `calcular_metricas.expand` (fan-out) e `consolidar_metricas` (fan-in) |
| Agendamento | `schedule="0 6 * * *"`, `start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo")`, `catchup=False` |
| Retry com backoff | `Buscar Produtos`: 5 retries, 30 s iniciais, `retry_exponential_backoff=True`, teto de 10 min |
| try/except + raise | Erros de rede, HTTP ou payload viram `AirflowException`, que aciona o retry |
| Callbacks de ciclo de vida | `on_success_callback`, `on_retry_callback` e `on_failure_callback` na task crítica |
| Dynamic Task Mapping | `calcular_metricas.expand(lote=...)`. Uma categoria nova vira uma task nova sem alterar código |
| Pool | `ecommerce_pool` com 2 slots, criado no `airflow-init` e aplicado em `calcular_metricas` |
| TaskGroups | `ingestao`, `analise` e `persistencia` |
| PostgresHook + Connection | Conn `postgres_analytics`, definida por `AIRFLOW_CONN_POSTGRES_ANALYTICS` no compose |
| Gravação idempotente | PK `(data_referencia, categoria)` + `INSERT ... ON CONFLICT DO UPDATE`, numa transação única que também remove categorias que sumiram do catálogo no mesmo dia |
| **Opcional:** operador customizado | `ValidarProdutosOperator`: descarta registros fora do schema e falha **sem retry** se os inválidos passarem de 20% |
| **Opcional:** tabela de histórico | `metricas_categoria_historico` em modo append, com `run_id` e `coletado_em` |
| **Opcional:** SLA / alerta | `sla=30min` em `salvar_snapshot` + `sla_miss_callback`. O `on_failure_callback` em todas as tasks simula envio para `#pricing-alertas` |

## Decisões de design

- **`data_referencia` = fim do intervalo da run, no fuso de Brasília.** A run das 06:00 de 14/11 grava o snapshot de 14/11. É determinístico por run, então reprocessar a mesma run sempre cai na mesma chave.
- **Falha de API ≠ falha de schema.** Instabilidade de rede é transitória e vale retentar com backoff. Payload fora do contrato não se resolve sozinho: o operador falha direto (`AirflowFailException`) e dispara o alerta.
- **Categorias descobertas em tempo de execução.** Nada no código conhece as categorias. O fan-out é derivado dos dados, e o pool impede que um catálogo grande sature o ambiente.
- **Base analítica separada do metadados do Airflow**, como seria em produção.
- **Airflow 2.10.5:** versão estável da série 2.x, que ainda tem `sla` (recurso removido no Airflow 3).
