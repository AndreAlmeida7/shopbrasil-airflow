"""SQL do pipeline. DDL é idempotente (IF NOT EXISTS) para a task de criação poder rodar sempre."""

from shopbrasil.config import TABELA_HISTORICO, TABELA_SNAPSHOT

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABELA_SNAPSHOT} (
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_{TABELA_SNAPSHOT} PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS {TABELA_HISTORICO} (
    id              BIGSERIAL     PRIMARY KEY,
    data_referencia DATE          NOT NULL,
    categoria       VARCHAR(100)  NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    coletado_em     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_{TABELA_HISTORICO}_categoria_data
    ON {TABELA_HISTORICO} (categoria, data_referencia);
"""

# Upsert: a PK (data_referencia, categoria) garante 1 linha por categoria por dia.
# Re-rodar a mesma run sobrescreve os valores em vez de duplicar.
UPSERT_SNAPSHOT = f"""
INSERT INTO {TABELA_SNAPSHOT}
    (data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos, run_id)
VALUES
    (%(data_referencia)s, %(categoria)s, %(preco_medio)s, %(preco_minimo)s,
     %(preco_maximo)s, %(qtd_produtos)s, %(run_id)s)
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    qtd_produtos  = EXCLUDED.qtd_produtos,
    run_id        = EXCLUDED.run_id,
    atualizado_em = now();
"""

# Remove categorias que sumiram do catálogo num reprocessamento do mesmo dia,
# para o snapshot refletir exatamente a última execução.
REMOVER_CATEGORIAS_OBSOLETAS = f"""
DELETE FROM {TABELA_SNAPSHOT}
WHERE data_referencia = %(data_referencia)s
  AND NOT (categoria = ANY(%(categorias)s));
"""

INSERT_HISTORICO = f"""
INSERT INTO {TABELA_HISTORICO}
    (data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos, run_id)
VALUES
    (%(data_referencia)s, %(categoria)s, %(preco_medio)s, %(preco_minimo)s,
     %(preco_maximo)s, %(qtd_produtos)s, %(run_id)s);
"""
