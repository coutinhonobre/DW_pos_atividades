"""Provisiona o Metabase via API: usuário admin, conexão com o banco `dw` e
três dashboards nativos, um por data mart do projeto — "Painel Vendas"
(fato_vendas), "Painel Compras" (fato_compras) e "Painel Logística -
Northwind" (fato_entregas) — cada um na sua própria coleção, com filtros de
dashboard encadeados via variáveis SQL nativas.

Roda uma vez (serviço `metabase-setup` do docker-compose, sem restart) e é
idempotente: cada dashboard só é recriado se ainda não existir (checagem por
nome, independente entre os três). Os números só aparecem depois que a DAG
`carga_inicial_dw` (ou `carga_incremental_dw`) for disparada — antes disso os
cards ficam vazios.
"""
from __future__ import annotations

import os
import sys
import time

import requests

MB_URL = "http://metabase:3000"
# Lidos do ambiente (ver metabase-setup no docker-compose.yml), com o mesmo
# valor de antes como default — mesmo padrão de credenciais simples
# (POSTGRES_PASSWORD, MYSQL_ROOT_PASSWORD) usado no resto do projeto.
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@metabase.com")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "Metabase123!")
# Usuário padrão (não-admin), só visualiza os painéis.
STANDARD_EMAIL = os.environ.get("MB_STANDARD_EMAIL", "metabase@metabase.com")
STANDARD_PASSWORD = os.environ.get("MB_STANDARD_PASSWORD", "metabase")
DB_NAME = "DW - Northwind/Mercearia"

VENDAS_COLLECTION_NAME = "Vendas - Northwind/Mercearia"
VENDAS_DASHBOARD_NAME = "Painel Vendas"
COMPRAS_COLLECTION_NAME = "Compras - Mercearia"
COMPRAS_DASHBOARD_NAME = "Painel Compras"
COLLECTION_NAME = "Logística - Northwind"
DASHBOARD_NAME = "Painel Logística - Northwind"

# dim_tempos.nome_mes vem de TO_CHAR(..., 'TMMonth'), que depende do locale do
# Postgres do container (normalmente en_US) - traduzido aqui a partir do
# número do mês para não depender de locale.
MES_CASE = """CASE dt.mes
        WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
    END"""

# ----------------------------------------------------------------
# Filtros do dashboard (Período + Transportadora): cada um é uma variável SQL
# nativa opcional ([[ ... ]] some se o parâmetro não for preenchido) mais um
# parâmetro no nível do dashboard, ligados um ao outro em build_dashboard()
# via parameter_mappings.
# ----------------------------------------------------------------

FILTER_PERIODO_SQL = "[[AND dt.data >= {{data_inicio}}]] [[AND dt.data <= {{data_fim}}]]"
FILTER_TRANSP_SQL = "[[AND tr.nome_transportadora = {{transportadora}}]]"
FILTER_SISTEMA_SQL = "[[AND fv.sistema_origem = {{sistema_origem}}]]"
FILTER_FORNECEDOR_SQL = "[[AND fo.nome_fornecedor = {{fornecedor}}]]"

TAG_DEFS = {
    "data_inicio": {"id": "tag-data-inicio", "name": "data_inicio", "display-name": "Data inicial", "type": "date"},
    "data_fim": {"id": "tag-data-fim", "name": "data_fim", "display-name": "Data final", "type": "date"},
    "transportadora": {"id": "tag-transportadora", "name": "transportadora", "display-name": "Transportadora", "type": "text"},
    "sistema_origem": {"id": "tag-sistema-origem", "name": "sistema_origem", "display-name": "Sistema de origem", "type": "text"},
    "fornecedor": {"id": "tag-fornecedor", "name": "fornecedor", "display-name": "Fornecedor", "type": "text"},
}

PARAM_DEFS = {
    "data_inicio": {"id": "param-data-inicio", "name": "Data inicial", "slug": "data_inicio", "type": "date/single"},
    "data_fim": {"id": "param-data-fim", "name": "Data final", "slug": "data_fim", "type": "date/single"},
    "transportadora": {"id": "param-transportadora", "name": "Transportadora", "slug": "transportadora", "type": "string/="},
    "sistema_origem": {"id": "param-sistema-origem", "name": "Sistema de origem", "slug": "sistema_origem", "type": "string/="},
    "fornecedor": {"id": "param-fornecedor", "name": "Fornecedor", "slug": "fornecedor", "type": "string/="},
}

# Ranking/comparação por transportadora (ou sistema/fornecedor) fica de fora
# do respectivo filtro de propósito - filtrar por um único valor colapsaria
# o gráfico de comparação a 1 barra, o que não agrega nada.
FILTERS_PERIODO = ["data_inicio", "data_fim"]
FILTERS_PERIODO_TRANSP = ["data_inicio", "data_fim", "transportadora"]
FILTERS_PERIODO_SISTEMA = ["data_inicio", "data_fim", "sistema_origem"]
FILTERS_PERIODO_FORNECEDOR = ["data_inicio", "data_fim", "fornecedor"]


def tags_for(keys):
    return {k: TAG_DEFS[k] for k in keys}


def wait_ready():
    print("Aguardando Metabase ficar pronto...", flush=True)
    for _ in range(120):
        try:
            r = requests.get(f"{MB_URL}/api/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise SystemExit("Metabase não ficou pronto a tempo")


def get_session() -> str:
    """Faz o setup inicial (1a vez) ou login (reexecuções) e devolve o
    token de sessão."""
    props = requests.get(f"{MB_URL}/api/session/properties", timeout=10).json()
    setup_token = props.get("setup-token")

    if setup_token:
        print("Rodando setup inicial do Metabase...", flush=True)
        r = requests.post(f"{MB_URL}/api/setup", json={
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "DW",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
            "prefs": {
                "site_name": "DW Analytics",
                "site_locale": "pt-BR",
                "allow_tracking": False,
            },
        }, timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    print("Setup já realizado antes, autenticando...", flush=True)
    r = requests.post(f"{MB_URL}/api/session", json={
        "username": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def get_or_create_standard_user(session: "MB"):
    """Cria o usuário padrão (grupo 'All Users', sem privilégio de admin) se
    ainda não existir."""
    users = session.get("/api/user")["data"]
    if any(u["email"] == STANDARD_EMAIL for u in users):
        print(f"Usuário padrão '{STANDARD_EMAIL}' já existe.", flush=True)
        return
    print(f"Criando usuário padrão '{STANDARD_EMAIL}'...", flush=True)
    session.post("/api/user", json={
        "first_name": "Analista",
        "last_name": "DW",
        "email": STANDARD_EMAIL,
        "password": STANDARD_PASSWORD,
    })


def get_or_create_collection(session: "MB", name: str, description: str) -> int:
    collections = session.get("/api/collection")
    for c in collections:
        if c["name"] == name:
            return c["id"]
    c = session.post("/api/collection", json={
        "name": name,
        "description": description,
        "color": "#509EE3",
    })
    return c["id"]


class MB:
    def __init__(self, base_url: str, session_token: str):
        self.base_url = base_url
        self.headers = {"X-Metabase-Session": session_token, "Content-Type": "application/json"}

    def get(self, path):
        r = requests.get(f"{self.base_url}{path}", headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path, json):
        r = requests.post(f"{self.base_url}{path}", headers=self.headers, json=json, timeout=60)
        r.raise_for_status()
        return r.json()

    def put(self, path, json):
        r = requests.put(f"{self.base_url}{path}", headers=self.headers, json=json, timeout=60)
        r.raise_for_status()
        if r.text:
            return r.json()
        return None


# Cubo OLAP (crosstab): o display nativo "pivot" do Metabase depende de
# reescrever a query em múltiplas combinações de agrupamento (grouping
# sets) — funciona pra perguntas MBQL, mas quebra em pergunta nativa (SQL):
# testado ao vivo, devolve linhas vazias ([0]) em vez dos dados. Em vez
# disso, o crosstab é montado na própria query, com agregação condicional
# (FILTER) — a técnica padrão de PIVOT manual em bancos sem operador PIVOT
# nativo (é o caso do Postgres). O "cubo" fica explícito no formato da
# linha (categoria/transportadora x trimestre), sem depender de nenhum
# recurso client-side.
QUARTER_FILTER_COLS = ", ".join(
    f'sum({{measure}}) FILTER (WHERE dt.trimestre = {q}) AS "T{q}"' for q in (1, 2, 3, 4)
)


def make_card(mb: MB, database_id: int, collection_id: int, name: str, sql: str, display: str, viz: dict, filters: list) -> dict:
    native = {"query": sql}
    if filters:
        native["template-tags"] = tags_for(filters)
    card = mb.post("/api/card", json={
        "name": name,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "native": native,
            "database": database_id,
        },
        "display": display,
        "visualization_settings": viz,
    })
    return {"id": card["id"], "filters": filters}


def build_cards_entregas(mb: MB, database_id: int, collection_id: int) -> dict:
    cards = {}

    cards["total_pedidos"] = make_card(
        mb, database_id, collection_id, "Total de Pedidos",
        f"""
        SELECT count(*) AS pedidos
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_TRANSP,
    )
    cards["valor_total_frete"] = make_card(
        mb, database_id, collection_id, "Frete Total",
        f"""
        SELECT sum(fe.valor_frete) AS frete
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_TRANSP,
    )
    cards["prazo_medio_envio"] = make_card(
        mb, database_id, collection_id, "Prazo Médio de Envio (dias)",
        f"""
        SELECT round(avg(fe.dias_para_envio), 1) AS dias
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE fe.dias_para_envio IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_TRANSP,
    )
    cards["atraso_medio"] = make_card(
        mb, database_id, collection_id, "Atraso Médio vs. Prometido (dias)",
        f"""
        SELECT round(avg(fe.atraso_dias), 1) AS dias
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE fe.atraso_dias IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_TRANSP,
    )
    cards["pct_no_prazo"] = make_card(
        mb, database_id, collection_id, "% Entregue no Prazo Prometido",
        f"""
        SELECT round(100.0 * sum(CASE WHEN fe.atraso_dias <= 0 THEN 1 ELSE 0 END) / NULLIF(count(fe.atraso_dias), 0), 2) AS pct
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE fe.atraso_dias IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_TRANSP,
    )

    cards["atraso_por_transportadora"] = make_card(
        mb, database_id, collection_id, "Atraso Médio por Transportadora",
        f"""
        SELECT tr.nome_transportadora AS transportadora, round(avg(fe.atraso_dias), 1) AS atraso_medio
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE fe.atraso_dias IS NOT NULL {FILTER_PERIODO_SQL}
        GROUP BY tr.nome_transportadora
        ORDER BY atraso_medio DESC
        """,
        "bar", {"graph.dimensions": ["transportadora"], "graph.metrics": ["atraso_medio"]}, FILTERS_PERIODO,
    )
    cards["frete_por_transportadora"] = make_card(
        mb, database_id, collection_id, "Frete Médio por Transportadora",
        f"""
        SELECT tr.nome_transportadora AS transportadora, round(avg(fe.valor_frete), 2) AS frete_medio
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY tr.nome_transportadora
        ORDER BY frete_medio DESC
        """,
        "bar", {"graph.dimensions": ["transportadora"], "graph.metrics": ["frete_medio"]}, FILTERS_PERIODO,
    )

    cards["pedidos_por_mes"] = make_card(
        mb, database_id, collection_id, "Pedidos por Mês",
        f"""
        SELECT {MES_CASE} AS mes, count(*) AS pedidos
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        GROUP BY dt.mes
        ORDER BY dt.mes
        """,
        "line", {
            "graph.dimensions": ["mes"], "graph.metrics": ["pedidos"],
            "graph.x_axis.scale": "ordinal",
        }, FILTERS_PERIODO_TRANSP,
    )

    cards["top_funcionarios_valor"] = make_card(
        mb, database_id, collection_id, "Top 10 Funcionários por Valor Despachado",
        f"""
        SELECT fu.nome_funcionario AS funcionario, sum(fe.valor_pedido) AS valor_total
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        LEFT JOIN dim_funcionarios fu ON fu.id_dim_funcionario = fe.id_dim_funcionario
        LEFT JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE fu.nome_funcionario <> 'Não aplicável' {FILTER_PERIODO_SQL} {FILTER_TRANSP_SQL}
        GROUP BY fu.nome_funcionario
        ORDER BY valor_total DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["funcionario"], "graph.metrics": ["valor_total"]}, FILTERS_PERIODO_TRANSP,
    )

    cards["resumo_transportadora"] = make_card(
        mb, database_id, collection_id, "Resumo por Transportadora",
        f"""
        SELECT
            tr.nome_transportadora AS transportadora,
            count(*) AS pedidos,
            sum(fe.valor_pedido) AS valor_total,
            round(avg(fe.valor_frete), 2) AS frete_medio,
            round(avg(fe.dias_para_envio), 1) AS prazo_medio_envio,
            round(avg(fe.atraso_dias), 1) AS atraso_medio
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY tr.nome_transportadora
        ORDER BY pedidos DESC
        """,
        "table", {}, FILTERS_PERIODO,
    )

    cards["cubo_transportadora_periodo"] = make_card(
        mb, database_id, collection_id, "Cubo OLAP: Frete por Transportadora x Trimestre",
        f"""
        SELECT
            tr.nome_transportadora AS transportadora,
            {QUARTER_FILTER_COLS.format(measure='fe.valor_frete')},
            sum(fe.valor_frete) AS total
        FROM fato_entregas fe
        JOIN dim_tempos dt ON dt.id_dim_tempo = fe.id_dim_tempo_pedido
        JOIN dim_transportadoras tr ON tr.id_dim_transportadora = fe.id_dim_transportadora
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY tr.nome_transportadora
        ORDER BY total DESC
        """,
        "table", {}, FILTERS_PERIODO,
    )

    return cards


def dashcard(id_, card, row, col, size_x, size_y):
    entry = {"id": id_, "card_id": card["id"], "row": row, "col": col, "size_x": size_x, "size_y": size_y}
    if card["filters"]:
        entry["parameter_mappings"] = [
            {
                "parameter_id": PARAM_DEFS[f]["id"],
                "card_id": card["id"],
                "target": ["variable", ["template-tag", f]],
            }
            for f in card["filters"]
        ]
    return entry


def build_dashboard_entregas(mb: MB, collection_id: int, cards: dict) -> int:
    dash = mb.post("/api/dashboard", json={
        "name": DASHBOARD_NAME,
        "collection_id": collection_id,
        "description": (
            "Painel operacional do Mart Logística/Entregas (Northwind) - prazo, "
            "atraso e frete por transportadora e por vendedor. Filtros: "
            "Período e Transportadora, no topo do painel."
        ),
    })
    dash_id = dash["id"]

    # grade de 24 colunas (padrão do Metabase)
    dashcards = [
        # linha 0: KPIs
        dashcard(-1, cards["total_pedidos"], 0, 0, 5, 3),
        dashcard(-2, cards["valor_total_frete"], 0, 5, 5, 3),
        dashcard(-3, cards["prazo_medio_envio"], 0, 10, 5, 3),
        dashcard(-4, cards["atraso_medio"], 0, 15, 5, 3),
        dashcard(-5, cards["pct_no_prazo"], 0, 20, 4, 3),
        # linha 1: transportadora
        dashcard(-6, cards["atraso_por_transportadora"], 3, 0, 12, 8),
        dashcard(-7, cards["frete_por_transportadora"], 3, 12, 12, 8),
        # linha 2: temporal + vendedores
        dashcard(-8, cards["pedidos_por_mes"], 11, 0, 12, 7),
        dashcard(-9, cards["top_funcionarios_valor"], 11, 12, 12, 7),
        # linha 3: tabela detalhada
        dashcard(-10, cards["resumo_transportadora"], 18, 0, 24, 8),
        # linha 4: cubo OLAP (pivot table, 2 dimensões x 1 medida)
        dashcard(-11, cards["cubo_transportadora_periodo"], 26, 0, 24, 8),
    ]
    mb.put(f"/api/dashboard/{dash_id}", json={
        "dashcards": dashcards,
        "parameters": [PARAM_DEFS[f] for f in FILTERS_PERIODO_TRANSP],
    })
    return dash_id


def build_cards_vendas(mb: MB, database_id: int, collection_id: int) -> dict:
    cards = {}

    cards["total_vendido"] = make_card(
        mb, database_id, collection_id, "Total Vendido",
        f"""
        SELECT sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_SISTEMA,
    )
    cards["qtd_itens_vendidos"] = make_card(
        mb, database_id, collection_id, "Itens Vendidos",
        f"""
        SELECT sum(fv.quantidade) AS itens
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_SISTEMA,
    )
    cards["num_vendas"] = make_card(
        mb, database_id, collection_id, "Número de Vendas",
        f"""
        SELECT count(DISTINCT fv.sistema_origem || '-' || fv.id_venda_original) AS vendas
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_SISTEMA,
    )
    cards["ticket_medio"] = make_card(
        mb, database_id, collection_id, "Ticket Médio",
        f"""
        SELECT round(
            sum(fv.valor_total_item) / NULLIF(count(DISTINCT fv.sistema_origem || '-' || fv.id_venda_original), 0), 2
        ) AS ticket_medio
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_SISTEMA,
    )

    cards["vendas_por_sistema"] = make_card(
        mb, database_id, collection_id, "Vendas por Sistema de Origem",
        f"""
        SELECT fv.sistema_origem AS sistema, sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY fv.sistema_origem
        """,
        "pie", {"pie.dimension": "sistema", "pie.metric": "valor"}, FILTERS_PERIODO,
    )
    cards["top10_produtos"] = make_card(
        mb, database_id, collection_id, "Top 10 Produtos por Valor Vendido",
        f"""
        SELECT dp.nome_produto AS produto, sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        JOIN dim_produtos dp ON dp.id_dim_produto = fv.id_dim_produto
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        GROUP BY dp.nome_produto
        ORDER BY valor DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["produto"], "graph.metrics": ["valor"]}, FILTERS_PERIODO_SISTEMA,
    )
    cards["vendas_por_mes"] = make_card(
        mb, database_id, collection_id, "Vendas por Mês",
        f"""
        SELECT {MES_CASE} AS mes, sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        GROUP BY dt.mes
        ORDER BY dt.mes
        """,
        "line", {
            "graph.dimensions": ["mes"], "graph.metrics": ["valor"],
            "graph.x_axis.scale": "ordinal",
        }, FILTERS_PERIODO_SISTEMA,
    )
    cards["top10_clientes"] = make_card(
        mb, database_id, collection_id, "Top 10 Clientes por Valor Comprado",
        f"""
        SELECT cl.nome_cliente AS cliente, sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        JOIN dim_clientes cl ON cl.id_dim_cliente = fv.id_dim_cliente
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        GROUP BY cl.nome_cliente
        ORDER BY valor DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["cliente"], "graph.metrics": ["valor"]}, FILTERS_PERIODO_SISTEMA,
    )
    cards["vendas_por_categoria"] = make_card(
        mb, database_id, collection_id, "Vendas por Categoria de Produto",
        f"""
        SELECT dp.nome_categoria AS categoria, sum(fv.valor_total_item) AS valor
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        JOIN dim_produtos dp ON dp.id_dim_produto = fv.id_dim_produto
        WHERE dp.nome_categoria IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        GROUP BY dp.nome_categoria
        ORDER BY valor DESC
        """,
        "bar", {"graph.dimensions": ["categoria"], "graph.metrics": ["valor"]}, FILTERS_PERIODO_SISTEMA,
    )
    cards["ticket_por_faixa_renda"] = make_card(
        mb, database_id, collection_id, "Ticket Médio por Faixa de Renda",
        f"""
        SELECT cl.faixa_renda AS faixa,
               round(sum(fv.valor_total_item) / NULLIF(count(DISTINCT fv.sistema_origem || '-' || fv.id_venda_original), 0), 2) AS ticket_medio
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        JOIN dim_clientes cl ON cl.id_dim_cliente = fv.id_dim_cliente
        WHERE cl.faixa_renda IS NOT NULL {FILTER_PERIODO_SQL}
        GROUP BY cl.faixa_renda
        ORDER BY ticket_medio DESC
        """,
        "bar", {"graph.dimensions": ["faixa"], "graph.metrics": ["ticket_medio"]}, FILTERS_PERIODO,
    )

    cards["cubo_categoria_periodo"] = make_card(
        mb, database_id, collection_id, "Cubo OLAP: Valor Vendido por Categoria x Trimestre",
        f"""
        SELECT
            dp.nome_categoria AS categoria,
            {QUARTER_FILTER_COLS.format(measure='fv.valor_total_item')},
            sum(fv.valor_total_item) AS total
        FROM fato_vendas fv
        JOIN dim_tempos dt ON dt.id_dim_tempo = fv.id_dim_tempo
        JOIN dim_produtos dp ON dp.id_dim_produto = fv.id_dim_produto
        WHERE dp.nome_categoria IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_SISTEMA_SQL}
        GROUP BY dp.nome_categoria
        ORDER BY total DESC
        """,
        "table", {}, FILTERS_PERIODO_SISTEMA,
    )

    return cards


def build_dashboard_vendas(mb: MB, collection_id: int, cards: dict) -> int:
    dash = mb.post("/api/dashboard", json={
        "name": VENDAS_DASHBOARD_NAME,
        "collection_id": collection_id,
        "description": (
            "Painel do Mart Vendas (fato_vendas, grão de item vendido) - receita, mix de "
            "produtos e desempenho comercial, cobrindo Mercearia e Northwind. Filtros: "
            "Período e Sistema de origem, no topo do painel."
        ),
    })
    dash_id = dash["id"]

    dashcards = [
        # linha 0: KPIs
        dashcard(-1, cards["total_vendido"], 0, 0, 6, 3),
        dashcard(-2, cards["qtd_itens_vendidos"], 0, 6, 6, 3),
        dashcard(-3, cards["num_vendas"], 0, 12, 6, 3),
        dashcard(-4, cards["ticket_medio"], 0, 18, 6, 3),
        # linha 1: sistema + top produtos
        dashcard(-5, cards["vendas_por_sistema"], 3, 0, 8, 8),
        dashcard(-6, cards["top10_produtos"], 3, 8, 16, 8),
        # linha 2: temporal + clientes
        dashcard(-7, cards["vendas_por_mes"], 11, 0, 12, 7),
        dashcard(-8, cards["top10_clientes"], 11, 12, 12, 7),
        # linha 3: categoria + faixa de renda
        dashcard(-9, cards["vendas_por_categoria"], 18, 0, 12, 7),
        dashcard(-10, cards["ticket_por_faixa_renda"], 18, 12, 12, 7),
        # linha 4: cubo OLAP (pivot table, 2 dimensões x 1 medida)
        dashcard(-11, cards["cubo_categoria_periodo"], 25, 0, 24, 8),
    ]
    mb.put(f"/api/dashboard/{dash_id}", json={
        "dashcards": dashcards,
        "parameters": [PARAM_DEFS[f] for f in FILTERS_PERIODO_SISTEMA],
    })
    return dash_id


def build_cards_compras(mb: MB, database_id: int, collection_id: int) -> dict:
    cards = {}

    cards["total_comprado"] = make_card(
        mb, database_id, collection_id, "Total Comprado",
        f"""
        SELECT sum(fc.valor_total_item) AS valor
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_FORNECEDOR,
    )
    cards["qtd_itens_comprados"] = make_card(
        mb, database_id, collection_id, "Itens Comprados",
        f"""
        SELECT sum(fc.quantidade) AS itens
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_FORNECEDOR,
    )
    cards["lead_time_medio"] = make_card(
        mb, database_id, collection_id, "Lead Time Médio (dias)",
        f"""
        SELECT round(avg(fc.lead_time_dias), 1) AS dias
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE fc.lead_time_dias IS NOT NULL {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_FORNECEDOR,
    )
    cards["num_compras"] = make_card(
        mb, database_id, collection_id, "Número de Compras",
        f"""
        SELECT count(DISTINCT fc.id_compra_original) AS compras
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        """,
        "scalar", {}, FILTERS_PERIODO_FORNECEDOR,
    )

    cards["top10_fornecedores_valor"] = make_card(
        mb, database_id, collection_id, "Top 10 Fornecedores por Valor Comprado",
        f"""
        SELECT fo.nome_fornecedor AS fornecedor, sum(fc.valor_total_item) AS valor
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY fo.nome_fornecedor
        ORDER BY valor DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["fornecedor"], "graph.metrics": ["valor"]}, FILTERS_PERIODO,
    )
    cards["lead_time_por_fornecedor"] = make_card(
        mb, database_id, collection_id, "Lead Time Médio por Fornecedor",
        f"""
        SELECT fo.nome_fornecedor AS fornecedor, round(avg(fc.lead_time_dias), 1) AS lead_time
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE fc.lead_time_dias IS NOT NULL {FILTER_PERIODO_SQL}
        GROUP BY fo.nome_fornecedor
        ORDER BY lead_time DESC
        """,
        "bar", {"graph.dimensions": ["fornecedor"], "graph.metrics": ["lead_time"]}, FILTERS_PERIODO,
    )
    cards["compras_por_mes"] = make_card(
        mb, database_id, collection_id, "Compras por Mês",
        f"""
        SELECT {MES_CASE} AS mes, sum(fc.valor_total_item) AS valor
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        GROUP BY dt.mes
        ORDER BY dt.mes
        """,
        "line", {
            "graph.dimensions": ["mes"], "graph.metrics": ["valor"],
            "graph.x_axis.scale": "ordinal",
        }, FILTERS_PERIODO_FORNECEDOR,
    )
    cards["top10_produtos_comprados"] = make_card(
        mb, database_id, collection_id, "Top 10 Produtos por Quantidade Comprada",
        f"""
        SELECT dp.nome_produto AS produto, sum(fc.quantidade) AS quantidade
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        JOIN dim_produtos dp ON dp.id_dim_produto = fc.id_dim_produto
        LEFT JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL} {FILTER_FORNECEDOR_SQL}
        GROUP BY dp.nome_produto
        ORDER BY quantidade DESC
        LIMIT 10
        """,
        "row", {"graph.dimensions": ["produto"], "graph.metrics": ["quantidade"]}, FILTERS_PERIODO_FORNECEDOR,
    )
    cards["resumo_fornecedor"] = make_card(
        mb, database_id, collection_id, "Resumo por Fornecedor",
        f"""
        SELECT
            fo.nome_fornecedor AS fornecedor,
            count(DISTINCT fc.id_compra_original) AS compras,
            sum(fc.valor_total_item) AS valor_total,
            round(avg(fc.lead_time_dias), 1) AS lead_time_medio
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        JOIN dim_fornecedores fo ON fo.id_dim_fornecedor = fc.id_dim_fornecedor
        WHERE 1=1 {FILTER_PERIODO_SQL}
        GROUP BY fo.nome_fornecedor
        ORDER BY valor_total DESC
        """,
        "table", {}, FILTERS_PERIODO,
    )

    cards["cubo_categoria_periodo"] = make_card(
        mb, database_id, collection_id, "Cubo OLAP: Valor Comprado por Categoria x Trimestre",
        f"""
        SELECT
            dp.nome_categoria AS categoria,
            {QUARTER_FILTER_COLS.format(measure='fc.valor_total_item')},
            sum(fc.valor_total_item) AS total
        FROM fato_compras fc
        JOIN dim_tempos dt ON dt.id_dim_tempo = fc.id_dim_tempo
        JOIN dim_produtos dp ON dp.id_dim_produto = fc.id_dim_produto
        WHERE dp.nome_categoria IS NOT NULL {FILTER_PERIODO_SQL}
        GROUP BY dp.nome_categoria
        ORDER BY total DESC
        """,
        "table", {}, FILTERS_PERIODO,
    )

    return cards


def build_dashboard_compras(mb: MB, collection_id: int, cards: dict) -> int:
    dash = mb.post("/api/dashboard", json={
        "name": COMPRAS_DASHBOARD_NAME,
        "collection_id": collection_id,
        "description": (
            "Painel do Mart Compras (fato_compras, grão de item comprado, exclusivo da "
            "Mercearia) - custo de reposição, lead time e dependência de fornecedor. "
            "Filtros: Período e Fornecedor, no topo do painel."
        ),
    })
    dash_id = dash["id"]

    dashcards = [
        # linha 0: KPIs
        dashcard(-1, cards["total_comprado"], 0, 0, 6, 3),
        dashcard(-2, cards["qtd_itens_comprados"], 0, 6, 6, 3),
        dashcard(-3, cards["lead_time_medio"], 0, 12, 6, 3),
        dashcard(-4, cards["num_compras"], 0, 18, 6, 3),
        # linha 1: fornecedores
        dashcard(-5, cards["top10_fornecedores_valor"], 3, 0, 12, 8),
        dashcard(-6, cards["lead_time_por_fornecedor"], 3, 12, 12, 8),
        # linha 2: temporal + produtos
        dashcard(-7, cards["compras_por_mes"], 11, 0, 12, 7),
        dashcard(-8, cards["top10_produtos_comprados"], 11, 12, 12, 7),
        # linha 3: tabela detalhada
        dashcard(-9, cards["resumo_fornecedor"], 18, 0, 24, 8),
        # linha 4: cubo OLAP (pivot table, 2 dimensões x 1 medida)
        dashcard(-10, cards["cubo_categoria_periodo"], 26, 0, 24, 8),
    ]
    mb.put(f"/api/dashboard/{dash_id}", json={
        "dashcards": dashcards,
        "parameters": [PARAM_DEFS[f] for f in FILTERS_PERIODO_FORNECEDOR],
    })
    return dash_id


def get_or_create_database(mb: MB) -> int:
    dbs = mb.get("/api/database")["data"]
    db_id = next((d["id"] for d in dbs if d["name"] == DB_NAME), None)
    if db_id is not None:
        print(f"Banco '{DB_NAME}' já existe (id={db_id}).", flush=True)
        return db_id

    print(f"Criando conexão com o banco '{DB_NAME}'...", flush=True)
    db = mb.post("/api/database", json={
        "engine": "postgres",
        "name": DB_NAME,
        "details": {
            "host": "postgres",
            "port": 5432,
            "dbname": "dw",
            "user": "postgres",
            "password": "postgres",
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "public",
        },
        "is_full_sync": True,
    })
    db_id = db["id"]
    print("Aguardando sincronização do schema...", flush=True)
    for _ in range(60):
        info = mb.get(f"/api/database/{db_id}")
        if info.get("initial_sync_status") == "complete":
            break
        time.sleep(3)
    return db_id


def provision_dashboard(mb, db_id, dashboard_name, collection_name, collection_description, build_cards_fn, build_dashboard_fn):
    dashboards = mb.get("/api/dashboard")
    existing = [d for d in dashboards if d["name"] == dashboard_name]
    if existing:
        print(f"Dashboard '{dashboard_name}' já existe (id={existing[0]['id']}). Nada a fazer.", flush=True)
        return existing[0]["id"]

    collection_id = get_or_create_collection(mb, collection_name, collection_description)
    print(f"Coleção '{collection_name}' (id={collection_id}).", flush=True)

    print(f"Criando as consultas (cards) de '{dashboard_name}'...", flush=True)
    cards = build_cards_fn(mb, db_id, collection_id)

    print(f"Montando o dashboard '{dashboard_name}'...", flush=True)
    return build_dashboard_fn(mb, collection_id, cards)


def main():
    wait_ready()
    token = get_session()
    mb = MB(MB_URL, token)

    get_or_create_standard_user(mb)
    db_id = get_or_create_database(mb)

    dash_ids = {
        VENDAS_DASHBOARD_NAME: provision_dashboard(
            mb, db_id, VENDAS_DASHBOARD_NAME, VENDAS_COLLECTION_NAME,
            "Painéis de BI sobre o Mart Vendas (fato_vendas).",
            build_cards_vendas, build_dashboard_vendas,
        ),
        COMPRAS_DASHBOARD_NAME: provision_dashboard(
            mb, db_id, COMPRAS_DASHBOARD_NAME, COMPRAS_COLLECTION_NAME,
            "Painéis de BI sobre o Mart Compras (fato_compras).",
            build_cards_compras, build_dashboard_compras,
        ),
        DASHBOARD_NAME: provision_dashboard(
            mb, db_id, DASHBOARD_NAME, COLLECTION_NAME,
            "Painéis de BI sobre o Mart Logística/Entregas (Northwind).",
            build_cards_entregas, build_dashboard_entregas,
        ),
    }

    print("=" * 70, flush=True)
    for name, dash_id in dash_ids.items():
        print(f"{name}: http://localhost:3000/dashboard/{dash_id}", flush=True)
    print(f"Login padrão (visualização):  {STANDARD_EMAIL} / {STANDARD_PASSWORD}", flush=True)
    print(f"Login admin (administração):  {ADMIN_EMAIL} / {ADMIN_PASSWORD}", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"Erro HTTP: {e.response.status_code} {e.response.text}", file=sys.stderr)
        raise
