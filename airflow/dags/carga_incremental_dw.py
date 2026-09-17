from __future__ import annotations

from datetime import datetime

import pymysql
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.python import PythonOperator

TIPO_VENDA_MAP = {1: "À vista", 2: "Prazo", 3: "Cartão"}
TIPO_TELEFONE_MAP = {1: "Residencial", 2: "Comercial", 3: "Celular"}

COLS_CLIENTE = [
    "tipo_pessoa", "nome_cliente", "sexo", "faixa_renda", "estado_civil",
    "faixa_etaria", "ano_nascimento", "nome_profissao", "ddd_principal", "tipo_telefone",
]
COLS_PRODUTO = ["nome_produto", "nome_categoria", "valor", "moeda"]
COLS_ENDERECO = [
    "tipo_endereco", "nome_logradouro", "cep", "nome_bairro",
    "regiao_cidade", "nome_cidade", "nome_estado", "sigla_uf", "pais",
]
COLS_FUNCIONARIO = ["nome_funcionario", "cargo", "data_contratacao", "cidade", "pais"]
COLS_TRANSPORTADORA = ["nome_transportadora", "telefone"]


def get_mysql_conn():
    return pymysql.connect(
        host="mysql", port=3306, user="root", password="mysql",
        database="mercearia", charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )


def get_pg_conn(dbname):
    return psycopg2.connect(host="postgres", port=5432, user="postgres", password="postgres", dbname=dbname)


def faixa_etaria(data_nascimento):
    if data_nascimento is None:
        return None
    from datetime import date
    idade = (date.today() - data_nascimento).days // 365
    if idade <= 25:
        return "18-25"
    if idade <= 35:
        return "26-35"
    if idade <= 45:
        return "36-45"
    if idade <= 60:
        return "46-60"
    return "60+"


def faixa_renda(renda):
    if renda is None:
        return None
    renda = float(renda)
    if renda < 2000:
        return "Até 2 mil"
    if renda < 5000:
        return "2 a 5 mil"
    if renda < 10000:
        return "5 a 10 mil"
    if renda < 20000:
        return "10 a 20 mil"
    return "Acima de 20 mil"


def upsert_dimensao(pg_conn, tabela, natural_cols, natural_vals, cols, novos_valores):
    """INSERT se a chave natural ainda não existe; UPDATE só se algum atributo mudou; nada se está igual."""
    with pg_conn.cursor() as cur:
        where = " AND ".join(f"{c} = %s" for c in natural_cols)
        cur.execute(f"SELECT {','.join(cols)} FROM {tabela} WHERE {where}", natural_vals)
        atual = cur.fetchone()
        if atual is None:
            cur.execute(
                f"INSERT INTO {tabela} ({','.join(natural_cols)}, {','.join(cols)}) "
                f"VALUES ({','.join(['%s'] * len(natural_vals))}, {','.join(['%s'] * len(cols))})",
                (*natural_vals, *novos_valores),
            )
            return 1, 0
        if tuple(atual) != tuple(novos_valores):
            sets = ", ".join(f"{c} = %s" for c in cols)
            cur.execute(f"UPDATE {tabela} SET {sets} WHERE {where}", (*novos_valores, *natural_vals))
            return 0, 1
        return 0, 0


# ----------------------------------------------------------------
# Staging: detecta e traz só os fatos novos, comparando com o maior
# id de origem já presente na fato — não usa data como corte.
# ----------------------------------------------------------------

def stage_vendas_mercearia():
    pg_conn = get_pg_conn("dw")
    mysql_conn = get_mysql_conn()
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id_venda_original::int), 0) FROM fato_vendas WHERE sistema_origem = 'Mercearia'")
            watermark = cur.fetchone()[0]
            cur.execute("TRUNCATE staging.stg_vendas_mercearia")

        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT v.ID_VENDA, v.ID_PESSOA, e.ID_ENDERECO, v.DATA_VENDA, v.TIPO_VENDA,
                       iv.ID_ITEMVENDA, iv.ID_PRODUTO, iv.QUANTIDADE, iv.VLR_UNITARIO
                FROM Vendas v
                JOIN Itens_Vendas iv ON iv.ID_VENDA = v.ID_VENDA
                JOIN Enderecos e ON e.ID_PESSOA = v.ID_PESSOA
                WHERE v.ID_VENDA > %s
            """, (watermark,))
            rows = [
                (r["ID_VENDA"], r["ID_PESSOA"], r["ID_ENDERECO"], r["DATA_VENDA"], r["TIPO_VENDA"],
                 r["ID_ITEMVENDA"], r["ID_PRODUTO"], r["QUANTIDADE"], r["VLR_UNITARIO"])
                for r in cur.fetchall()
            ]

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO staging.stg_vendas_mercearia
                        (id_venda, id_pessoa, id_endereco, data_venda, tipo_venda,
                         id_itemvenda, id_produto, quantidade, vlr_unitario)
                    VALUES %s
                """, rows)
        pg_conn.commit()
        print(f"watermark={watermark}, {len(rows)} novos itens de venda (Mercearia)")
    finally:
        mysql_conn.close()
        pg_conn.close()


def stage_vendas_northwind():
    pg_conn = get_pg_conn("dw")
    nw_conn = get_pg_conn("northwind")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id_venda_original::int), 0) FROM fato_vendas WHERE sistema_origem = 'Northwind'")
            watermark = cur.fetchone()[0]
            cur.execute("TRUNCATE staging.stg_vendas_northwind")

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT o.order_id, o.customer_id, o.employee_id, o.order_date, o.ship_via,
                       od.product_id, od.quantity, od.unit_price, od.discount
                FROM orders o
                JOIN order_details od ON od.order_id = o.order_id
                WHERE o.order_id > %s
            """, (watermark,))
            rows = cur.fetchall()

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO staging.stg_vendas_northwind
                        (order_id, customer_id, employee_id, order_date, ship_via,
                         product_id, quantity, unit_price, discount)
                    VALUES %s
                """, rows)
        pg_conn.commit()
        print(f"watermark={watermark}, {len(rows)} novos itens de pedido (Northwind)")
    finally:
        nw_conn.close()
        pg_conn.close()


def stage_compras_mercearia():
    pg_conn = get_pg_conn("dw")
    mysql_conn = get_mysql_conn()
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id_compra_original::int), 0) FROM fato_compras WHERE sistema_origem = 'Mercearia'")
            watermark = cur.fetchone()[0]
            cur.execute("TRUNCATE staging.stg_compras_mercearia")

        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT c.ID_COMPRA, c.ID_PESSOA, e.ID_ENDERECO, c.DATA_PEDIDO, c.DATA_ENTRADA,
                       ic.ID_ITEMCOMPRA, ic.ID_PRODUTO, ic.QUANTIDADE, ic.VLR_UNITARIO
                FROM Compras c
                JOIN Itens_compras ic ON ic.ID_COMPRA = c.ID_COMPRA
                JOIN Enderecos e ON e.ID_PESSOA = c.ID_PESSOA
                WHERE c.ID_COMPRA > %s
            """, (watermark,))
            rows = [
                (r["ID_COMPRA"], r["ID_PESSOA"], r["ID_ENDERECO"], r["DATA_PEDIDO"], r["DATA_ENTRADA"],
                 r["ID_ITEMCOMPRA"], r["ID_PRODUTO"], r["QUANTIDADE"], r["VLR_UNITARIO"])
                for r in cur.fetchall()
            ]

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO staging.stg_compras_mercearia
                        (id_compra, id_pessoa, id_endereco, data_pedido, data_entrada,
                         id_itemcompra, id_produto, quantidade, vlr_unitario)
                    VALUES %s
                """, rows)
        pg_conn.commit()
        print(f"watermark={watermark}, {len(rows)} novos itens de compra (Mercearia)")
    finally:
        mysql_conn.close()
        pg_conn.close()


# ----------------------------------------------------------------
# Dimensões: só olha as chaves naturais que apareceram na staging,
# e só grava (INSERT/UPDATE) quando o valor realmente mudou.
# ----------------------------------------------------------------

def atualizar_dim_cliente():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_pessoa FROM staging.stg_vendas_mercearia
                UNION SELECT id_pessoa FROM staging.stg_compras_mercearia
            """)
            pessoa_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT customer_id FROM staging.stg_vendas_northwind")
            customer_ids = [r[0] for r in cur.fetchall()]

        inserted = updated = 0

        if pessoa_ids:
            with mysql_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(pessoa_ids))
                cur.execute(f"""
                    SELECT p.ID_PESSOA, p.NOME, p.SEXO, p.RENDA, p.ESTADO_CIVIL, p.DATA_NASCIMENTO,
                           pr.NOME_PROFISSAO
                    FROM Pessoas p
                    JOIN Profissoes pr ON pr.ID_PROFISSAO = p.ID_PROFISSAO
                    WHERE p.ID_PESSOA IN ({fmt})
                """, pessoa_ids)
                pessoas = cur.fetchall()
                cur.execute(f"SELECT ID_PESSOA, DDD, TIPO FROM Telefones WHERE PREFERENCIAL = 1 AND ID_PESSOA IN ({fmt})", pessoa_ids)
                telefone_pref = {r["ID_PESSOA"]: r for r in cur.fetchall()}

            for p in pessoas:
                tel = telefone_pref.get(p["ID_PESSOA"])
                novos = (
                    "Física", p["NOME"], p["SEXO"], faixa_renda(p["RENDA"]), p["ESTADO_CIVIL"],
                    faixa_etaria(p["DATA_NASCIMENTO"]),
                    p["DATA_NASCIMENTO"].year if p["DATA_NASCIMENTO"] else None,
                    p["NOME_PROFISSAO"],
                    str(tel["DDD"]) if tel else None,
                    TIPO_TELEFONE_MAP.get(tel["TIPO"]) if tel else None,
                )
                i, u = upsert_dimensao(
                    pg_conn, "dim_cliente", ["sistema_origem", "id_cliente_original"],
                    ["Mercearia", str(p["ID_PESSOA"])], COLS_CLIENTE, novos,
                )
                inserted += i
                updated += u

        if customer_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(customer_ids))
                cur.execute(f"SELECT customer_id, company_name, contact_title FROM customers WHERE customer_id IN ({fmt})", customer_ids)
                for customer_id, company_name, contact_title in cur.fetchall():
                    novos = ("Jurídica", company_name, None, None, None, None, None, contact_title, None, None)
                    i, u = upsert_dimensao(
                        pg_conn, "dim_cliente", ["sistema_origem", "id_cliente_original"],
                        ["Northwind", customer_id], COLS_CLIENTE, novos,
                    )
                    inserted += i
                    updated += u

        pg_conn.commit()
        print(f"dim_cliente: {inserted} inseridos, {updated} atualizados")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_dim_produtos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_produto FROM staging.stg_vendas_mercearia
                UNION SELECT id_produto FROM staging.stg_compras_mercearia
            """)
            produtos_mercearia = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT product_id FROM staging.stg_vendas_northwind")
            produtos_northwind = [r[0] for r in cur.fetchall()]

        inserted = updated = 0

        if produtos_mercearia:
            with mysql_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(produtos_mercearia))
                cur.execute(f"""
                    SELECT pr.ID_PRODUTO, pr.PRODUTO, pr.VALOR_VENDA, c.NOME_CATEGORIA
                    FROM Produtos pr
                    JOIN Categorias c ON c.ID_CATEGORIA = pr.ID_CATEGORIA
                    WHERE pr.ID_PRODUTO IN ({fmt})
                """, produtos_mercearia)
                for r in cur.fetchall():
                    novos = (r["PRODUTO"], r["NOME_CATEGORIA"], r["VALOR_VENDA"], "BRL")
                    i, u = upsert_dimensao(
                        pg_conn, "dim_produtos", ["sistema_origem", "id_produto_original"],
                        ["Mercearia", str(r["ID_PRODUTO"])], COLS_PRODUTO, novos,
                    )
                    inserted += i
                    updated += u

        if produtos_northwind:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(produtos_northwind))
                cur.execute(f"""
                    SELECT p.product_id, p.product_name, p.unit_price, c.category_name
                    FROM products p
                    JOIN categories c ON c.category_id = p.category_id
                    WHERE p.product_id IN ({fmt})
                """, produtos_northwind)
                for product_id, product_name, unit_price, category_name in cur.fetchall():
                    novos = (product_name, category_name, unit_price, "USD")
                    i, u = upsert_dimensao(
                        pg_conn, "dim_produtos", ["sistema_origem", "id_produto_original"],
                        ["Northwind", str(product_id)], COLS_PRODUTO, novos,
                    )
                    inserted += i
                    updated += u

        pg_conn.commit()
        print(f"dim_produtos: {inserted} inseridos, {updated} atualizados")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_dim_enderecos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_endereco FROM staging.stg_vendas_mercearia
                UNION SELECT id_endereco FROM staging.stg_compras_mercearia
            """)
            endereco_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT customer_id FROM staging.stg_vendas_northwind")
            customer_ids = [r[0] for r in cur.fetchall()]

        inserted = updated = 0

        if endereco_ids:
            with mysql_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(endereco_ids))
                cur.execute(f"""
                    SELECT e.ID_ENDERECO, e.TIPO_ENDERECO, l.LOGRADOURO, l.CEP,
                           b.NOME_BAIRRO, b.REGIAOCIDADE, c.NOME_CIDADE, u.NOME_ESTADO, u.SIGLA
                    FROM Enderecos e
                    JOIN Logradouros l ON l.ID_LOGRADOURO = e.ID_LOGRADOURO
                    JOIN Bairros b ON b.ID_BAIRRO = l.ID_BAIRRO
                    JOIN Cidades c ON c.ID_CIDADE = b.ID_CIDADE
                    JOIN Uf u ON u.ID_UF = c.ID_UF
                    WHERE e.ID_ENDERECO IN ({fmt})
                """, endereco_ids)
                for r in cur.fetchall():
                    tipo = "Residencial" if r["TIPO_ENDERECO"] == 1 else "Comercial"
                    novos = (
                        tipo, r["LOGRADOURO"], str(r["CEP"]), r["NOME_BAIRRO"], r["REGIAOCIDADE"],
                        r["NOME_CIDADE"], r["NOME_ESTADO"], r["SIGLA"], "Brasil",
                    )
                    i, u = upsert_dimensao(
                        pg_conn, "dim_enderecos", ["sistema_origem", "id_endereco_original"],
                        ["Mercearia", str(r["ID_ENDERECO"])], COLS_ENDERECO, novos,
                    )
                    inserted += i
                    updated += u

        if customer_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(customer_ids))
                cur.execute(f"SELECT customer_id, address, postal_code, region, city, country FROM customers WHERE customer_id IN ({fmt})", customer_ids)
                for customer_id, address, postal_code, region, city, country in cur.fetchall():
                    novos = (None, address, postal_code, None, region, city, None, None, country)
                    i, u = upsert_dimensao(
                        pg_conn, "dim_enderecos", ["sistema_origem", "id_endereco_original"],
                        ["Northwind", customer_id], COLS_ENDERECO, novos,
                    )
                    inserted += i
                    updated += u

        pg_conn.commit()
        print(f"dim_enderecos: {inserted} inseridos, {updated} atualizados")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_dim_funcionario():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT employee_id FROM staging.stg_vendas_northwind WHERE employee_id IS NOT NULL")
            employee_ids = [r[0] for r in cur.fetchall()]

        inserted = updated = 0
        if employee_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(employee_ids))
                cur.execute(f"""
                    SELECT employee_id, first_name, last_name, title, hire_date, city, country
                    FROM employees WHERE employee_id IN ({fmt})
                """, employee_ids)
                for eid, first, last, title, hire_date, city, country in cur.fetchall():
                    novos = (f"{first} {last}", title, hire_date, city, country)
                    i, u = upsert_dimensao(
                        pg_conn, "dim_funcionario", ["sistema_origem", "id_funcionario_original"],
                        ["Northwind", str(eid)], COLS_FUNCIONARIO, novos,
                    )
                    inserted += i
                    updated += u

        pg_conn.commit()
        print(f"dim_funcionario: {inserted} inseridos, {updated} atualizados")
    finally:
        nw_conn.close()
        pg_conn.close()


def atualizar_dim_transportadora():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ship_via FROM staging.stg_vendas_northwind WHERE ship_via IS NOT NULL")
            ship_ids = [r[0] for r in cur.fetchall()]

        inserted = updated = 0
        if ship_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(ship_ids))
                cur.execute(f"SELECT shipper_id, company_name, phone FROM shippers WHERE shipper_id IN ({fmt})", ship_ids)
                for sid, name, phone in cur.fetchall():
                    novos = (name, phone)
                    i, u = upsert_dimensao(
                        pg_conn, "dim_transportadora", ["sistema_origem", "id_transportadora_original"],
                        ["Northwind", str(sid)], COLS_TRANSPORTADORA, novos,
                    )
                    inserted += i
                    updated += u

        pg_conn.commit()
        print(f"dim_transportadora: {inserted} inseridos, {updated} atualizados")
    finally:
        nw_conn.close()
        pg_conn.close()


# ----------------------------------------------------------------
# Fatos: insere só as linhas que ficaram na staging (append-only).
# ----------------------------------------------------------------

def carregar_fato_vendas():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT data, id_dim_tempo FROM dim_tempo")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_cliente_original, id_dim_cliente FROM dim_cliente")
            cliente_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_produto_original, id_dim_produto FROM dim_produtos")
            produto_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_endereco_original, id_dim_endereco FROM dim_enderecos")
            endereco_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_funcionario_original, id_dim_funcionario FROM dim_funcionario")
            funcionario_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_transportadora_original, id_dim_transportadora FROM dim_transportadora")
            transportadora_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}

            cur.execute("""
                SELECT id_venda, id_pessoa, id_endereco, data_venda, tipo_venda,
                       id_itemvenda, id_produto, quantidade, vlr_unitario
                FROM staging.stg_vendas_mercearia
            """)
            staged_mercearia = cur.fetchall()
            cur.execute("""
                SELECT order_id, customer_id, employee_id, order_date, ship_via,
                       product_id, quantity, unit_price, discount
                FROM staging.stg_vendas_northwind
            """)
            staged_northwind = cur.fetchall()

        rows = []
        for (id_venda, id_pessoa, id_endereco, data_venda, tipo_venda,
             id_itemvenda, id_produto, quantidade, vlr_unitario) in staged_mercearia:
            valor_total = float(quantidade) * float(vlr_unitario)
            rows.append((
                "Mercearia", str(id_venda), str(id_itemvenda),
                tempo_map.get(data_venda),
                cliente_map.get(("Mercearia", str(id_pessoa))),
                produto_map.get(("Mercearia", str(id_produto))),
                endereco_map.get(("Mercearia", str(id_endereco))),
                -1, -1,
                TIPO_VENDA_MAP.get(tipo_venda),
                quantidade, vlr_unitario, 0, valor_total,
            ))

        for (order_id, customer_id, employee_id, order_date, ship_via,
             product_id, quantity, unit_price, discount) in staged_northwind:
            valor_total = float(quantity) * float(unit_price) * (1 - float(discount))
            rows.append((
                "Northwind", str(order_id), None,
                tempo_map.get(order_date),
                cliente_map.get(("Northwind", customer_id)),
                produto_map.get(("Northwind", str(product_id))),
                endereco_map.get(("Northwind", customer_id)),
                funcionario_map.get(("Northwind", str(employee_id)), -1) if employee_id else -1,
                transportadora_map.get(("Northwind", str(ship_via)), -1) if ship_via else -1,
                None,
                quantity, unit_price, discount, valor_total,
            ))

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_vendas
                        (sistema_origem, id_venda_original, id_item_original, id_dim_tempo, id_dim_cliente,
                         id_dim_produto, id_dim_endereco, id_dim_funcionario, id_dim_transportadora,
                         tipo_venda, quantidade, valor_unitario, valor_desconto, valor_total_item)
                    VALUES %s
                """, rows)
            pg_conn.commit()
        print(f"fato_vendas: {len(rows)} linhas novas inseridas")
    finally:
        pg_conn.close()


def carregar_fato_compras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT data, id_dim_tempo FROM dim_tempo")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_cliente_original, id_dim_cliente FROM dim_cliente WHERE sistema_origem = 'Mercearia'")
            cliente_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_produto_original, id_dim_produto FROM dim_produtos WHERE sistema_origem = 'Mercearia'")
            produto_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_endereco_original, id_dim_endereco FROM dim_enderecos WHERE sistema_origem = 'Mercearia'")
            endereco_map = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("""
                SELECT id_compra, id_pessoa, id_endereco, data_pedido, data_entrada,
                       id_itemcompra, id_produto, quantidade, vlr_unitario
                FROM staging.stg_compras_mercearia
            """)
            staged = cur.fetchall()

        rows = []
        for (id_compra, id_pessoa, id_endereco, data_pedido, data_entrada,
             id_itemcompra, id_produto, quantidade, vlr_unitario) in staged:
            valor_total = float(quantidade) * float(vlr_unitario)
            lead_time = (data_entrada - data_pedido).days if data_entrada else None
            rows.append((
                "Mercearia", str(id_compra), str(id_itemcompra),
                tempo_map.get(data_pedido),
                cliente_map.get(str(id_pessoa)),
                produto_map.get(str(id_produto)),
                endereco_map.get(str(id_endereco)),
                quantidade, vlr_unitario, valor_total, lead_time,
            ))

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_compras
                        (sistema_origem, id_compra_original, id_item_original, id_dim_tempo, id_dim_cliente,
                         id_dim_produto, id_dim_endereco, quantidade, valor_unitario, valor_total_item, lead_time_dias)
                    VALUES %s
                """, rows)
            pg_conn.commit()
        print(f"fato_compras: {len(rows)} linhas novas inseridas")
    finally:
        pg_conn.close()


def limpar_staging():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("TRUNCATE staging.stg_vendas_mercearia, staging.stg_vendas_northwind, staging.stg_compras_mercearia")
        pg_conn.commit()
        print("staging limpa")
    finally:
        pg_conn.close()


with DAG(
    dag_id="carga_incremental_dw",
    description="Carga parcial do DW: detecta fatos novos (por id, não por data) via staging, "
                "atualiza dimensões só quando o valor mudou e insere só os fatos novos",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-incremental"],
) as dag:

    t_stage_vendas_mercearia = PythonOperator(task_id="stage_vendas_mercearia", python_callable=stage_vendas_mercearia)
    t_stage_vendas_northwind = PythonOperator(task_id="stage_vendas_northwind", python_callable=stage_vendas_northwind)
    t_stage_compras_mercearia = PythonOperator(task_id="stage_compras_mercearia", python_callable=stage_compras_mercearia)

    t_dim_cliente = PythonOperator(task_id="atualizar_dim_cliente", python_callable=atualizar_dim_cliente)
    t_dim_produtos = PythonOperator(task_id="atualizar_dim_produtos", python_callable=atualizar_dim_produtos)
    t_dim_enderecos = PythonOperator(task_id="atualizar_dim_enderecos", python_callable=atualizar_dim_enderecos)
    t_dim_funcionario = PythonOperator(task_id="atualizar_dim_funcionario", python_callable=atualizar_dim_funcionario)
    t_dim_transportadora = PythonOperator(task_id="atualizar_dim_transportadora", python_callable=atualizar_dim_transportadora)

    t_fato_vendas = PythonOperator(task_id="carregar_fato_vendas", python_callable=carregar_fato_vendas)
    t_fato_compras = PythonOperator(task_id="carregar_fato_compras", python_callable=carregar_fato_compras)
    t_limpar_staging = PythonOperator(task_id="limpar_staging", python_callable=limpar_staging)

    staging_tasks = [t_stage_vendas_mercearia, t_stage_vendas_northwind, t_stage_compras_mercearia]
    dim_tasks = [t_dim_cliente, t_dim_produtos, t_dim_enderecos, t_dim_funcionario, t_dim_transportadora]

    cross_downstream(staging_tasks, dim_tasks)
    cross_downstream(dim_tasks, [t_fato_vendas, t_fato_compras])
    [t_fato_vendas, t_fato_compras] >> t_limpar_staging
