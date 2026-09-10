from __future__ import annotations

from datetime import datetime

import pymysql
import psycopg2
import psycopg2.extras
from airflow import DAG
from airflow.operators.python import PythonOperator

TIPO_VENDA_MAP = {1: "À vista", 2: "Prazo", 3: "Cartão"}
TIPO_TELEFONE_MAP = {1: "Residencial", 2: "Comercial", 3: "Celular"}


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


def carregar_dim_enderecos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        rows = []
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT e.ID_ENDERECO, e.TIPO_ENDERECO, l.LOGRADOURO, l.CEP,
                       b.NOME_BAIRRO, b.REGIAOCIDADE, c.NOME_CIDADE, u.NOME_ESTADO, u.SIGLA
                FROM Enderecos e
                JOIN Logradouros l ON l.ID_LOGRADOURO = e.ID_LOGRADOURO
                JOIN Bairros b ON b.ID_BAIRRO = l.ID_BAIRRO
                JOIN Cidades c ON c.ID_CIDADE = b.ID_CIDADE
                JOIN Uf u ON u.ID_UF = c.ID_UF
            """)
            for r in cur.fetchall():
                tipo = "Residencial" if r["TIPO_ENDERECO"] == 1 else "Comercial"
                rows.append((
                    "Mercearia", str(r["ID_ENDERECO"]), tipo, r["LOGRADOURO"], str(r["CEP"]),
                    r["NOME_BAIRRO"], r["REGIAOCIDADE"], r["NOME_CIDADE"], r["NOME_ESTADO"], r["SIGLA"], "Brasil",
                ))

        with nw_conn.cursor() as cur:
            cur.execute("SELECT customer_id, address, postal_code, region, city, country FROM customers")
            for customer_id, address, postal_code, region, city, country in cur.fetchall():
                rows.append((
                    "Northwind", customer_id, None, address, postal_code,
                    None, region, city, None, None, country,
                ))

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO dim_enderecos
                    (sistema_origem, id_endereco_original, tipo_endereco, nome_logradouro, cep,
                     nome_bairro, regiao_cidade, nome_cidade, nome_estado, sigla_uf, pais)
                VALUES %s
                ON CONFLICT (sistema_origem, id_endereco_original) DO NOTHING
            """, rows)
        pg_conn.commit()
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_dim_produtos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        rows = []
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT pr.ID_PRODUTO, pr.PRODUTO, pr.VALOR_VENDA, c.NOME_CATEGORIA
                FROM Produtos pr
                JOIN Categorias c ON c.ID_CATEGORIA = pr.ID_CATEGORIA
            """)
            for r in cur.fetchall():
                rows.append((
                    "Mercearia", str(r["ID_PRODUTO"]), r["PRODUTO"], r["NOME_CATEGORIA"], r["VALOR_VENDA"], "BRL",
                ))

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT p.product_id, p.product_name, p.unit_price, c.category_name
                FROM products p
                JOIN categories c ON c.category_id = p.category_id
            """)
            for product_id, product_name, unit_price, category_name in cur.fetchall():
                rows.append(("Northwind", str(product_id), product_name, category_name, unit_price, "USD"))

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO dim_produtos
                    (sistema_origem, id_produto_original, nome_produto, nome_categoria, valor, moeda)
                VALUES %s
                ON CONFLICT (sistema_origem, id_produto_original) DO NOTHING
            """, rows)
        pg_conn.commit()
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_dim_cliente():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        rows = []
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT p.ID_PESSOA, p.NOME, p.SEXO, p.RENDA, p.ESTADO_CIVIL, p.DATA_NASCIMENTO,
                       pr.NOME_PROFISSAO
                FROM Pessoas p
                JOIN Profissoes pr ON pr.ID_PROFISSAO = p.ID_PROFISSAO
            """)
            pessoas = cur.fetchall()

            cur.execute("SELECT ID_PESSOA, DDD, TIPO FROM Telefones WHERE PREFERENCIAL = 1")
            telefone_pref = {r["ID_PESSOA"]: r for r in cur.fetchall()}

        for p in pessoas:
            tel = telefone_pref.get(p["ID_PESSOA"])
            rows.append((
                "Mercearia", str(p["ID_PESSOA"]), "Física", p["NOME"], p["SEXO"],
                faixa_renda(p["RENDA"]), p["ESTADO_CIVIL"], faixa_etaria(p["DATA_NASCIMENTO"]),
                p["DATA_NASCIMENTO"].year if p["DATA_NASCIMENTO"] else None,
                p["NOME_PROFISSAO"],
                str(tel["DDD"]) if tel else None,
                TIPO_TELEFONE_MAP.get(tel["TIPO"]) if tel else None,
            ))

        with nw_conn.cursor() as cur:
            cur.execute("SELECT customer_id, company_name, contact_title FROM customers")
            for customer_id, company_name, contact_title in cur.fetchall():
                rows.append((
                    "Northwind", customer_id, "Jurídica", company_name, None,
                    None, None, None, None, contact_title, None, None,
                ))

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO dim_cliente
                    (sistema_origem, id_cliente_original, tipo_pessoa, nome_cliente, sexo,
                     faixa_renda, estado_civil, faixa_etaria, ano_nascimento, nome_profissao,
                     ddd_principal, tipo_telefone)
                VALUES %s
                ON CONFLICT (sistema_origem, id_cliente_original) DO NOTHING
            """, rows)
        pg_conn.commit()
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_dim_funcionario():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with nw_conn.cursor() as cur:
            cur.execute("SELECT employee_id, first_name, last_name, title, hire_date, city, country FROM employees")
            rows = [
                ("Northwind", str(eid), f"{first} {last}", title, hire_date, city, country)
                for eid, first, last, title, hire_date, city, country in cur.fetchall()
            ]
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO dim_funcionario
                    (sistema_origem, id_funcionario_original, nome_funcionario, cargo, data_contratacao, cidade, pais)
                VALUES %s
                ON CONFLICT (sistema_origem, id_funcionario_original) DO NOTHING
            """, rows)
        pg_conn.commit()
    finally:
        nw_conn.close()
        pg_conn.close()


def carregar_dim_transportadora():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with nw_conn.cursor() as cur:
            cur.execute("SELECT shipper_id, company_name, phone FROM shippers")
            rows = [("Northwind", str(sid), name, phone) for sid, name, phone in cur.fetchall()]
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO dim_transportadora
                    (sistema_origem, id_transportadora_original, nome_transportadora, telefone)
                VALUES %s
                ON CONFLICT (sistema_origem, id_transportadora_original) DO NOTHING
            """, rows)
        pg_conn.commit()
    finally:
        nw_conn.close()
        pg_conn.close()


def carregar_fato_vendas():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_vendas")

            cur.execute("SELECT data, id_dim_tempo FROM dim_tempo")
            tempo_map = {row[0]: row[1] for row in cur.fetchall()}
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

        rows = []

        with mysql_conn.cursor() as cur:
            cur.execute("SELECT ID_PESSOA, ID_ENDERECO FROM Enderecos")
            endereco_pessoa = {r["ID_PESSOA"]: r["ID_ENDERECO"] for r in cur.fetchall()}

            cur.execute("""
                SELECT v.ID_VENDA, v.ID_PESSOA, v.DATA_VENDA, v.TIPO_VENDA,
                       iv.ID_ITEMVENDA, iv.ID_PRODUTO, iv.QUANTIDADE, iv.VLR_UNITARIO
                FROM Vendas v
                JOIN Itens_Vendas iv ON iv.ID_VENDA = v.ID_VENDA
            """)
            for r in cur.fetchall():
                id_endereco_orig = str(endereco_pessoa.get(r["ID_PESSOA"]))
                valor_total = float(r["QUANTIDADE"]) * float(r["VLR_UNITARIO"])
                rows.append((
                    "Mercearia", str(r["ID_VENDA"]), str(r["ID_ITEMVENDA"]),
                    tempo_map.get(r["DATA_VENDA"]),
                    cliente_map.get(("Mercearia", str(r["ID_PESSOA"]))),
                    produto_map.get(("Mercearia", str(r["ID_PRODUTO"]))),
                    endereco_map.get(("Mercearia", id_endereco_orig)),
                    -1, -1,
                    TIPO_VENDA_MAP.get(r["TIPO_VENDA"]),
                    r["QUANTIDADE"], r["VLR_UNITARIO"], 0, valor_total,
                ))

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT o.order_id, o.customer_id, o.employee_id, o.order_date, o.ship_via,
                       od.product_id, od.quantity, od.unit_price, od.discount
                FROM orders o
                JOIN order_details od ON od.order_id = o.order_id
            """)
            for order_id, customer_id, employee_id, order_date, ship_via, product_id, quantity, unit_price, discount in cur.fetchall():
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

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO fato_vendas
                    (sistema_origem, id_venda_original, id_item_original, id_dim_tempo, id_dim_cliente,
                     id_dim_produto, id_dim_endereco, id_dim_funcionario, id_dim_transportadora,
                     tipo_venda, quantidade, valor_unitario, valor_desconto, valor_total_item)
                VALUES %s
            """, rows)
        pg_conn.commit()
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_fato_compras():
    mysql_conn = get_mysql_conn()
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_compras")

            cur.execute("SELECT data, id_dim_tempo FROM dim_tempo")
            tempo_map = {row[0]: row[1] for row in cur.fetchall()}
            cur.execute("SELECT id_cliente_original, id_dim_cliente FROM dim_cliente WHERE sistema_origem = 'Mercearia'")
            cliente_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_produto_original, id_dim_produto FROM dim_produtos WHERE sistema_origem = 'Mercearia'")
            produto_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_endereco_original, id_dim_endereco FROM dim_enderecos WHERE sistema_origem = 'Mercearia'")
            endereco_map = {r[0]: r[1] for r in cur.fetchall()}

        rows = []
        with mysql_conn.cursor() as cur:
            cur.execute("SELECT ID_PESSOA, ID_ENDERECO FROM Enderecos")
            endereco_pessoa = {r["ID_PESSOA"]: r["ID_ENDERECO"] for r in cur.fetchall()}

            cur.execute("""
                SELECT c.ID_COMPRA, c.ID_PESSOA, c.DATA_PEDIDO, c.DATA_ENTRADA,
                       ic.ID_ITEMCOMPRA, ic.ID_PRODUTO, ic.QUANTIDADE, ic.VLR_UNITARIO
                FROM Compras c
                JOIN Itens_compras ic ON ic.ID_COMPRA = c.ID_COMPRA
            """)
            for r in cur.fetchall():
                id_endereco_orig = str(endereco_pessoa.get(r["ID_PESSOA"]))
                valor_total = float(r["QUANTIDADE"]) * float(r["VLR_UNITARIO"])
                lead_time = (r["DATA_ENTRADA"] - r["DATA_PEDIDO"]).days if r["DATA_ENTRADA"] else None
                rows.append((
                    "Mercearia", str(r["ID_COMPRA"]), str(r["ID_ITEMCOMPRA"]),
                    tempo_map.get(r["DATA_PEDIDO"]),
                    cliente_map.get(str(r["ID_PESSOA"])),
                    produto_map.get(str(r["ID_PRODUTO"])),
                    endereco_map.get(id_endereco_orig),
                    r["QUANTIDADE"], r["VLR_UNITARIO"], valor_total, lead_time,
                ))

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO fato_compras
                    (sistema_origem, id_compra_original, id_item_original, id_dim_tempo, id_dim_cliente,
                     id_dim_produto, id_dim_endereco, quantidade, valor_unitario, valor_total_item, lead_time_dias)
                VALUES %s
            """, rows)
        pg_conn.commit()
    finally:
        mysql_conn.close()
        pg_conn.close()


with DAG(
    dag_id="carga_inicial_dw",
    description="Primeira carga do Data Warehouse a partir da Mercearia (MySQL) e do Northwind (Postgres)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-inicial"],
) as dag:

    t_dim_enderecos = PythonOperator(task_id="carregar_dim_enderecos", python_callable=carregar_dim_enderecos)
    t_dim_produtos = PythonOperator(task_id="carregar_dim_produtos", python_callable=carregar_dim_produtos)
    t_dim_cliente = PythonOperator(task_id="carregar_dim_cliente", python_callable=carregar_dim_cliente)
    t_dim_funcionario = PythonOperator(task_id="carregar_dim_funcionario", python_callable=carregar_dim_funcionario)
    t_dim_transportadora = PythonOperator(task_id="carregar_dim_transportadora", python_callable=carregar_dim_transportadora)
    t_fato_vendas = PythonOperator(task_id="carregar_fato_vendas", python_callable=carregar_fato_vendas)
    t_fato_compras = PythonOperator(task_id="carregar_fato_compras", python_callable=carregar_fato_compras)

    [t_dim_enderecos, t_dim_produtos, t_dim_cliente, t_dim_funcionario, t_dim_transportadora] >> t_fato_vendas
    [t_dim_enderecos, t_dim_produtos, t_dim_cliente] >> t_fato_compras
