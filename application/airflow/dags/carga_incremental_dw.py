"""Carga incremental do Data Warehouse.

O corte de "o que é novo" é sempre pela tabela de fatos do corporativo
(corporativo.vendas / corporativo.compras) — não por data, pelo maior id de
origem já presente. Fluxo:

1. staging: só toca a origem (MySQL/Northwind) para trazer as poucas linhas
   com id > watermark — a única parte que realmente lê Mercearia/Northwind
   nesta DAG, e só as linhas novas.
2. corporativo (DW normalizado): atualiza lookups/histórico (cliente,
   funcionario, produto, endereco, transportadora, fornecedor) usando só as
   chaves naturais que apareceram na staging, e insere as vendas/compras
   novas (append-only).
3. data_marting (estrela): nunca toca a origem — dims são atualizadas a
   partir do corporativo, fatos são inseridos a partir da staging + dos
   dims já atualizados.
4. limpeza da staging.
"""
from __future__ import annotations

from datetime import datetime

import psycopg2.extras
from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.operators.python import PythonOperator

from common_etl import (
    TIPO_TELEFONE_MAP,
    faixa_renda,
    get_mysql_conn,
    get_or_create_by_nome,
    get_pg_conn,
    inserir_corporativo_compra,
    inserir_corporativo_item_compra,
    inserir_corporativo_item_venda,
    inserir_corporativo_venda_mercearia,
    inserir_corporativo_venda_northwind,
    refresh_dim_clientes,
    refresh_dim_enderecos,
    refresh_dim_fornecedores,
    refresh_dim_funcionarios,
    refresh_dim_produtos,
    refresh_dim_transportadoras,
    resolver_geografia_mercearia,
    resolver_geografia_northwind,
    upsert_historizado,
    upsert_simples,
)

# ==================================================================
# FASE 1 — staging: só as linhas novas, por watermark na fato do corporativo
# ==================================================================


def stage_vendas_mercearia():
    pg_conn = get_pg_conn("dw")
    mysql_conn = get_mysql_conn()
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id_venda_origem::int), 0) FROM corporativo.vendas WHERE sistema_origem = 'Mercearia'")
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
            cur.execute("SELECT COALESCE(MAX(id_venda_origem::int), 0) FROM corporativo.vendas WHERE sistema_origem = 'Northwind'")
            watermark = cur.fetchone()[0]
            cur.execute("TRUNCATE staging.stg_vendas_northwind")

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT o.order_id, o.customer_id, o.employee_id, o.order_date,
                       o.required_date, o.shipped_date, o.ship_via, o.freight,
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
                        (order_id, customer_id, employee_id, order_date, required_date, shipped_date,
                         ship_via, freight, product_id, quantity, unit_price, discount)
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
            cur.execute("SELECT COALESCE(MAX(id_compra_origem::int), 0) FROM corporativo.compras WHERE sistema_origem = 'Mercearia'")
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


# ==================================================================
# FASE 2 — corporativo: só as chaves naturais que apareceram na staging
# ==================================================================


def atualizar_corporativo_enderecos():
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

        n = 0
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
                enderecos_mercearia = cur.fetchall()
            with pg_conn.cursor() as cur:
                for r in enderecos_mercearia:
                    tipo = "Residencial" if r["TIPO_ENDERECO"] == 1 else "Comercial"
                    id_cidade = resolver_geografia_mercearia(cur, r["NOME_ESTADO"], r["SIGLA"], r["NOME_CIDADE"], r["REGIAOCIDADE"])
                    upsert_simples(
                        cur, "enderecos", "id_endereco", ["sistema_origem", "id_endereco_origem"],
                        ["Mercearia", str(r["ID_ENDERECO"])],
                        ["tipo_endereco", "logradouro", "cep", "bairro", "id_cidade"],
                        (tipo, r["LOGRADOURO"], str(r["CEP"]), r["NOME_BAIRRO"], id_cidade),
                    )
                    n += 1

        if customer_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(customer_ids))
                cur.execute(f"SELECT customer_id, address, postal_code, region, city, country FROM customers WHERE customer_id IN ({fmt})", customer_ids)
                clientes_northwind = cur.fetchall()
            with pg_conn.cursor() as cur:
                for customer_id, address, postal_code, region, city, country in clientes_northwind:
                    id_cidade = resolver_geografia_northwind(cur, country, region, city)
                    upsert_simples(
                        cur, "enderecos", "id_endereco", ["sistema_origem", "id_endereco_origem"],
                        ["Northwind", customer_id],
                        ["tipo_endereco", "logradouro", "cep", "bairro", "id_cidade"],
                        (None, address, postal_code, None, id_cidade),
                    )
                    n += 1
        pg_conn.commit()
        print(f"corporativo.enderecos: {n} linhas processadas")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_corporativo_cliente():
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

        n = 0
        if pessoa_ids:
            with mysql_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(pessoa_ids))
                cur.execute(f"""
                    SELECT p.ID_PESSOA, p.NOME, p.SEXO, p.RENDA, p.ESTADO_CIVIL, p.DATA_NASCIMENTO,
                           pr.NOME_PROFISSAO
                    FROM Pessoas p JOIN Profissoes pr ON pr.ID_PROFISSAO = p.ID_PROFISSAO
                    WHERE p.ID_PESSOA IN ({fmt})
                """, pessoa_ids)
                pessoas = cur.fetchall()
                cur.execute(f"SELECT ID_PESSOA, DDD, TIPO FROM Telefones WHERE PREFERENCIAL = 1 AND ID_PESSOA IN ({fmt})", pessoa_ids)
                telefone_pref = {r["ID_PESSOA"]: r for r in cur.fetchall()}

            with pg_conn.cursor() as cur:
                for p in pessoas:
                    tel = telefone_pref.get(p["ID_PESSOA"])
                    id_profissao = get_or_create_by_nome(cur, "profissoes", "id_profissao", p["NOME_PROFISSAO"])
                    cur.execute(
                        "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Mercearia' AND id_endereco_origem = %s "
                        "AND id_endereco = (SELECT id_endereco FROM staging.stg_vendas_mercearia WHERE id_pessoa = %s LIMIT 1)",
                        (str(p["ID_PESSOA"]), p["ID_PESSOA"]),
                    )
                    r_end = cur.fetchone()
                    id_endereco = r_end[0] if r_end else None
                    novos = (
                        "Física", p["NOME"], p["SEXO"], faixa_renda(p["RENDA"]), p["ESTADO_CIVIL"],
                        p["DATA_NASCIMENTO"].year if p["DATA_NASCIMENTO"] else None,
                        id_profissao, id_endereco,
                        str(tel["DDD"]) if tel else None, TIPO_TELEFONE_MAP.get(tel["TIPO"]) if tel else None,
                    )
                    upsert_historizado(
                        cur, "clientes", "id_versao_cliente", "id_cliente", ["sistema_origem", "id_cliente_origem"],
                        ["Mercearia", str(p["ID_PESSOA"])],
                        ["tipo_pessoa", "nome", "sexo", "faixa_renda", "estado_civil", "ano_nascimento",
                         "id_profissao", "id_endereco", "ddd_telefone", "numero_telefone"],
                        novos,
                    )
                    n += 1

        if customer_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(customer_ids))
                cur.execute(f"SELECT customer_id, company_name, contact_title FROM customers WHERE customer_id IN ({fmt})", customer_ids)
                clientes_northwind = cur.fetchall()
            with pg_conn.cursor() as cur:
                for customer_id, company_name, contact_title in clientes_northwind:
                    cur.execute(
                        "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Northwind' AND id_endereco_origem = %s",
                        (customer_id,),
                    )
                    r_end = cur.fetchone()
                    id_endereco = r_end[0] if r_end else None
                    novos = ("Jurídica", company_name, None, None, None, None, None, id_endereco, None, None)
                    upsert_historizado(
                        cur, "clientes", "id_versao_cliente", "id_cliente", ["sistema_origem", "id_cliente_origem"],
                        ["Northwind", customer_id],
                        ["tipo_pessoa", "nome", "sexo", "faixa_renda", "estado_civil", "ano_nascimento",
                         "id_profissao", "id_endereco", "ddd_telefone", "numero_telefone"],
                        novos,
                    )
                    n += 1
        pg_conn.commit()
        print(f"corporativo.clientes: {n} linhas processadas")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_corporativo_produtos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_produto FROM staging.stg_vendas_mercearia
                UNION SELECT id_produto FROM staging.stg_compras_mercearia
            """)
            produtos_mercearia_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT product_id FROM staging.stg_vendas_northwind")
            produtos_northwind_ids = [r[0] for r in cur.fetchall()]

        n = 0
        if produtos_mercearia_ids:
            with mysql_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(produtos_mercearia_ids))
                cur.execute(f"""
                    SELECT pr.ID_PRODUTO, pr.PRODUTO, pr.VALOR_VENDA, c.NOME_CATEGORIA
                    FROM Produtos pr JOIN Categorias c ON c.ID_CATEGORIA = pr.ID_CATEGORIA
                    WHERE pr.ID_PRODUTO IN ({fmt})
                """, produtos_mercearia_ids)
                produtos_mercearia = cur.fetchall()
            with pg_conn.cursor() as cur:
                for r in produtos_mercearia:
                    id_categoria = get_or_create_by_nome(cur, "categorias", "id_categoria", r["NOME_CATEGORIA"])
                    upsert_historizado(
                        cur, "produtos", "id_versao_produto", "id_produto", ["sistema_origem", "id_produto_origem"],
                        ["Mercearia", str(r["ID_PRODUTO"])],
                        ["nome", "valor", "moeda", "id_categoria"],
                        (r["PRODUTO"], r["VALOR_VENDA"], "BRL", id_categoria),
                    )
                    n += 1

        if produtos_northwind_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(produtos_northwind_ids))
                cur.execute(f"""
                    SELECT p.product_id, p.product_name, p.unit_price, c.category_name
                    FROM products p JOIN categories c ON c.category_id = p.category_id
                    WHERE p.product_id IN ({fmt})
                """, produtos_northwind_ids)
                produtos_northwind = cur.fetchall()
            with pg_conn.cursor() as cur:
                for product_id, product_name, unit_price, category_name in produtos_northwind:
                    id_categoria = get_or_create_by_nome(cur, "categorias", "id_categoria", category_name)
                    upsert_historizado(
                        cur, "produtos", "id_versao_produto", "id_produto", ["sistema_origem", "id_produto_origem"],
                        ["Northwind", str(product_id)],
                        ["nome", "valor", "moeda", "id_categoria"],
                        (product_name, unit_price, "USD", id_categoria),
                    )
                    n += 1
        pg_conn.commit()
        print(f"corporativo.produtos: {n} linhas processadas")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def atualizar_corporativo_funcionario():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT employee_id FROM staging.stg_vendas_northwind WHERE employee_id IS NOT NULL")
            employee_ids = [r[0] for r in cur.fetchall()]

        n = 0
        if employee_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(employee_ids))
                cur.execute(f"""
                    SELECT employee_id, first_name, last_name, title, hire_date,
                           address, city, region, postal_code, country
                    FROM employees WHERE employee_id IN ({fmt})
                """, employee_ids)
                employees = cur.fetchall()
            with pg_conn.cursor() as cur:
                for eid, first, last, title, hire_date, address, city, region, postal_code, country in employees:
                    id_cidade = resolver_geografia_northwind(cur, country, region, city)
                    id_endereco = upsert_simples(
                        cur, "enderecos", "id_endereco", ["sistema_origem", "id_endereco_origem"],
                        ["Northwind", f"EMP-{eid}"],
                        ["tipo_endereco", "logradouro", "cep", "bairro", "id_cidade"],
                        ("Comercial", address, postal_code, None, id_cidade),
                    )
                    id_cargo = get_or_create_by_nome(cur, "cargos", "id_cargo", title) if title else None
                    upsert_historizado(
                        cur, "funcionarios", "id_versao_funcionario", "id_funcionario", ["sistema_origem", "id_funcionario_origem"],
                        ["Northwind", str(eid)],
                        ["nome", "data_contratacao", "id_cargo", "id_endereco"],
                        (f"{first} {last}", hire_date, id_cargo, id_endereco),
                    )
                    n += 1
        pg_conn.commit()
        print(f"corporativo.funcionarios: {n} linhas processadas")
    finally:
        nw_conn.close()
        pg_conn.close()


def atualizar_corporativo_transportadora():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ship_via FROM staging.stg_vendas_northwind WHERE ship_via IS NOT NULL")
            ship_ids = [r[0] for r in cur.fetchall()]

        n = 0
        if ship_ids:
            with nw_conn.cursor() as cur:
                fmt = ",".join(["%s"] * len(ship_ids))
                cur.execute(f"SELECT shipper_id, company_name, phone FROM shippers WHERE shipper_id IN ({fmt})", ship_ids)
                shippers = cur.fetchall()
            with pg_conn.cursor() as cur:
                for sid, name, phone in shippers:
                    upsert_simples(
                        cur, "transportadoras", "id_transportadora", ["sistema_origem", "id_transportadora_origem"],
                        ["Northwind", str(sid)], ["nome", "telefone"], (name, phone),
                    )
                    n += 1
        pg_conn.commit()
        print(f"corporativo.transportadoras: {n} linhas processadas")
    finally:
        nw_conn.close()
        pg_conn.close()


def atualizar_corporativo_fornecedor():
    """A pessoa que aparece como contraparte de uma compra nova é materializada
    como Fornecedor (a Mercearia não tem tabela própria pra isso — ver
    carregar_corporativo_fornecedor da carga inicial)."""
    mysql_conn = get_mysql_conn()
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT id_pessoa FROM staging.stg_compras_mercearia")
            pessoa_ids = [r[0] for r in cur.fetchall()]
        if not pessoa_ids:
            print("corporativo.fornecedores: 0 linhas processadas")
            return

        with mysql_conn.cursor() as cur:
            fmt = ",".join(["%s"] * len(pessoa_ids))
            cur.execute(f"SELECT ID_PESSOA, NOME FROM Pessoas WHERE ID_PESSOA IN ({fmt})", pessoa_ids)
            pessoas = cur.fetchall()
            cur.execute(f"SELECT ID_PESSOA, DDD, TELEFONE FROM Telefones WHERE PREFERENCIAL = 1 AND ID_PESSOA IN ({fmt})", pessoa_ids)
            telefone_pref = {r["ID_PESSOA"]: r for r in cur.fetchall()}
            cur.execute(f"SELECT ID_PESSOA, ID_ENDERECO FROM Enderecos WHERE ID_PESSOA IN ({fmt})", pessoa_ids)
            endereco_pessoa = {r["ID_PESSOA"]: r["ID_ENDERECO"] for r in cur.fetchall()}

        with pg_conn.cursor() as cur:
            for p in pessoas:
                tel = telefone_pref.get(p["ID_PESSOA"])
                telefone = f"({tel['DDD']}) {tel['TELEFONE']}" if tel else None
                cur.execute(
                    "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Mercearia' AND id_endereco_origem = %s",
                    (str(endereco_pessoa.get(p["ID_PESSOA"])),),
                )
                r_end = cur.fetchone()
                id_endereco = r_end[0] if r_end else None
                upsert_simples(
                    cur, "fornecedores", "id_fornecedor", ["sistema_origem", "id_fornecedor_origem"],
                    ["Mercearia", str(p["ID_PESSOA"])], ["nome", "telefone", "id_endereco"],
                    (p["NOME"], telefone, id_endereco),
                )
        pg_conn.commit()
        print(f"corporativo.fornecedores: {len(pessoas)} linhas processadas")
    finally:
        mysql_conn.close()
        pg_conn.close()


# ----------------------------------------------------------------
# Fatos do corporativo: append-only, direto da staging (que já é o
# delta) — mesma função usada na carga inicial.
# ----------------------------------------------------------------

def carregar_corporativo_vendas():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_venda, id_pessoa, id_endereco, data_venda, tipo_venda,
                       id_itemvenda, id_produto, quantidade, vlr_unitario
                FROM staging.stg_vendas_mercearia
            """)
            staged_mercearia = cur.fetchall()
            cur.execute("""
                SELECT order_id, customer_id, employee_id, order_date, required_date, shipped_date,
                       ship_via, freight, product_id, quantity, unit_price, discount
                FROM staging.stg_vendas_northwind
            """)
            staged_northwind = cur.fetchall()

            venda_id_cache = {}
            n = 0
            for (id_venda, id_pessoa, id_endereco, data_venda, tipo_venda,
                 id_itemvenda, id_produto, quantidade, vlr_unitario) in staged_mercearia:
                iv = inserir_corporativo_venda_mercearia(cur, id_venda, id_pessoa, data_venda, tipo_venda, venda_id_cache)
                inserir_corporativo_item_venda(cur, "Mercearia", iv, str(id_itemvenda), id_produto, quantidade, vlr_unitario)
                n += 1

            for (order_id, customer_id, employee_id, order_date, required_date, shipped_date,
                 ship_via, freight, product_id, quantity, unit_price, discount) in staged_northwind:
                iv = inserir_corporativo_venda_northwind(
                    cur, order_id, customer_id, employee_id, order_date, ship_via, venda_id_cache,
                    required_date=required_date, shipped_date=shipped_date, freight=freight,
                )
                inserir_corporativo_item_venda(cur, "Northwind", iv, None, product_id, quantity, unit_price, float(unit_price) * float(quantity) * float(discount))
                n += 1
        pg_conn.commit()
        print(f"corporativo.vendas/item_venda: {n} itens novos inseridos")
    finally:
        pg_conn.close()


def carregar_corporativo_compras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id_compra, id_pessoa, id_endereco, data_pedido, data_entrada,
                       id_itemcompra, id_produto, quantidade, vlr_unitario
                FROM staging.stg_compras_mercearia
            """)
            staged = cur.fetchall()

            compra_id_cache = {}
            for (id_compra, id_pessoa, id_endereco, data_pedido, data_entrada,
                 id_itemcompra, id_produto, quantidade, vlr_unitario) in staged:
                ic = inserir_corporativo_compra(cur, id_compra, id_pessoa, data_pedido, data_entrada, compra_id_cache)
                inserir_corporativo_item_compra(cur, ic, str(id_itemcompra), id_produto, quantidade, vlr_unitario)
        pg_conn.commit()
        print(f"corporativo.compras/item_compra: {len(staged)} itens novos inseridos")
    finally:
        pg_conn.close()


# ==================================================================
# FASE 3 — data_marting: dims a partir do corporativo, fatos a partir
# da staging (delta) resolvendo pra chave substituta do dim.
# ==================================================================


def atualizar_marting_dim_clientes():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT 'Mercearia', id_pessoa::text FROM staging.stg_vendas_mercearia
                UNION SELECT DISTINCT 'Mercearia', id_pessoa::text FROM staging.stg_compras_mercearia
                UNION SELECT DISTINCT 'Northwind', customer_id FROM staging.stg_vendas_northwind
            """)
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_clientes(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_clientes: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_produtos():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT 'Mercearia', id_produto::text FROM staging.stg_vendas_mercearia
                UNION SELECT DISTINCT 'Mercearia', id_produto::text FROM staging.stg_compras_mercearia
                UNION SELECT DISTINCT 'Northwind', product_id::text FROM staging.stg_vendas_northwind
            """)
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_produtos(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_produtos: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_enderecos():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT 'Mercearia', id_endereco::text FROM staging.stg_vendas_mercearia
                UNION SELECT DISTINCT 'Mercearia', id_endereco::text FROM staging.stg_compras_mercearia
                UNION SELECT DISTINCT 'Northwind', customer_id FROM staging.stg_vendas_northwind
            """)
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_enderecos(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_enderecos: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_funcionarios():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT 'Northwind', employee_id::text FROM staging.stg_vendas_northwind WHERE employee_id IS NOT NULL")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_funcionarios(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_funcionarios: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_transportadoras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT 'Northwind', ship_via::text FROM staging.stg_vendas_northwind WHERE ship_via IS NOT NULL")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_transportadoras(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_transportadoras: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def atualizar_marting_dim_fornecedores():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT 'Mercearia', id_pessoa::text FROM staging.stg_compras_mercearia")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_fornecedores(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_fornecedores: {len(chaves)} chaves atualizadas")
    finally:
        pg_conn.close()


def carregar_marting_fato_vendas():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT data, id_dim_tempo FROM dim_tempos")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_cliente_original, id_dim_cliente FROM dim_clientes")
            cliente_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_produto_original, id_dim_produto FROM dim_produtos")
            produto_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_endereco_original, id_dim_endereco FROM dim_enderecos")
            endereco_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_funcionario_original, id_dim_funcionario FROM dim_funcionarios")
            funcionario_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_transportadora_original, id_dim_transportadora FROM dim_transportadoras")
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
                None, quantidade, vlr_unitario, 0, valor_total,
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
                None, quantity, unit_price, discount, valor_total,
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


def carregar_marting_fato_compras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT data, id_dim_tempo FROM dim_tempos")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_produto_original, id_dim_produto FROM dim_produtos WHERE sistema_origem = 'Mercearia'")
            produto_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_endereco_original, id_dim_endereco FROM dim_enderecos WHERE sistema_origem = 'Mercearia'")
            endereco_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_fornecedor_original, id_dim_fornecedor FROM dim_fornecedores WHERE sistema_origem = 'Mercearia'")
            fornecedor_map = {r[0]: r[1] for r in cur.fetchall()}

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
                fornecedor_map.get(str(id_pessoa)),
                produto_map.get(str(id_produto)),
                endereco_map.get(str(id_endereco)),
                quantidade, vlr_unitario, valor_total, lead_time,
            ))

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_compras
                        (sistema_origem, id_compra_original, id_item_original, id_dim_tempo, id_dim_fornecedor,
                         id_dim_produto, id_dim_endereco, quantidade, valor_unitario, valor_total_item, lead_time_dias)
                    VALUES %s
                """, rows)
            pg_conn.commit()
        print(f"fato_compras: {len(rows)} linhas novas inseridas")
    finally:
        pg_conn.close()


def carregar_marting_fato_entregas():
    """Mart Logística/Entregas: grão de pedido (não item) — agrega
    staging.stg_vendas_northwind por order_id antes de inserir. Exclusivo do
    Northwind, mesma decisão de modelagem da carga inicial."""
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT data, id_dim_tempo FROM dim_tempos")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_cliente_original, id_dim_cliente FROM dim_clientes WHERE sistema_origem = 'Northwind'")
            cliente_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_funcionario_original, id_dim_funcionario FROM dim_funcionarios WHERE sistema_origem = 'Northwind'")
            funcionario_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_transportadora_original, id_dim_transportadora FROM dim_transportadoras WHERE sistema_origem = 'Northwind'")
            transportadora_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT id_endereco_original, id_dim_endereco FROM dim_enderecos WHERE sistema_origem = 'Northwind'")
            endereco_map = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("""
                SELECT order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight,
                       SUM(quantity * unit_price * (1 - discount)) AS valor_pedido
                FROM staging.stg_vendas_northwind
                GROUP BY order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight
            """)
            pedidos = cur.fetchall()

        rows = []
        for (order_id, customer_id, employee_id, order_date, required_date, shipped_date,
             ship_via, freight, valor_pedido) in pedidos:
            prazo_dias = (required_date - order_date).days if required_date else None
            dias_para_envio = (shipped_date - order_date).days if shipped_date else None
            atraso_dias = (shipped_date - required_date).days if (shipped_date and required_date) else None
            rows.append((
                "Northwind", str(order_id),
                tempo_map.get(order_date),
                tempo_map.get(shipped_date) if shipped_date else None,
                cliente_map.get(customer_id),
                funcionario_map.get(str(employee_id), -1) if employee_id else -1,
                transportadora_map.get(str(ship_via), -1) if ship_via else -1,
                endereco_map.get(customer_id),
                valor_pedido, freight, prazo_dias, dias_para_envio, atraso_dias,
            ))

        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO fato_entregas
                        (sistema_origem, id_venda_original, id_dim_tempo_pedido, id_dim_tempo_envio,
                         id_dim_cliente, id_dim_funcionario, id_dim_transportadora, id_dim_endereco,
                         valor_pedido, valor_frete, prazo_dias, dias_para_envio, atraso_dias)
                    VALUES %s
                """, rows)
            pg_conn.commit()
        print(f"fato_entregas: {len(rows)} pedidos novos inseridos")
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
    description="Carga incremental: staging (watermark na fato do corporativo) -> corporativo (histórico) -> data_marting (estrela)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-incremental"],
) as dag:

    t_stage_vendas_mercearia = PythonOperator(task_id="stage_vendas_mercearia", python_callable=stage_vendas_mercearia)
    t_stage_vendas_northwind = PythonOperator(task_id="stage_vendas_northwind", python_callable=stage_vendas_northwind)
    t_stage_compras_mercearia = PythonOperator(task_id="stage_compras_mercearia", python_callable=stage_compras_mercearia)

    t_corp_enderecos = PythonOperator(task_id="atualizar_corporativo_enderecos", python_callable=atualizar_corporativo_enderecos)
    t_corp_cliente = PythonOperator(task_id="atualizar_corporativo_cliente", python_callable=atualizar_corporativo_cliente)
    t_corp_produtos = PythonOperator(task_id="atualizar_corporativo_produtos", python_callable=atualizar_corporativo_produtos)
    t_corp_funcionario = PythonOperator(task_id="atualizar_corporativo_funcionario", python_callable=atualizar_corporativo_funcionario)
    t_corp_transportadora = PythonOperator(task_id="atualizar_corporativo_transportadora", python_callable=atualizar_corporativo_transportadora)
    t_corp_fornecedor = PythonOperator(task_id="atualizar_corporativo_fornecedor", python_callable=atualizar_corporativo_fornecedor)

    t_corp_vendas = PythonOperator(task_id="carregar_corporativo_vendas", python_callable=carregar_corporativo_vendas)
    t_corp_compras = PythonOperator(task_id="carregar_corporativo_compras", python_callable=carregar_corporativo_compras)

    t_mart_dim_clientes = PythonOperator(task_id="atualizar_marting_dim_clientes", python_callable=atualizar_marting_dim_clientes)
    t_mart_dim_produtos = PythonOperator(task_id="atualizar_marting_dim_produtos", python_callable=atualizar_marting_dim_produtos)
    t_mart_dim_enderecos = PythonOperator(task_id="atualizar_marting_dim_enderecos", python_callable=atualizar_marting_dim_enderecos)
    t_mart_dim_funcionarios = PythonOperator(task_id="atualizar_marting_dim_funcionarios", python_callable=atualizar_marting_dim_funcionarios)
    t_mart_dim_transportadoras = PythonOperator(task_id="atualizar_marting_dim_transportadoras", python_callable=atualizar_marting_dim_transportadoras)
    t_mart_dim_fornecedores = PythonOperator(task_id="atualizar_marting_dim_fornecedores", python_callable=atualizar_marting_dim_fornecedores)

    t_mart_fato_vendas = PythonOperator(task_id="carregar_marting_fato_vendas", python_callable=carregar_marting_fato_vendas)
    t_mart_fato_compras = PythonOperator(task_id="carregar_marting_fato_compras", python_callable=carregar_marting_fato_compras)
    t_mart_fato_entregas = PythonOperator(task_id="carregar_marting_fato_entregas", python_callable=carregar_marting_fato_entregas)

    t_limpar_staging = PythonOperator(task_id="limpar_staging", python_callable=limpar_staging)

    staging_tasks = [t_stage_vendas_mercearia, t_stage_vendas_northwind, t_stage_compras_mercearia]

    # geografia (enderecos) primeiro, pra não disputar pais/estado/cidade
    # com funcionario/fornecedor em paralelo; os dois primeiro que cliente
    # e fornecedor porque criam/dependem de endereco.
    cross_downstream(staging_tasks, [t_corp_enderecos, t_corp_produtos, t_corp_transportadora])
    t_corp_enderecos >> [t_corp_cliente, t_corp_funcionario, t_corp_fornecedor]

    [t_corp_cliente, t_corp_produtos, t_corp_funcionario, t_corp_transportadora] >> t_corp_vendas
    [t_corp_fornecedor, t_corp_produtos] >> t_corp_compras

    mart_dims = [t_mart_dim_clientes, t_mart_dim_produtos, t_mart_dim_enderecos,
                 t_mart_dim_funcionarios, t_mart_dim_transportadoras, t_mart_dim_fornecedores]
    cross_downstream([t_corp_vendas, t_corp_compras], mart_dims)
    cross_downstream(mart_dims, [t_mart_fato_vendas, t_mart_fato_compras, t_mart_fato_entregas])
    [t_mart_fato_vendas, t_mart_fato_compras, t_mart_fato_entregas] >> t_limpar_staging
