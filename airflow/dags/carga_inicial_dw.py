"""Primeira carga do Data Warehouse.

Pipeline em duas camadas:
1. corporativo (DW normalizado, Inmon): lê Mercearia (MySQL) e Northwind
   (Postgres) direto, resolve geografia/lookups e grava histórico em
   cliente/funcionario/produto.
2. data_marting (estrela, Kimball): lê só o corporativo — nunca a origem —
   e monta dim_*/fato_* para consulta de BI.
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
# FASE 1 — corporativo (DW normalizado)
# ==================================================================


def carregar_corporativo_enderecos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
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
            enderecos_mercearia = cur.fetchall()

        with nw_conn.cursor() as cur:
            cur.execute("SELECT customer_id, address, postal_code, region, city, country FROM customers")
            clientes_northwind = cur.fetchall()

        n = 0
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


def carregar_corporativo_produtos():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT pr.ID_PRODUTO, pr.PRODUTO, pr.VALOR_VENDA, c.NOME_CATEGORIA
                FROM Produtos pr
                JOIN Categorias c ON c.ID_CATEGORIA = pr.ID_CATEGORIA
            """)
            produtos_mercearia = cur.fetchall()

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT p.product_id, p.product_name, p.unit_price, c.category_name
                FROM products p JOIN categories c ON c.category_id = p.category_id
            """)
            produtos_northwind = cur.fetchall()

        with pg_conn.cursor() as cur:
            for r in produtos_mercearia:
                id_categoria = get_or_create_by_nome(cur, "categorias", "id_categoria", r["NOME_CATEGORIA"])
                upsert_historizado(
                    cur, "produtos", "id_produto", ["sistema_origem", "id_produto_origem"],
                    ["Mercearia", str(r["ID_PRODUTO"])],
                    ["nome", "valor", "moeda", "id_categoria"],
                    (r["PRODUTO"], r["VALOR_VENDA"], "BRL", id_categoria),
                )
            for product_id, product_name, unit_price, category_name in produtos_northwind:
                id_categoria = get_or_create_by_nome(cur, "categorias", "id_categoria", category_name)
                upsert_historizado(
                    cur, "produtos", "id_produto", ["sistema_origem", "id_produto_origem"],
                    ["Northwind", str(product_id)],
                    ["nome", "valor", "moeda", "id_categoria"],
                    (product_name, unit_price, "USD", id_categoria),
                )
        pg_conn.commit()
        print(f"corporativo.produtos: {len(produtos_mercearia) + len(produtos_northwind)} linhas processadas")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_corporativo_cliente():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT p.ID_PESSOA, p.NOME, p.SEXO, p.RENDA, p.ESTADO_CIVIL, p.DATA_NASCIMENTO,
                       pr.NOME_PROFISSAO
                FROM Pessoas p JOIN Profissoes pr ON pr.ID_PROFISSAO = p.ID_PROFISSAO
            """)
            pessoas = cur.fetchall()
            cur.execute("SELECT ID_PESSOA, DDD, TIPO FROM Telefones WHERE PREFERENCIAL = 1")
            telefone_pref = {r["ID_PESSOA"]: r for r in cur.fetchall()}
            cur.execute("SELECT ID_PESSOA, ID_ENDERECO FROM Enderecos")
            endereco_pessoa = {r["ID_PESSOA"]: r["ID_ENDERECO"] for r in cur.fetchall()}

        with nw_conn.cursor() as cur:
            cur.execute("SELECT customer_id, company_name, contact_title FROM customers")
            clientes_northwind = cur.fetchall()

        with pg_conn.cursor() as cur:
            for p in pessoas:
                tel = telefone_pref.get(p["ID_PESSOA"])
                id_profissao = get_or_create_by_nome(cur, "profissoes", "id_profissao", p["NOME_PROFISSAO"])
                cur.execute(
                    "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Mercearia' AND id_endereco_origem = %s",
                    (str(endereco_pessoa.get(p["ID_PESSOA"])),),
                )
                r = cur.fetchone()
                id_endereco = r[0] if r else None
                novos = (
                    "Física", p["NOME"], p["SEXO"], faixa_renda(p["RENDA"]), p["ESTADO_CIVIL"],
                    p["DATA_NASCIMENTO"].year if p["DATA_NASCIMENTO"] else None,
                    id_profissao, id_endereco,
                    str(tel["DDD"]) if tel else None, TIPO_TELEFONE_MAP.get(tel["TIPO"]) if tel else None,
                )
                upsert_historizado(
                    cur, "clientes", "id_cliente", ["sistema_origem", "id_cliente_origem"],
                    ["Mercearia", str(p["ID_PESSOA"])],
                    ["tipo_pessoa", "nome", "sexo", "faixa_renda", "estado_civil", "ano_nascimento",
                     "id_profissao", "id_endereco", "ddd_telefone", "numero_telefone"],
                    novos,
                )

            for customer_id, company_name, contact_title in clientes_northwind:
                cur.execute(
                    "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Northwind' AND id_endereco_origem = %s",
                    (customer_id,),
                )
                r = cur.fetchone()
                id_endereco = r[0] if r else None
                novos = (
                    "Jurídica", company_name, None, None, None, None,
                    None, id_endereco, None, None,
                )
                upsert_historizado(
                    cur, "clientes", "id_cliente", ["sistema_origem", "id_cliente_origem"],
                    ["Northwind", customer_id],
                    ["tipo_pessoa", "nome", "sexo", "faixa_renda", "estado_civil", "ano_nascimento",
                     "id_profissao", "id_endereco", "ddd_telefone", "numero_telefone"],
                    novos,
                )
        pg_conn.commit()
        print(f"corporativo.clientes: {len(pessoas) + len(clientes_northwind)} linhas processadas")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_corporativo_funcionario():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT employee_id, first_name, last_name, title, hire_date,
                       address, city, region, postal_code, country
                FROM employees
            """)
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
                    cur, "funcionarios", "id_funcionario", ["sistema_origem", "id_funcionario_origem"],
                    ["Northwind", str(eid)],
                    ["nome", "data_contratacao", "id_cargo", "id_endereco"],
                    (f"{first} {last}", hire_date, id_cargo, id_endereco),
                )
        pg_conn.commit()
        print(f"corporativo.funcionarios: {len(employees)} linhas processadas")
    finally:
        nw_conn.close()
        pg_conn.close()


def carregar_corporativo_transportadora():
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with nw_conn.cursor() as cur:
            cur.execute("SELECT shipper_id, company_name, phone FROM shippers")
            shippers = cur.fetchall()
        with pg_conn.cursor() as cur:
            for sid, name, phone in shippers:
                upsert_simples(
                    cur, "transportadoras", "id_transportadora", ["sistema_origem", "id_transportadora_origem"],
                    ["Northwind", str(sid)], ["nome", "telefone"], (name, phone),
                )
        pg_conn.commit()
        print(f"corporativo.transportadoras: {len(shippers)} linhas processadas")
    finally:
        nw_conn.close()
        pg_conn.close()


def carregar_corporativo_fornecedor():
    """
    A Mercearia não tem uma tabela própria de fornecedor — Compras usa
    ID_PESSOA, a mesma tabela dos clientes de Vendas. O corporativo separa
    os dois papéis: aqui a pessoa que aparece como contraparte de uma
    compra é materializada como Fornecedor, não como Cliente.
    """
    mysql_conn = get_mysql_conn()
    pg_conn = get_pg_conn("dw")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT ID_PESSOA FROM Compras")
            pessoa_ids = [r["ID_PESSOA"] for r in cur.fetchall()]
            if not pessoa_ids:
                return
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
                r = cur.fetchone()
                id_endereco = r[0] if r else None
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


def carregar_corporativo_vendas():
    mysql_conn = get_mysql_conn()
    nw_conn = get_pg_conn("northwind")
    pg_conn = get_pg_conn("dw")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT v.ID_VENDA, v.ID_PESSOA, v.DATA_VENDA, v.TIPO_VENDA,
                       iv.ID_ITEMVENDA, iv.ID_PRODUTO, iv.QUANTIDADE, iv.VLR_UNITARIO
                FROM Vendas v JOIN Itens_Vendas iv ON iv.ID_VENDA = v.ID_VENDA
            """)
            vendas_mercearia = cur.fetchall()

        with nw_conn.cursor() as cur:
            cur.execute("""
                SELECT o.order_id, o.customer_id, o.employee_id, o.order_date, o.ship_via,
                       od.product_id, od.quantity, od.unit_price, od.discount
                FROM orders o JOIN order_details od ON od.order_id = o.order_id
            """)
            vendas_northwind = cur.fetchall()

        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM corporativo.itens_vendas")
            cur.execute("DELETE FROM corporativo.vendas")

            venda_id_cache = {}  # (sistema_origem, id_venda_origem) -> id_venda

            for r in vendas_mercearia:
                id_venda = inserir_corporativo_venda_mercearia(
                    cur, r["ID_VENDA"], r["ID_PESSOA"], r["DATA_VENDA"], r["TIPO_VENDA"], venda_id_cache,
                )
                inserir_corporativo_item_venda(cur, id_venda, str(r["ID_ITEMVENDA"]), r["ID_PRODUTO"], r["QUANTIDADE"], r["VLR_UNITARIO"])

            for order_id, customer_id, employee_id, order_date, ship_via, product_id, quantity, unit_price, discount in vendas_northwind:
                id_venda = inserir_corporativo_venda_northwind(
                    cur, order_id, customer_id, employee_id, order_date, ship_via, venda_id_cache,
                )
                inserir_corporativo_item_venda(
                    cur, id_venda, None, product_id, quantity, unit_price,
                    float(unit_price) * float(quantity) * float(discount),
                )
        pg_conn.commit()
        print(f"corporativo.vendas/item_venda: {len(vendas_mercearia) + len(vendas_northwind)} itens processados")
    finally:
        mysql_conn.close()
        nw_conn.close()
        pg_conn.close()


def carregar_corporativo_compras():
    mysql_conn = get_mysql_conn()
    pg_conn = get_pg_conn("dw")
    try:
        with mysql_conn.cursor() as cur:
            cur.execute("""
                SELECT c.ID_COMPRA, c.ID_PESSOA, c.DATA_PEDIDO, c.DATA_ENTRADA,
                       ic.ID_ITEMCOMPRA, ic.ID_PRODUTO, ic.QUANTIDADE, ic.VLR_UNITARIO
                FROM Compras c JOIN Itens_compras ic ON ic.ID_COMPRA = c.ID_COMPRA
            """)
            compras = cur.fetchall()

        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM corporativo.itens_compras")
            cur.execute("DELETE FROM corporativo.compras")

            compra_id_cache = {}

            for r in compras:
                id_compra = inserir_corporativo_compra(
                    cur, r["ID_COMPRA"], r["ID_PESSOA"], r["DATA_PEDIDO"], r["DATA_ENTRADA"], compra_id_cache,
                )
                inserir_corporativo_item_compra(cur, id_compra, str(r["ID_ITEMCOMPRA"]), r["ID_PRODUTO"], r["QUANTIDADE"], r["VLR_UNITARIO"])
        pg_conn.commit()
        print(f"corporativo.compras/item_compra: {len(compras)} itens processados")
    finally:
        mysql_conn.close()
        pg_conn.close()


# ==================================================================
# FASE 2 — data_marting (estrela), lida só do corporativo
# ==================================================================


def carregar_marting_dim_clientes():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_cliente_origem FROM corporativo.clientes")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_clientes(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_clientes: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_produtos():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_produto_origem FROM corporativo.produtos")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_produtos(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_produtos: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_enderecos():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_endereco_origem FROM corporativo.enderecos")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_enderecos(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_enderecos: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_funcionarios():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_funcionario_origem FROM corporativo.funcionarios WHERE id_funcionario_origem IS NOT NULL")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_funcionarios(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_funcionarios: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_transportadoras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_transportadora_origem FROM corporativo.transportadoras WHERE id_transportadora_origem IS NOT NULL")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_transportadoras(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_transportadoras: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_dim_fornecedores():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT DISTINCT sistema_origem, id_fornecedor_origem FROM corporativo.fornecedores WHERE id_fornecedor_origem IS NOT NULL")
            chaves = cur.fetchall()
            for sistema_origem, id_origem in chaves:
                refresh_dim_fornecedores(cur, sistema_origem, id_origem)
        pg_conn.commit()
        print(f"dim_fornecedores: {len(chaves)} linhas")
    finally:
        pg_conn.close()


def carregar_marting_fato_vendas():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_vendas")

            cur.execute("SELECT t.id_tempo, dt.id_dim_tempo FROM corporativo.tempos t JOIN dim_tempos dt ON dt.data = t.data")
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

            cur.execute("SELECT id_cliente, sistema_origem, id_cliente_origem FROM corporativo.clientes")
            cliente_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_produto, sistema_origem, id_produto_origem FROM corporativo.produtos")
            produto_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_funcionario, sistema_origem, id_funcionario_origem FROM corporativo.funcionarios")
            funcionario_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_transportadora, sistema_origem, id_transportadora_origem FROM corporativo.transportadoras")
            transportadora_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_endereco, sistema_origem, id_endereco_origem FROM corporativo.enderecos")
            endereco_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

            cur.execute("""
                SELECT v.sistema_origem, v.id_venda_origem, iv.id_item_origem, v.id_tempo, v.tipo_venda,
                       v.id_cliente, v.id_funcionario, v.id_transportadora, v.id_endereco_entrega,
                       iv.id_produto, iv.quantidade, iv.valor_unitario, iv.valor_desconto
                FROM corporativo.vendas v JOIN corporativo.itens_vendas iv ON iv.id_venda = v.id_venda
            """)
            vendas = cur.fetchall()

        rows = []
        for (sistema_origem, id_venda_origem, id_item_origem, id_tempo, tipo_venda,
             id_cliente, id_funcionario, id_transportadora, id_endereco_entrega,
             id_produto, quantidade, valor_unitario, valor_desconto) in vendas:
            valor_total = float(quantidade) * float(valor_unitario) - float(valor_desconto or 0)
            end_nat = endereco_nat.get(id_endereco_entrega)
            rows.append((
                sistema_origem, id_venda_origem, id_item_origem,
                tempo_map.get(id_tempo),
                cliente_map.get(cliente_nat.get(id_cliente)),
                produto_map.get(produto_nat.get(id_produto)),
                endereco_map.get(end_nat) if end_nat else None,
                funcionario_map.get(funcionario_nat.get(id_funcionario), -1),
                transportadora_map.get(transportadora_nat.get(id_transportadora), -1),
                tipo_venda, quantidade, valor_unitario, valor_desconto, valor_total,
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
        print(f"fato_vendas: {len(rows)} linhas carregadas a partir do corporativo")
    finally:
        pg_conn.close()


def carregar_marting_fato_compras():
    pg_conn = get_pg_conn("dw")
    try:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM fato_compras")

            cur.execute("SELECT t.id_tempo, dt.id_dim_tempo FROM corporativo.tempos t JOIN dim_tempos dt ON dt.data = t.data")
            tempo_map = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_produto_original, id_dim_produto FROM dim_produtos")
            produto_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_endereco_original, id_dim_endereco FROM dim_enderecos")
            endereco_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}
            cur.execute("SELECT sistema_origem, id_fornecedor_original, id_dim_fornecedor FROM dim_fornecedores")
            fornecedor_map = {(r[0], r[1]): r[2] for r in cur.fetchall()}

            cur.execute("SELECT id_produto, sistema_origem, id_produto_origem FROM corporativo.produtos")
            produto_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_endereco, sistema_origem, id_endereco_origem FROM corporativo.enderecos")
            endereco_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            cur.execute("SELECT id_fornecedor, sistema_origem, id_fornecedor_origem FROM corporativo.fornecedores")
            fornecedor_nat = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

            cur.execute("""
                SELECT c.sistema_origem, c.id_compra_origem, ic.id_item_origem, c.id_tempo, c.lead_time_dias,
                       c.id_fornecedor, c.id_endereco, ic.id_produto, ic.quantidade, ic.valor_unitario
                FROM corporativo.compras c JOIN corporativo.itens_compras ic ON ic.id_compra = c.id_compra
            """)
            compras = cur.fetchall()

        rows = []
        for (sistema_origem, id_compra_origem, id_item_origem, id_tempo, lead_time_dias,
             id_fornecedor, id_endereco, id_produto, quantidade, valor_unitario) in compras:
            valor_total = float(quantidade) * float(valor_unitario)
            rows.append((
                sistema_origem, id_compra_origem, id_item_origem,
                tempo_map.get(id_tempo),
                fornecedor_map.get(fornecedor_nat.get(id_fornecedor)),
                produto_map.get(produto_nat.get(id_produto)),
                endereco_map.get(endereco_nat.get(id_endereco)),
                quantidade, valor_unitario, valor_total, lead_time_dias,
            ))

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO fato_compras
                    (sistema_origem, id_compra_original, id_item_original, id_dim_tempo, id_dim_fornecedor,
                     id_dim_produto, id_dim_endereco, quantidade, valor_unitario, valor_total_item, lead_time_dias)
                VALUES %s
            """, rows)
        pg_conn.commit()
        print(f"fato_compras: {len(rows)} linhas carregadas a partir do corporativo")
    finally:
        pg_conn.close()


with DAG(
    dag_id="carga_inicial_dw",
    description="Primeira carga: Mercearia/Northwind -> corporativo (DW normalizado, Inmon) -> data_marting (estrela, Kimball)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dw", "etl", "carga-inicial"],
) as dag:

    t_corp_enderecos = PythonOperator(task_id="carregar_corporativo_enderecos", python_callable=carregar_corporativo_enderecos)
    t_corp_produtos = PythonOperator(task_id="carregar_corporativo_produtos", python_callable=carregar_corporativo_produtos)
    t_corp_cliente = PythonOperator(task_id="carregar_corporativo_cliente", python_callable=carregar_corporativo_cliente)
    t_corp_funcionario = PythonOperator(task_id="carregar_corporativo_funcionario", python_callable=carregar_corporativo_funcionario)
    t_corp_transportadora = PythonOperator(task_id="carregar_corporativo_transportadora", python_callable=carregar_corporativo_transportadora)
    t_corp_fornecedor = PythonOperator(task_id="carregar_corporativo_fornecedor", python_callable=carregar_corporativo_fornecedor)
    t_corp_vendas = PythonOperator(task_id="carregar_corporativo_vendas", python_callable=carregar_corporativo_vendas)
    t_corp_compras = PythonOperator(task_id="carregar_corporativo_compras", python_callable=carregar_corporativo_compras)

    t_mart_dim_clientes = PythonOperator(task_id="carregar_marting_dim_clientes", python_callable=carregar_marting_dim_clientes)
    t_mart_dim_produtos = PythonOperator(task_id="carregar_marting_dim_produtos", python_callable=carregar_marting_dim_produtos)
    t_mart_dim_enderecos = PythonOperator(task_id="carregar_marting_dim_enderecos", python_callable=carregar_marting_dim_enderecos)
    t_mart_dim_funcionarios = PythonOperator(task_id="carregar_marting_dim_funcionarios", python_callable=carregar_marting_dim_funcionarios)
    t_mart_dim_transportadoras = PythonOperator(task_id="carregar_marting_dim_transportadoras", python_callable=carregar_marting_dim_transportadoras)
    t_mart_dim_fornecedores = PythonOperator(task_id="carregar_marting_dim_fornecedores", python_callable=carregar_marting_dim_fornecedores)
    t_mart_fato_vendas = PythonOperator(task_id="carregar_marting_fato_vendas", python_callable=carregar_marting_fato_vendas)
    t_mart_fato_compras = PythonOperator(task_id="carregar_marting_fato_compras", python_callable=carregar_marting_fato_compras)

    # geografia primeiro (endereco), depois quem depende dela; produtos e
    # transportadora são independentes. Sequência evita corrida nas
    # tabelas de lookup compartilhadas (pais/estado/cidade).
    t_corp_enderecos >> [t_corp_cliente, t_corp_funcionario, t_corp_fornecedor]
    t_corp_produtos
    t_corp_transportadora

    [t_corp_cliente, t_corp_produtos, t_corp_funcionario, t_corp_transportadora] >> t_corp_vendas
    [t_corp_fornecedor, t_corp_produtos] >> t_corp_compras

    mart_dims = [t_mart_dim_clientes, t_mart_dim_produtos, t_mart_dim_enderecos,
                 t_mart_dim_funcionarios, t_mart_dim_transportadoras, t_mart_dim_fornecedores]
    cross_downstream([t_corp_vendas, t_corp_compras], mart_dims)
    cross_downstream(mart_dims, [t_mart_fato_vendas, t_mart_fato_compras])
