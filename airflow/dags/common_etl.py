"""Helpers compartilhados pelas DAGs carga_inicial_dw e carga_incremental_dw.

Pipeline: Mercearia/Northwind -> staging -> corporativo (DW normalizado,
Inmon) -> data_marting (estrela, Kimball). Os dims/fatos do data_marting
nunca são lidos direto da origem — só do corporativo, para não bater duas
vezes nos bancos transacionais pela mesma linha.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg2
import pymysql

TIPO_VENDA_MAP = {1: "À vista", 2: "Prazo", 3: "Cartão"}
TIPO_TELEFONE_MAP = {1: "Residencial", 2: "Comercial", 3: "Celular"}


def _normalizar(v):
    # Postgres devolve NUMERIC como Decimal; MySQL devolve DOUBLE como float.
    # Decimal(15.99) == float(15.99) pode dar False por causa da representação
    # binária do float — sem isso, todo valor numérico "parece" ter mudado
    # numa reexecução com o mesmo dado, e o upsert historizado cria uma versão
    # nova por engano.
    if isinstance(v, float):
        return Decimal(str(v))
    return v


def _valores_iguais(atuais, novos):
    return tuple(_normalizar(v) for v in atuais) == tuple(_normalizar(v) for v in novos)


def get_id_tempo(cur, data):
    """corporativo.tempos cobre 1996-01-01 a 2035-12-31 (gerado no DDL) —
    qualquer data de Mercearia/Northwind cai nesse intervalo."""
    cur.execute("SELECT id_tempo FROM corporativo.tempos WHERE data = %s", (data,))
    return cur.fetchone()[0]


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


def faixa_etaria_por_ano(ano_nascimento):
    """Igual a faixa_etaria, mas a partir do ano (corporativo.clientes só guarda o ano, não a data cheia)."""
    if ano_nascimento is None:
        return None
    idade = date.today().year - ano_nascimento
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


# ----------------------------------------------------------------
# corporativo — lookups simples (existem ou não pela chave natural,
# não são historizados: Pais/Estado/Cidade/Categoria/Cargo/Profissao)
# ----------------------------------------------------------------

def get_or_create_by_nome(cur, tabela, id_col, nome):
    # INSERT ... ON CONFLICT DO NOTHING (em vez de SELECT-depois-INSERT) porque
    # tasks do Airflow rodam em paralelo e podem disputar a mesma linha nova
    # (ex: dois tasks resolvendo "Brasil" ao mesmo tempo).
    cur.execute(
        f"INSERT INTO corporativo.{tabela} (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING RETURNING {id_col}",
        (nome,),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute(f"SELECT {id_col} FROM corporativo.{tabela} WHERE nome = %s", (nome,))
    return cur.fetchone()[0]


def get_or_create_estado(cur, nome, sigla, id_pais):
    cur.execute(
        "INSERT INTO corporativo.estados (nome, sigla, id_pais) VALUES (%s, %s, %s) "
        "ON CONFLICT (id_pais, sigla) DO NOTHING RETURNING id_estado",
        (nome, sigla, id_pais),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_estado FROM corporativo.estados WHERE id_pais = %s AND sigla = %s", (id_pais, sigla))
    return cur.fetchone()[0]


def get_or_create_cidade(cur, nome, regiao, id_estado):
    cur.execute(
        "INSERT INTO corporativo.cidades (nome, regiao, id_estado) VALUES (%s, %s, %s) "
        "ON CONFLICT (id_estado, nome) DO NOTHING RETURNING id_cidade",
        (nome, regiao, id_estado),
    )
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("SELECT id_cidade FROM corporativo.cidades WHERE id_estado = %s AND nome = %s", (id_estado, nome))
    return cur.fetchone()[0]


def resolver_geografia_mercearia(cur, nome_estado, sigla, nome_cidade, regiao_cidade):
    id_pais = get_or_create_by_nome(cur, "paises", "id_pais", "Brasil")
    id_estado = get_or_create_estado(cur, nome_estado, sigla, id_pais)
    return get_or_create_cidade(cur, nome_cidade, regiao_cidade, id_estado)


def resolver_geografia_northwind(cur, country, region, city):
    # Northwind não tem tabelas de geografia próprias, só texto livre em
    # customers/employees — sigla/estado ficam aproximados.
    id_pais = get_or_create_by_nome(cur, "paises", "id_pais", country or "Não informado")
    sigla = (region or "NA")[:10]
    id_estado = get_or_create_estado(cur, region or "Não informado", sigla, id_pais)
    return get_or_create_cidade(cur, city or "Não informado", region, id_estado)


def upsert_simples(cur, tabela, id_col, natural_cols, natural_vals, cols, novos_valores):
    """Insere se a chave natural não existe; atualiza se algum atributo mudou. Retorna o id."""
    where = " AND ".join(f"{c} = %s" for c in natural_cols)
    cur.execute(f"SELECT {id_col}, {','.join(cols)} FROM corporativo.{tabela} WHERE {where}", natural_vals)
    atual = cur.fetchone()
    if atual is None:
        cur.execute(
            f"INSERT INTO corporativo.{tabela} ({','.join(natural_cols)}, {','.join(cols)}) "
            f"VALUES ({','.join(['%s'] * len(natural_vals))}, {','.join(['%s'] * len(cols))}) "
            f"RETURNING {id_col}",
            (*natural_vals, *novos_valores),
        )
        return cur.fetchone()[0]
    id_atual, *valores_atuais = atual
    if not _valores_iguais(valores_atuais, novos_valores):
        sets = ", ".join(f"{c} = %s" for c in cols)
        cur.execute(f"UPDATE corporativo.{tabela} SET {sets} WHERE {id_col} = %s", (*novos_valores, id_atual))
    return id_atual


def upsert_historizado(cur, tabela, id_col, natural_cols, natural_vals, cols, novos_valores):
    """
    Cliente/Funcionario/Produto: nunca sofrem UPDATE nos atributos versionados.
    - Chave natural nova: insere a primeira versão.
    - Já existe e mudou algum atributo: fecha a versão atual (flag_atual=false,
      data_fim_validade=ontem) e insere uma nova versão com o mesmo id natural.
    - Já existe e não mudou nada: não faz nada.
    Retorna o id natural (estável entre versões).

    Limitação conhecida: data_inicio_validade tem granularidade de dia, então
    duas mudanças reais de atributo no mesmo dia colidem na PK composta
    (id, data). Para este projeto (cargas em lote, não streaming) não é um
    cenário esperado.
    """
    where = " AND ".join(f"{c} = %s" for c in natural_cols)
    cur.execute(
        f"SELECT {id_col}, {','.join(cols)} FROM corporativo.{tabela} WHERE {where} AND flag_atual = TRUE",
        natural_vals,
    )
    atual = cur.fetchone()

    if atual is None:
        cur.execute(
            f"INSERT INTO corporativo.{tabela} ({','.join(natural_cols)}, {','.join(cols)}) "
            f"VALUES ({','.join(['%s'] * len(natural_vals))}, {','.join(['%s'] * len(cols))}) "
            f"RETURNING {id_col}",
            (*natural_vals, *novos_valores),
        )
        return cur.fetchone()[0]

    id_atual, *valores_atuais = atual
    if _valores_iguais(valores_atuais, novos_valores):
        return id_atual

    cur.execute(
        f"UPDATE corporativo.{tabela} SET flag_atual = FALSE, data_fim_validade = CURRENT_DATE - 1 "
        f"WHERE {id_col} = %s AND flag_atual = TRUE",
        (id_atual,),
    )
    cur.execute(
        f"INSERT INTO corporativo.{tabela} ({id_col}, {','.join(natural_cols)}, {','.join(cols)}) "
        f"VALUES (%s, {','.join(['%s'] * len(natural_vals))}, {','.join(['%s'] * len(cols))})",
        (id_atual, *natural_vals, *novos_valores),
    )
    return id_atual


# ----------------------------------------------------------------
# data_marting — os dims/fatos em estrela são sempre lidos do
# corporativo (nunca da origem), achatando o histórico na versão
# vigente (flag_atual=true) e a hierarquia de geografia num texto só.
# ----------------------------------------------------------------

def refresh_dim_clientes(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT c.tipo_pessoa, c.nome, c.sexo, c.faixa_renda, c.estado_civil, c.ano_nascimento,
               p.nome AS nome_profissao, c.ddd_telefone, c.numero_telefone
        FROM corporativo.clientes c
        LEFT JOIN corporativo.profissoes p ON p.id_profissao = c.id_profissao
        WHERE c.sistema_origem = %s AND c.id_cliente_origem = %s AND c.flag_atual = TRUE
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    tipo_pessoa, nome, sexo, faixa_renda_, estado_civil, ano_nascimento, nome_profissao, ddd, numero = r
    faixa_etaria_ = faixa_etaria_por_ano(ano_nascimento)
    cur.execute("""
        INSERT INTO dim_clientes
            (sistema_origem, id_cliente_original, tipo_pessoa, nome_cliente, sexo,
             faixa_renda, estado_civil, faixa_etaria, ano_nascimento, nome_profissao,
             ddd_principal, tipo_telefone)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_cliente_original) DO UPDATE SET
            tipo_pessoa = EXCLUDED.tipo_pessoa, nome_cliente = EXCLUDED.nome_cliente,
            sexo = EXCLUDED.sexo, faixa_renda = EXCLUDED.faixa_renda,
            estado_civil = EXCLUDED.estado_civil, faixa_etaria = EXCLUDED.faixa_etaria,
            ano_nascimento = EXCLUDED.ano_nascimento, nome_profissao = EXCLUDED.nome_profissao,
            ddd_principal = EXCLUDED.ddd_principal, tipo_telefone = EXCLUDED.tipo_telefone
    """, (sistema_origem, id_origem, tipo_pessoa, nome, sexo, faixa_renda_, estado_civil,
          faixa_etaria_, ano_nascimento, nome_profissao, ddd, numero))


def refresh_dim_produtos(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT pr.nome, cat.nome AS nome_categoria, pr.valor, pr.moeda
        FROM corporativo.produtos pr
        LEFT JOIN corporativo.categorias cat ON cat.id_categoria = pr.id_categoria
        WHERE pr.sistema_origem = %s AND pr.id_produto_origem = %s AND pr.flag_atual = TRUE
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    nome, nome_categoria, valor, moeda = r
    cur.execute("""
        INSERT INTO dim_produtos (sistema_origem, id_produto_original, nome_produto, nome_categoria, valor, moeda)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_produto_original) DO UPDATE SET
            nome_produto = EXCLUDED.nome_produto, nome_categoria = EXCLUDED.nome_categoria,
            valor = EXCLUDED.valor, moeda = EXCLUDED.moeda
    """, (sistema_origem, id_origem, nome, nome_categoria, valor, moeda))


def refresh_dim_enderecos(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT e.tipo_endereco, e.logradouro, e.cep, e.bairro, cid.regiao,
               cid.nome AS nome_cidade, est.nome AS nome_estado, est.sigla, pa.nome AS pais
        FROM corporativo.enderecos e
        JOIN corporativo.cidades cid ON cid.id_cidade = e.id_cidade
        JOIN corporativo.estados est ON est.id_estado = cid.id_estado
        JOIN corporativo.paises pa ON pa.id_pais = est.id_pais
        WHERE e.sistema_origem = %s AND e.id_endereco_origem = %s
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    tipo_endereco, logradouro, cep, bairro, regiao, nome_cidade, nome_estado, sigla, pais = r
    cur.execute("""
        INSERT INTO dim_enderecos
            (sistema_origem, id_endereco_original, tipo_endereco, nome_logradouro, cep,
             nome_bairro, regiao_cidade, nome_cidade, nome_estado, sigla_uf, pais)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_endereco_original) DO UPDATE SET
            tipo_endereco = EXCLUDED.tipo_endereco, nome_logradouro = EXCLUDED.nome_logradouro,
            cep = EXCLUDED.cep, nome_bairro = EXCLUDED.nome_bairro, regiao_cidade = EXCLUDED.regiao_cidade,
            nome_cidade = EXCLUDED.nome_cidade, nome_estado = EXCLUDED.nome_estado,
            sigla_uf = EXCLUDED.sigla_uf, pais = EXCLUDED.pais
    """, (sistema_origem, id_origem, tipo_endereco, logradouro, str(cep) if cep else None,
          bairro, regiao, nome_cidade, nome_estado, sigla, pais))


def refresh_dim_funcionarios(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT f.nome, car.nome AS cargo, f.data_contratacao, cid.nome AS cidade, pa.nome AS pais
        FROM corporativo.funcionarios f
        LEFT JOIN corporativo.cargos car ON car.id_cargo = f.id_cargo
        LEFT JOIN corporativo.enderecos e ON e.id_endereco = f.id_endereco
        LEFT JOIN corporativo.cidades cid ON cid.id_cidade = e.id_cidade
        LEFT JOIN corporativo.estados est ON est.id_estado = cid.id_estado
        LEFT JOIN corporativo.paises pa ON pa.id_pais = est.id_pais
        WHERE f.sistema_origem = %s AND f.id_funcionario_origem = %s AND f.flag_atual = TRUE
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    nome, cargo, data_contratacao, cidade, pais = r
    cur.execute("""
        INSERT INTO dim_funcionarios (sistema_origem, id_funcionario_original, nome_funcionario, cargo, data_contratacao, cidade, pais)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_funcionario_original) DO UPDATE SET
            nome_funcionario = EXCLUDED.nome_funcionario, cargo = EXCLUDED.cargo,
            data_contratacao = EXCLUDED.data_contratacao, cidade = EXCLUDED.cidade, pais = EXCLUDED.pais
    """, (sistema_origem, id_origem, nome, cargo, data_contratacao, cidade, pais))


def refresh_dim_transportadoras(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT nome, telefone FROM corporativo.transportadoras
        WHERE sistema_origem = %s AND id_transportadora_origem = %s
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    nome, telefone = r
    cur.execute("""
        INSERT INTO dim_transportadoras (sistema_origem, id_transportadora_original, nome_transportadora, telefone)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_transportadora_original) DO UPDATE SET
            nome_transportadora = EXCLUDED.nome_transportadora, telefone = EXCLUDED.telefone
    """, (sistema_origem, id_origem, nome, telefone))


def inserir_corporativo_venda_mercearia(cur, id_venda_origem, id_pessoa, data_venda, tipo_venda, venda_id_cache):
    """Devolve o id_venda do corporativo, criando o cabeçalho na 1ª vez que essa venda aparece."""
    chave = ("Mercearia", str(id_venda_origem))
    if chave not in venda_id_cache:
        cur.execute(
            "SELECT id_cliente, id_endereco FROM corporativo.clientes "
            "WHERE sistema_origem = 'Mercearia' AND id_cliente_origem = %s AND flag_atual = TRUE",
            (str(id_pessoa),),
        )
        id_cliente, id_endereco_cliente = cur.fetchone()
        id_tempo = get_id_tempo(cur, data_venda)
        cur.execute(
            "INSERT INTO corporativo.vendas "
            "(sistema_origem, id_venda_origem, id_tempo, tipo_venda, id_cliente, id_endereco_entrega) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING id_venda",
            ("Mercearia", str(id_venda_origem), id_tempo, TIPO_VENDA_MAP.get(tipo_venda), id_cliente, id_endereco_cliente),
        )
        venda_id_cache[chave] = cur.fetchone()[0]
    return venda_id_cache[chave]


def inserir_corporativo_venda_northwind(cur, order_id, customer_id, employee_id, order_date, ship_via, venda_id_cache):
    chave = ("Northwind", str(order_id))
    if chave not in venda_id_cache:
        cur.execute(
            "SELECT id_cliente FROM corporativo.clientes "
            "WHERE sistema_origem = 'Northwind' AND id_cliente_origem = %s AND flag_atual = TRUE",
            (customer_id,),
        )
        id_cliente = cur.fetchone()[0]
        cur.execute(
            "SELECT id_endereco FROM corporativo.enderecos WHERE sistema_origem = 'Northwind' AND id_endereco_origem = %s",
            (customer_id,),
        )
        r_end = cur.fetchone()
        id_endereco_entrega = r_end[0] if r_end else None

        id_funcionario = -1
        if employee_id:
            cur.execute(
                "SELECT id_funcionario FROM corporativo.funcionarios "
                "WHERE sistema_origem = 'Northwind' AND id_funcionario_origem = %s AND flag_atual = TRUE",
                (str(employee_id),),
            )
            r_func = cur.fetchone()
            id_funcionario = r_func[0] if r_func else -1

        id_transportadora = -1
        if ship_via:
            cur.execute(
                "SELECT id_transportadora FROM corporativo.transportadoras "
                "WHERE sistema_origem = 'Northwind' AND id_transportadora_origem = %s",
                (str(ship_via),),
            )
            r_transp = cur.fetchone()
            id_transportadora = r_transp[0] if r_transp else -1

        id_tempo = get_id_tempo(cur, order_date)
        cur.execute(
            "INSERT INTO corporativo.vendas "
            "(sistema_origem, id_venda_origem, id_tempo, id_cliente, id_funcionario, id_transportadora, id_endereco_entrega) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id_venda",
            ("Northwind", str(order_id), id_tempo, id_cliente, id_funcionario, id_transportadora, id_endereco_entrega),
        )
        venda_id_cache[chave] = cur.fetchone()[0]
    return venda_id_cache[chave]


def inserir_corporativo_item_venda(cur, id_venda, id_item_origem, id_produto, quantidade, valor_unitario, valor_desconto=0):
    cur.execute(
        "INSERT INTO corporativo.itens_vendas "
        "(id_item_origem, id_venda, id_produto, quantidade, valor_unitario, valor_desconto) "
        "VALUES (%s,%s,%s,%s,%s,%s)",
        (id_item_origem, id_venda, id_produto, quantidade, valor_unitario, valor_desconto),
    )


def inserir_corporativo_compra(cur, id_compra_origem, id_pessoa, data_pedido, data_entrada, compra_id_cache):
    chave = str(id_compra_origem)
    if chave not in compra_id_cache:
        cur.execute(
            "SELECT id_fornecedor, id_endereco FROM corporativo.fornecedores "
            "WHERE sistema_origem = 'Mercearia' AND id_fornecedor_origem = %s",
            (str(id_pessoa),),
        )
        id_fornecedor, id_endereco = cur.fetchone()
        lead_time = (data_entrada - data_pedido).days if data_entrada else None
        id_tempo = get_id_tempo(cur, data_pedido)
        cur.execute(
            "INSERT INTO corporativo.compras (id_compra_origem, id_tempo, lead_time_dias, id_fornecedor, id_endereco) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING id_compra",
            (str(id_compra_origem), id_tempo, lead_time, id_fornecedor, id_endereco),
        )
        compra_id_cache[chave] = cur.fetchone()[0]
    return compra_id_cache[chave]


def inserir_corporativo_item_compra(cur, id_compra, id_item_origem, id_produto, quantidade, valor_unitario):
    cur.execute(
        "INSERT INTO corporativo.itens_compras (id_item_origem, id_compra, id_produto, quantidade, valor_unitario) "
        "VALUES (%s,%s,%s,%s,%s)",
        (id_item_origem, id_compra, id_produto, quantidade, valor_unitario),
    )


def refresh_dim_fornecedores(cur, sistema_origem, id_origem):
    cur.execute("""
        SELECT nome, telefone FROM corporativo.fornecedores
        WHERE sistema_origem = %s AND id_fornecedor_origem = %s
    """, (sistema_origem, id_origem))
    r = cur.fetchone()
    if r is None:
        return
    nome, telefone = r
    cur.execute("""
        INSERT INTO dim_fornecedores (sistema_origem, id_fornecedor_original, nome_fornecedor, telefone)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (sistema_origem, id_fornecedor_original) DO UPDATE SET
            nome_fornecedor = EXCLUDED.nome_fornecedor, telefone = EXCLUDED.telefone
    """, (sistema_origem, id_origem, nome, telefone))
