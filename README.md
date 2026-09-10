# DW_pos_atividades

Projeto da disciplina de Data Warehouse (PosIFG). Simula uma empresa com dois
sistemas transacionais (Mercearia e Northwind) e integra os dois em um único
Data Warehouse, com um catálogo de metadados documentando a origem de cada
campo.

## Estrutura

```
docker-compose.yml          sobe os bancos (mercearia, northwind, dw) + Airflow
start.sh                    recria tudo do zero (schema + dados)
scripts/
  mercearia_mysql.sql       schema transacional da Mercearia (MySQL)
  mercearia_data_mysql.sql  dados fake da Mercearia (gerados com Faker pt_BR)
  northwind.sql             schema + dados do Northwind (Postgres, dump original)
  dw_postgres.sql           schema do Data Warehouse (Postgres, banco `dw`)
  metadados_postgres.sql    schema do catálogo de metadados (schema `metadados` no banco `dw`)
  metadados_seed_postgres.sql  inserts de metadados (execução manual, ver abaixo)
airflow/
  dags/carga_inicial_dw.py  DAG da primeira carga do DW (ver seção Airflow)
diagrams/
  dw_modelo_estrela.drawio  diagrama do modelo estrela do DW
  metadados_modelo.drawio   diagrama do modelo de metadados
apresentacao/
  Slides_Dw Diniz.pdf       material de referência do professor
```

## Subindo o ambiente

```bash
./start.sh
```

Isso recria os containers do zero (`docker compose down -v && up -d --force-recreate`)
e reprocessa todos os scripts de inicialização. Não há volume persistente — os bancos
sempre nascem limpos, com schema e dados fake reconstruídos a cada execução.

## Bancos de dados

### Mercearia — MySQL, `localhost:3306`

Banco transacional de uma mercearia (vendas, compras, pessoas, produtos, endereços).
14 tabelas, populadas com dados fake (Faker `pt_BR`): 500 pessoas, 100 produtos,
500 compras, 1500 vendas.

- Usuário: `root` / Senha: `mysql`

### Northwind — PostgreSQL, `localhost:5432`, banco `northwind`

Base de exemplo clássica (distribuidora de alimentos por atacado). Vem com os
dados originais do dump (830 pedidos).

### DW — PostgreSQL, `localhost:5432`, banco `dw`

Modelo estrela integrando os dois sistemas transacionais. Grão dos fatos: item
vendido / item comprado (uma linha por produto dentro de uma venda ou compra).

- `dim_tempo`, `dim_enderecos`, `dim_produtos`, `dim_cliente` — dimensões
  conformadas, compartilhadas pelos dois sistemas (`sistema_origem` identifica
  a procedência de cada linha)
- `dim_funcionario`, `dim_transportadora` — exclusivas do Northwind (a
  Mercearia não tem esse conceito); usam a linha `-1` ("Não aplicável") como
  membro desconhecido para as vendas da Mercearia, em vez de FK nula
- `fato_vendas` — cobre os dois sistemas
- `fato_compras` — só a Mercearia (o Northwind não tem processo de compra de
  fornecedor)

Decisões de modelagem registradas: sem conversão cambial entre BRL/USD (campo
`moeda` em `dim_produtos`); `faixa_renda`/`faixa_etaria` em `dim_cliente` são
faixas descritivas (banding), não os valores brutos.

Diagrama: `diagrams/dw_modelo_estrela.drawio`.

### Metadados — schema `metadados` dentro do banco `dw`

Catálogo que documenta a origem e a regra de transformação de cada campo do
DW: sistemas/tabelas/campos transacionais, tabelas/campos do DW, algoritmos
de ETL e a linhagem entre eles (`integracao_transacional_dw`).

O schema é criado junto com o `dw` (via `start.sh`), mas fica **vazio** — os
dados são propositalmente inseridos à parte, executando manualmente:

```bash
docker exec -i dw_postgres psql -U postgres < scripts/metadados_seed_postgres.sql
```

O seed documenta 28 tabelas / 158 campos transacionais, 8 tabelas / 88 campos
do DW e 91 linhas de linhagem, incluindo os campos conformados que vêm dos
dois sistemas ao mesmo tempo (ex: `dim_cliente.nome_cliente` ← `Pessoas.NOME`
e ← `customers.company_name`).

Diagrama: `diagrams/metadados_modelo.drawio`.

## Airflow — primeira carga do DW

Instância `standalone` (webserver + scheduler em um único container,
`apache/airflow:latest`), disponível em `http://localhost:8080` depois do
`./start.sh` (leva 1-2 min pra subir).

- Usuário: `airflow` / Senha: `airflow`

(fixos via `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` +
`airflow/simple_auth_manager_passwords.json`, montado no container — sem isso
o Airflow geraria uma senha aleatória a cada start)

A DAG `carga_inicial_dw` extrai da Mercearia (MySQL) e do Northwind (Postgres)
e carrega as tabelas do `dw` que a inicialização do schema deixa vazias
(`dim_tempo` já vem populada por `dw_postgres.sql`, via `generate_series`):

1. `carregar_dim_enderecos`, `carregar_dim_produtos`, `carregar_dim_cliente`,
   `carregar_dim_funcionario`, `carregar_dim_transportadora` — em paralelo
2. `carregar_fato_vendas`, `carregar_fato_compras` — depois das dimensões,
   pois fazem lookup da chave substituta de cada dimensão

Ela não tem `schedule` (é disparo manual — faz sentido para uma carga
inicial) e é idempotente nas dimensões (`ON CONFLICT DO NOTHING` pela chave
natural `sistema_origem` + id de origem); os fatos são recriados do zero a
cada execução (`DELETE` antes do `INSERT`).

Pra disparar pela UI: acesse `localhost:8080`, ative a DAG e clique em
"Trigger DAG". Pela CLI:

```bash
docker exec dw_airflow airflow dags trigger carga_inicial_dw
```

## Conexão via DBeaver / cliente SQL

| | Postgres (northwind / dw) | MySQL (mercearia) |
|---|---|---|
| Host | `localhost` | `localhost` |
| Porta | `5432` | `3306` |
| Usuário | `postgres` | `root` |
| Senha | `postgres` | `mysql` |
