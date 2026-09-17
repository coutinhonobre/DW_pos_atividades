# DW_pos_atividades

Projeto da disciplina de Data Warehouse (PosIFG). Simula uma empresa com dois
sistemas transacionais (Mercearia e Northwind) e integra os dois em um único
Data Warehouse, com um catálogo de metadados documentando a origem de cada
campo.

## Estrutura

```
docker-compose.yml          sobe os bancos (mercearia, northwind, dw) + Airflow
start.sh                    recria tudo do zero (schema + dados)
transacional/                caminho dos sistemas de origem (OLTP)
  mercearia_mysql.sql       schema transacional da Mercearia (MySQL)
  mercearia_data_mysql.sql  dados fake da Mercearia (gerados com Faker pt_BR)
  northwind.sql             schema + dados do Northwind (Postgres, dump original)
  demo_insert_mercearia.sql   inserts de teste na Mercearia
  demo_insert_northwind.sql   inserts de teste no Northwind
dw/                          camada corporativa integrada (Inmon: por assunto, normalizada)
  dw_postgres.sql           schema normalizado do DW (schema `corporativo` no banco `dw`)
  staging_postgres.sql      schema de staging (schema `staging` no banco `dw`), usado pela carga incremental
  metadados_postgres.sql    schema do catálogo de metadados (schema `metadados` no banco `dw`)
  metadados_seed_postgres.sql  inserts de metadados (execução manual, ver abaixo)
data_marting/                 camada de data marts (dimensional, dependente do DW normalizado)
  dw_postgres.sql           schema do data mart em estrela (Postgres, banco `dw`)
  demo_check_dw.sql         queries de conferência dos dados carregados
airflow/
  dags/carga_inicial_dw.py      DAG da primeira carga do DW (ver seção Airflow)
  dags/carga_incremental_dw.py  DAG da carga parcial/incremental (ver seção Airflow)
diagrams/
  dw_modelo_corporativo.drawio  diagrama do DW normalizado por assunto (dw/dw_postgres.sql)
  dw_modelo_estrela.drawio      diagrama do data mart em estrela (data_marting/dw_postgres.sql)
  metadados_modelo.drawio       diagrama do modelo de metadados
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

### DW normalizado — PostgreSQL, `localhost:5432`, banco `dw`, schema `corporativo`

Modelo corporativo do Inmon: orientado a assunto, integrado, não-volátil e
variante no tempo. Organizado em 5 assuntos, independentes de qualquer
sistema de origem (nomes de tabela sempre no plural — `id_cliente`,
`id_produto` etc. continuam no singular, são só o identificador):

- **Tempo** — `tempos` (calendário gerado uma vez via `generate_series`,
  não vem de sistema transacional; referência compartilhada, os fatos
  guardam a data direto)
- **Geografia** — `paises`, `estados`, `cidades`, `enderecos`
- **Pessoas** — `profissoes`, `clientes`, `cargos`, `funcionarios`
- **Produto e Parceiros** — `categorias`, `produtos`, `transportadoras`, `fornecedores`
- **Transações** — `vendas`, `itens_vendas`, `compras`, `itens_compras`

Integrado: toda entidade que vem de sistema transacional traz `sistema_origem`
+ `id_..._origem`, resolvendo Northwind/Mercearia para uma chave corporativa
única (sequência própria, ex. `corporativo.seq_clientes`).

Não-volátil / variante no tempo: `clientes`, `funcionarios` e `produtos` nunca
sofrem `UPDATE` nos atributos que mudam — uma alteração insere uma nova linha
(`data_inicio_validade` / `data_fim_validade` / `flag_atual`), preservando o
histórico. Por isso a PK dessas três tabelas é composta
(`id_..., data_inicio_validade`).

Como `vendas` / `itens_vendas` / `compras` / `itens_compras` guardam só o id
natural (não a versão), a ligação com `clientes` / `funcionarios` / `produtos`
é uma **junção temporal** (id + data do evento dentro do intervalo de
validade), não uma FK simples — por isso essas colunas não têm `REFERENCES`
formal, só comentário no DDL.

Diagrama: `diagrams/dw_modelo_corporativo.drawio`.

### Data Mart (estrela) — banco `dw`, schema `public`

Data mart dimensional (Kimball) derivado do DW normalizado acima, hoje ainda
único e combinando os dois processos de negócio (vendas e compras) em vez de
ser separado por fato — próximo passo é quebrar em um mart por fato (Vendas,
Compras), cada um com suas próprias dimensões conformadas.

Grão dos fatos: item vendido / item comprado (uma linha por produto dentro de
uma venda ou compra).

- `dim_tempos`, `dim_enderecos`, `dim_produtos`, `dim_clientes` — dimensões
  conformadas, compartilhadas pelos dois sistemas (`sistema_origem` identifica
  a procedência de cada linha)
- `dim_funcionarios`, `dim_transportadoras` — exclusivas do Northwind (a
  Mercearia não tem esse conceito); usam a linha `-1` ("Não aplicável") como
  membro desconhecido para as vendas da Mercearia, em vez de FK nula
- `dim_fornecedores` — a contraparte de uma compra da Mercearia; antes
  reaproveitava `dim_clientes` por engano, agora tem dimensão própria
- `fato_vendas` — cobre os dois sistemas
- `fato_compras` — só a Mercearia (o Northwind não tem processo de compra de
  fornecedor)

Decisões de modelagem registradas: sem conversão cambial entre BRL/USD (campo
`moeda` em `dim_produtos`); `faixa_renda`/`faixa_etaria` em `dim_clientes` são
faixas descritivas (banding), não os valores brutos.

Diagrama: `diagrams/dw_modelo_estrela.drawio`.

### Metadados — schema `metadados` dentro do banco `dw`

Catálogo que documenta a origem e a regra de transformação de cada campo do
DW: sistemas/tabelas/campos transacionais, tabelas/campos do DW, algoritmos
de ETL e a linhagem entre eles (`integracao_transacional_dw`).

O schema é criado junto com o `dw` (via `start.sh`), mas fica **vazio** — os
dados são propositalmente inseridos à parte, executando manualmente:

```bash
docker exec -i dw_postgres psql -U postgres < dw/metadados_seed_postgres.sql
```

O seed documenta 28 tabelas / 158 campos transacionais, 8 tabelas / 88 campos
do DW e 91 linhas de linhagem, incluindo os campos conformados que vêm dos
dois sistemas ao mesmo tempo (ex: `dim_clientes.nome_cliente` ← `Pessoas.NOME`
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

A DAG `carga_inicial_dw` popula as duas camadas, nessa ordem — nunca lê a
origem duas vezes para a mesma linha:

1. **corporativo** (`carregar_corporativo_*`) — lê Mercearia/Northwind direto,
   resolve geografia (`paises`/`estados`/`cidades`), grava histórico em
   `clientes`/`funcionarios`/`produtos` e por fim `vendas`/`itens_vendas`/
   `compras`/`itens_compras` (`DELETE` + reinsert completo, é carga inicial)
2. **data_marting** (`carregar_marting_*`) — nunca toca Mercearia/Northwind;
   lê só o `corporativo` e monta `dim_*`/`fato_*` (`dim_tempos` já vem
   populada por `dw_postgres.sql`, via `generate_series`)

Ela não tem `schedule` (é disparo manual — faz sentido para uma carga
inicial). As tabelas de lookup do corporativo são idempotentes (upsert pela
chave natural); os fatos, tanto no corporativo quanto no mart, são recriados
do zero a cada execução (`DELETE` antes do `INSERT`).

Pra disparar pela UI: acesse `localhost:8080`, ative a DAG e clique em
"Trigger DAG". Pela CLI:

```bash
docker exec dw_airflow airflow dags trigger carga_inicial_dw
```

## Airflow — carga incremental (parcial) do DW

A DAG `carga_incremental_dw` traz só o que mudou desde a última carga, sem
olhar data — o corte é pelo **maior id de origem já presente na fato do
corporativo** (`MAX(id_venda_origem)` em `corporativo.vendas` /
`MAX(id_compra_origem)` em `corporativo.compras`), então uma venda com data
retroativa lançada depois também seria pega, e uma venda "de hoje" que já foi
carregada não seria reprocessada. O objetivo é não sobrecarregar os bancos
transacionais: Mercearia/Northwind só são lidos uma vez, na staging, e nunca
de novo depois disso.

Fluxo (schema `staging` dentro do banco `dw`, como área intermediária):

1. **Staging** — `stage_vendas_mercearia`, `stage_vendas_northwind`,
   `stage_compras_mercearia` comparam o watermark (contra o `corporativo`,
   não contra o mart) com a origem e jogam só as linhas novas nas tabelas
   `staging.stg_*` (truncadas a cada execução) — única parte que toca
   Mercearia/Northwind
2. **corporativo** — `atualizar_corporativo_*` olham só as chaves naturais
   que apareceram na staging e fazem upsert (histórico pra
   clientes/funcionarios/produtos, simples pros demais); depois
   `carregar_corporativo_vendas`/`carregar_corporativo_compras` inserem
   (append-only) as vendas/compras novas direto da staging
3. **data_marting** — `atualizar_marting_dim_*` atualizam os `dim_*` a
   partir do corporativo (nunca da origem) para as mesmas chaves da
   staging; `carregar_marting_fato_*` inserem as linhas novas em
   `fato_vendas`/`fato_compras`, resolvendo pela staging + pelos dims já
   atualizados
4. **Limpeza** — `limpar_staging` esvazia as tabelas de staging no final; elas
   só têm dado enquanto a DAG está rodando

Importante: a atualização de dimensão (nas duas camadas) só acontece para
entidades que aparecem em algum fato novo dessa execução — se um cliente
mudar de renda mas não comprar nada nessa leva, a mudança só é refletida na
próxima vez que ele aparecer em uma venda/compra nova.

```bash
docker exec dw_airflow airflow dags trigger carga_incremental_dw
```

## Conexão via DBeaver / cliente SQL

| | Postgres (northwind / dw) | MySQL (mercearia) |
|---|---|---|
| Host | `localhost` | `localhost` |
| Porta | `5432` | `3306` |
| Usuário | `postgres` | `root` |
| Senha | `postgres` | `mysql` |
