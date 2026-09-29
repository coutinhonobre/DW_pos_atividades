# DW_pos_atividades

Projeto da disciplina de Data Warehouse (PosIFG). Simula uma empresa com dois
sistemas transacionais (Mercearia e Northwind) e integra os dois em um único
Data Warehouse, com um catálogo de metadados documentando a origem de cada
campo.

## Estrutura

```
docker-compose.yml          sobe os bancos (mercearia, northwind, dw) + Airflow + Metabase
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
  dw_modelo_estrela.drawio      diagrama dos data marts em estrela (data_marting/dw_postgres.sql)
  metadados_modelo.drawio       diagrama do modelo de metadados
metabase/
  Dockerfile                    imagem do container de provisionamento (metabase-setup)
  setup_dashboards.py           cria conexão e o dashboard do Mart Logística/Entregas via API do Metabase
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

Metabase sobe em `http://localhost:3000`. O container `metabase-setup` roda uma
vez, espera o Metabase ficar pronto e provisiona tudo sozinho via API (usuário
admin, conexão com o banco `dw` e o dashboard do Mart Logística/Entregas) —
ver seção BI abaixo. Os cards só mostram números depois que alguma carga
(`carga_inicial_dw` ou `carga_incremental_dw`) rodar no Airflow.

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
  (`vendas` também guarda `frete`, `id_tempo_envio` e `id_tempo_prazo` — só
  preenchidos para o Northwind, fonte do Mart Logística/Entregas abaixo)

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

### Data Marts (estrela) — banco `dw`, schema `public`

Três data marts dimensionais (Kimball), derivados do DW normalizado acima e
compartilhando as mesmas dimensões conformadas (`dim_tempos`, `dim_clientes`,
`dim_produtos`, `dim_enderecos`, `dim_funcionarios`, `dim_transportadoras`,
`dim_fornecedores`) — arquitetura de barramento (*bus architecture*), não um
esquema físico separado por mart:

- `dim_tempos`, `dim_enderecos`, `dim_produtos`, `dim_clientes` — dimensões
  conformadas, compartilhadas pelos dois sistemas (`sistema_origem` identifica
  a procedência de cada linha)
- `dim_funcionarios`, `dim_transportadoras` — exclusivas do Northwind (a
  Mercearia não tem esse conceito); usam a linha `-1` ("Não aplicável") como
  membro desconhecido para as vendas da Mercearia, em vez de FK nula
- `dim_fornecedores` — a contraparte de uma compra da Mercearia; antes
  reaproveitava `dim_clientes` por engano, agora tem dimensão própria

**Mart Vendas** (assunto Vendas) — fato `fato_vendas`, grão de item vendido
(uma linha por produto dentro de uma venda/pedido). Cobre os dois sistemas.
Dimensões: tempo, cliente, produto, endereço, funcionário, transportadora.
Uso: receita, mix de produtos, desempenho comercial por canal/vendedor/região.

**Mart Compras** (assunto Compras) — fato `fato_compras`, grão de item
comprado (uma linha por produto dentro de um pedido de compra). Só a
Mercearia (o Northwind não tem processo de compra de fornecedor). Dimensões:
tempo, fornecedor, produto, endereço. Uso: lead time de reposição, dependência
de fornecedor, custo de compra.

**Mart Logística/Entregas** (assunto Logística e Entregas) — fato
`fato_entregas`, grão de **pedido** (cabeçalho da venda, não de item — grão
mais alto que os outros dois marts, de propósito: frete e prazo são atributos
do pedido inteiro, não de cada linha). Só o Northwind, porque é o único
sistema de origem com o conceito de frete/prazo/data de envio (a Mercearia
não expede fisicamente) — as colunas `frete`, `id_tempo_envio` e
`id_tempo_prazo` em `corporativo.vendas` ficam `NULL` para toda venda da
Mercearia. Métricas: `valor_pedido` (agregado dos itens), `valor_frete`,
`prazo_dias` (prometido), `dias_para_envio` (real) e `atraso_dias` (real vs.
prometido — negativo é entregue antes do prazo). Dimensões: tempo (pedido e
envio), cliente, funcionário, transportadora, endereço. Uso: avaliar
transportadoras e vendedores por prazo de entrega, não só por valor vendido —
uma pergunta que o Mart Vendas, no grão de item, não responde diretamente.

Decisões de modelagem registradas: sem conversão cambial entre BRL/USD (campo
`moeda` em `dim_produtos`); `faixa_renda`/`faixa_etaria` em `dim_clientes` são
faixas descritivas (banding), não os valores brutos.

Diagrama: `diagrams/dw_modelo_estrela.drawio`.

### Metadados — schema `metadados` dentro do banco `dw`

Catálogo que documenta a origem e a regra de transformação de cada campo do
DW: sistemas/tabelas/campos transacionais, tabelas/campos do DW, algoritmos
de ETL e a linhagem entre eles (`integracao_transacional_dw`).

O schema e o seed são criados junto com o `dw` (via `start.sh`, scripts de
init do Postgres) — não precisa de passo manual.

O seed documenta 28 tabelas / 158 campos transacionais, 10 tabelas / 109 campos
do DW e 123 linhas de linhagem, incluindo os campos conformados que vêm dos
dois sistemas ao mesmo tempo (ex: `dim_clientes.nome_cliente` ← `Pessoas.NOME`
e ← `customers.company_name`) e os campos sem origem transacional, atribuídos
pelo próprio ETL (`sistema_origem`, `moeda` fixa, fallback `-1`), ancorados em
um `dado_externo` de "Regras de Negócio do ETL".

Diagrama: `diagrams/metadados_modelo.drawio`.

## Airflow — primeira carga do DW

Instância `standalone` (webserver + scheduler em um único container,
`apache/airflow:latest`), disponível em `http://localhost:8080` depois do
`./start.sh` (leva 1-2 min pra subir).

- Usuário: `airflow` / Senha: `airflow`

(fixos via `AIRFLOW_AUTH_USERNAME`/`AIRFLOW_AUTH_PASSWORD` no
`docker-compose.yml` — mesmo padrão de `POSTGRES_PASSWORD`/
`MYSQL_ROOT_PASSWORD`/`MB_ADMIN_PASSWORD` usado no resto do projeto. O
simple auth manager do Airflow só aceita senha via arquivo, não tem env var
pra senha inline, então o `command` do serviço `airflow` gera esse arquivo
a partir dessas env vars antes de subir — sem isso o Airflow geraria uma
senha aleatória a cada start)

A DAG `carga_inicial_dw` popula as duas camadas, nessa ordem — nunca lê a
origem duas vezes para a mesma linha:

1. **corporativo** (`carregar_corporativo_*`) — lê Mercearia/Northwind direto,
   resolve geografia (`paises`/`estados`/`cidades`), grava histórico em
   `clientes`/`funcionarios`/`produtos` e por fim `vendas`/`itens_vendas`/
   `compras`/`itens_compras` (`DELETE` + reinsert completo, é carga inicial)
2. **data_marting** (`carregar_marting_*`) — nunca toca Mercearia/Northwind;
   lê só o `corporativo` e monta `dim_*`/`fato_vendas`/`fato_compras`/
   `fato_entregas` (`dim_tempos` já vem populada por `dw_postgres.sql`, via
   `generate_series`; `fato_entregas` só recebe pedidos do Northwind)

Ela não tem `schedule` (é disparo manual — faz sentido para uma carga
inicial). As tabelas de lookup do corporativo são idempotentes (upsert pela
chave natural); os fatos, tanto no corporativo quanto no mart, são recriados
do zero a cada execução (`DELETE` antes do `INSERT`).

Pra disparar pela UI: acesse `localhost:8080`, ative a DAG e clique em
"Trigger DAG". Pela CLI:

```bash
docker exec nwmerc_airflow airflow dags trigger carga_inicial_dw
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
   `fato_vendas`/`fato_compras`/`fato_entregas`, resolvendo pela staging +
   pelos dims já atualizados (`fato_entregas` agrega a staging do Northwind
   por `order_id`, já que o grão é o pedido, não o item)
4. **Limpeza** — `limpar_staging` esvazia as tabelas de staging no final; elas
   só têm dado enquanto a DAG está rodando

Importante: a atualização de dimensão (nas duas camadas) só acontece para
entidades que aparecem em algum fato novo dessa execução — se um cliente
mudar de renda mas não comprar nada nessa leva, a mudança só é refletida na
próxima vez que ele aparecer em uma venda/compra nova.

```bash
docker exec nwmerc_airflow airflow dags trigger carga_incremental_dw
```

## BI — Metabase, um painel por data mart

`metabase/setup_dashboards.py` provisiona tudo via API do Metabase assim que
o container `metabase` fica pronto (sem clicar em nada na UI):

1. Cria o usuário admin e o usuário padrão de uso do dia a dia (credenciais
   vêm do ambiente — `MB_ADMIN_EMAIL`/`MB_ADMIN_PASSWORD`/`MB_STANDARD_EMAIL`/
   `MB_STANDARD_PASSWORD` no serviço `metabase-setup` do `docker-compose.yml`,
   com os valores abaixo como default) e a conexão com o banco `dw` (schema
   `public`, os data marts).
2. Cria uma coleção e um dashboard por mart — **Painel Vendas**
   (`fato_vendas`, 10 cards, filtros Período + Sistema de origem), **Painel
   Compras** (`fato_compras`, 9 cards, filtros Período + Fornecedor) e
   **Painel Logística — Northwind** (`fato_entregas`, 10 cards, filtros
   Período + Transportadora) — cada um com consultas nativas (SQL) sobre seu
   próprio fato/dimensões.
3. Monta cada dashboard com os cards já posicionados numa grade de 24
   colunas, com os filtros do dashboard encadeados às variáveis SQL nativas
   de cada card.

Só têm efeito na **primeira execução** (setup inicial do Metabase); mudar as
variáveis depois não troca a senha de uma instância já provisionada — nesse
caso, troque pela UI do Metabase ou refaça o `start.sh` (recria os
containers do zero).

- Login admin (administração): `admin@metabase.com` / `Metabase123!`
- Login padrão (só visualização): `metabase@metabase.com` / `metabase`

Painel Vendas (grão de item vendido, cobre os dois sistemas): total vendido,
ticket médio, vendas por sistema de origem, top 10 produtos/clientes, vendas
por mês/categoria, ticket médio por faixa de renda.

Painel Compras (grão de item comprado, só Mercearia): total comprado, lead
time médio, top 10 fornecedores por valor, lead time por fornecedor, compras
por mês, top 10 produtos comprados, resumo por fornecedor.

Painel Logística — Northwind (grão de pedido, pensando como analista de
logística, focado em prazo e custo de entrega, não só em valor vendido):

- **KPIs**: total de pedidos, frete total, prazo médio de envio, atraso médio
  vs. prometido e % entregue no prazo.
- **Atraso médio por transportadora** e **frete médio por transportadora** —
  responde "qual transportadora atrasa mais" e "qual é mais cara", perguntas
  diferentes (nem sempre a mais cara é a mais lenta).
- **Pedidos por mês** — sazonalidade da demanda de expedição.
- **Top 10 funcionários por valor despachado** — cruza vendedor com o
  processo de logística, não só o de vendas.
- **Resumo por transportadora** — tabela com todas as métricas lado a lado,
  para comparação direta.

Todas as queries são nativas, então rodam mesmo sem o Metabase "conhecer" o
schema de antemão — os números só aparecem depois que alguma carga
(`carga_inicial_dw` ou `carga_incremental_dw`) rodar; antes disso os cards
aparecem vazios, e não precisam ser recriados depois, só recarregar a página.

```bash
docker compose logs -f metabase-setup
```

mostra o progresso e, no final, a URL exata de cada dashboard
(`http://localhost:3000/dashboard/<id>`). Rodar de novo
(`docker compose up metabase-setup`, ou implícito no `start.sh`) é
idempotente: cada dashboard só é recriado se ainda não existir, checagem
independente entre os três.

## Conexão via DBeaver / cliente SQL

| | Postgres (northwind / dw) | MySQL (mercearia) |
|---|---|---|
| Host | `localhost` | `localhost` |
| Porta | `5432` | `3306` |
| Usuário | `postgres` | `root` |
| Senha | `postgres` | `mysql` |
