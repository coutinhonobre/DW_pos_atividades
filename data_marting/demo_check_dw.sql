-- Demo: contagem do DW, pra comparar antes/depois de cada insert e de cada carga.
-- Rodar: docker exec -i dw_postgres psql -U postgres -d dw < scripts/demo_check_dw.sql

SELECT
    (SELECT COUNT(*) FROM fato_vendas) AS total_fato_vendas,
    (SELECT COUNT(*) FROM fato_vendas WHERE sistema_origem = 'Mercearia') AS vendas_mercearia,
    (SELECT COUNT(*) FROM fato_vendas WHERE sistema_origem = 'Northwind') AS vendas_northwind,
    (SELECT COUNT(*) FROM fato_compras) AS total_fato_compras,
    (SELECT COUNT(*) FROM dim_clientes) AS total_dim_clientes,
    (SELECT COUNT(*) FROM dim_produtos) AS total_dim_produtos,
    (SELECT COUNT(*) FROM dim_enderecos) AS total_dim_enderecos;
