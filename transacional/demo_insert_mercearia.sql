-- Demo: simula uma nova venda na Mercearia pra apresentar a carga incremental.
-- Rodar: docker exec -i dw_mysql mysql -uroot -pmysql mercearia < transacional/demo_insert_mercearia.sql
-- Usa MAX(id)+1 em vez de id fixo, então pode rodar mais de uma vez sem colidir.

INSERT INTO Vendas (ID_VENDA, ID_PESSOA, DATA_VENDA, DATA_FATURAMENTO, TIPO_VENDA)
SELECT COALESCE(MAX(ID_VENDA), 0) + 1, 1, CURDATE(), NULL, 1 FROM Vendas;

INSERT INTO Itens_Vendas (ID_ITEMVENDA, ID_VENDA, ID_PRODUTO, QUANTIDADE, VLR_UNITARIO)
SELECT COALESCE(MAX(ID_ITEMVENDA), 0) + 1, (SELECT MAX(ID_VENDA) FROM Vendas), 1, 3, 18.67 FROM Itens_Vendas;

-- Conferência do que acabou de entrar
SELECT * FROM Vendas ORDER BY ID_VENDA DESC LIMIT 1;
SELECT * FROM Itens_Vendas ORDER BY ID_ITEMVENDA DESC LIMIT 1;
