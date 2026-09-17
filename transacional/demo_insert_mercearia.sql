-- Demo: simula uma nova venda e uma nova compra na Mercearia.
-- Rodar: docker exec -i dw_mysql mysql -uroot -pmysql mercearia < scripts/demo_insert_mercearia.sql
-- Usa MAX(id)+1 em vez de id fixo, então pode rodar mais de uma vez sem colidir.

-- Nova venda (pessoa 1) com 2 itens
INSERT INTO Vendas (ID_VENDA, ID_PESSOA, DATA_VENDA, DATA_FATURAMENTO, TIPO_VENDA)
SELECT COALESCE(MAX(ID_VENDA), 0) + 1, 1, CURDATE(), NULL, 1 FROM Vendas;

INSERT INTO Itens_Vendas (ID_ITEMVENDA, ID_VENDA, ID_PRODUTO, QUANTIDADE, VLR_UNITARIO)
SELECT COALESCE(MAX(ID_ITEMVENDA), 0) + 1, (SELECT MAX(ID_VENDA) FROM Vendas), 1, 3, 18.67 FROM Itens_Vendas;

INSERT INTO Itens_Vendas (ID_ITEMVENDA, ID_VENDA, ID_PRODUTO, QUANTIDADE, VLR_UNITARIO)
SELECT COALESCE(MAX(ID_ITEMVENDA), 0) + 1, (SELECT MAX(ID_VENDA) FROM Vendas), 2, 1, 23.07 FROM Itens_Vendas;

-- Nova compra (pessoa 1) com 1 item
INSERT INTO Compras (ID_COMPRA, DATA_PEDIDO, DATA_ENTRADA, ID_PESSOA)
SELECT COALESCE(MAX(ID_COMPRA), 0) + 1, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 3 DAY), 1 FROM Compras;

INSERT INTO Itens_compras (ID_ITEMCOMPRA, ID_COMPRA, ID_PRODUTO, QUANTIDADE, VLR_UNITARIO)
SELECT COALESCE(MAX(ID_ITEMCOMPRA), 0) + 1, (SELECT MAX(ID_COMPRA) FROM Compras), 1, 10, 12.50 FROM Itens_compras;

-- Conferência do que acabou de entrar
SELECT * FROM Vendas ORDER BY ID_VENDA DESC LIMIT 1;
SELECT * FROM Itens_Vendas ORDER BY ID_ITEMVENDA DESC LIMIT 2;
SELECT * FROM Compras ORDER BY ID_COMPRA DESC LIMIT 1;
SELECT * FROM Itens_compras ORDER BY ID_ITEMCOMPRA DESC LIMIT 1;
