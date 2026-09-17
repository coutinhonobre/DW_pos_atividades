-- Demo: simula um novo pedido no Northwind pra apresentar a carga incremental.
-- Rodar: docker exec -i dw_postgres psql -U postgres -d northwind < transacional/demo_insert_northwind.sql
-- Usa MAX(order_id)+1 em vez de id fixo, então pode rodar mais de uma vez sem colidir.

INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_via)
SELECT COALESCE(MAX(order_id), 0) + 1, 'ALFKI', 1, CURRENT_DATE, 1 FROM orders;

INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
SELECT MAX(order_id), 1, 18, 5, 0 FROM orders;

-- Conferência do que acabou de entrar
SELECT * FROM orders ORDER BY order_id DESC LIMIT 1;
SELECT * FROM order_details ORDER BY order_id DESC LIMIT 1;
