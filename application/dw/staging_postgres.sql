\c dw

CREATE SCHEMA staging;

CREATE TABLE staging.stg_vendas_mercearia (
    id_venda INT,
    id_pessoa INT,
    id_endereco INT,
    data_venda DATE,
    tipo_venda INT,
    id_itemvenda INT,
    id_produto INT,
    quantidade INT,
    vlr_unitario NUMERIC(15,2)
);

CREATE TABLE staging.stg_vendas_northwind (
    order_id INT,
    customer_id VARCHAR(5),
    employee_id INT,
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via INT,
    freight NUMERIC(15,2),
    product_id INT,
    quantity INT,
    unit_price NUMERIC(15,2),
    discount NUMERIC(6,4)
);

CREATE TABLE staging.stg_compras_mercearia (
    id_compra INT,
    id_pessoa INT,
    id_endereco INT,
    data_pedido DATE,
    data_entrada DATE,
    id_itemcompra INT,
    id_produto INT,
    quantidade INT,
    vlr_unitario NUMERIC(15,2)
);
