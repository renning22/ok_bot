CREATE TABLE okex_reported_orders(
    order_id int primary key,
    instrument_id varchar(16),
    size int,
    timestamp varchar(36),
    filled_qty int,
    fee double,
    price double,
    price_avg double,
    status int,
    type int,
    contract_val int,
    leverage int
);