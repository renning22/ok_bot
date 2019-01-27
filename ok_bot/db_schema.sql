CREATE TABLE okex_reported_orders(
    order_id int primary key NOT NULL,
    instrument_id varchar(16) NOT NULL,
    size int NOT NULL,
    timestamp varchar(36) NOT NULL,
    filled_qty int,
    fee double,
    price double,
    price_avg double,
    status int,
    type int,
    contract_val int,
    leverage int
);

CREATE TABLE okex_reported_bills(
    ledger_id TEXT primary key,
    timestamp TEXT,
    amount REAL,
    balance INTEGER,
    currency TEXT,
    type TEXT,
    order_id INTEGER DEFAULT NULL,
    instrument_id TEXT DEFAULT NULL

);