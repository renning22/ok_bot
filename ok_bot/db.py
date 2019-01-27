import sqlite3
from concurrent.futures import ProcessPoolExecutor

from absl import app, logging

from . import decorator

_DEV_DB = 'dev.db'
_PROD_DB = 'prod.db'
_DB_IN_USE = _PROD_DB


_executor = ProcessPoolExecutor(max_workers=1)


class DbCursor:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        logging.info('_DB_IN_USE: %s', _DB_IN_USE)
        self.conn = sqlite3.connect(_DB_IN_USE)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


def _use_dev_db():
    global _DB_IN_USE
    _DB_IN_USE = _DEV_DB

# For testing


def _delete_tables():
    with DbCursor() as c:
        c.execute('DROP TABLE IF EXISTS runtime_transactions;')
        c.execute('DROP TABLE IF EXISTS runtime_orders;')


def _create_database():
    with DbCursor() as c:
        c.execute('''
            CREATE TABLE runtime_transactions (
             transaction_id INTEGER PRIMARY KEY,
             status TEXT,
             creation_time TEXT DEFAULT (DATETIME('now','localtime')),
             last_update_time TEXT DEFAULT (DATETIME('now','localtime'))
            );
        ''')
        c.execute('''
            CREATE TRIGGER runtime_transactions_last_update
                AFTER UPDATE ON runtime_transactions
            BEGIN
                UPDATE runtime_transactions SET
                    last_update_time = DATETIME('now','localtime')
                WHERE transaction_id = old.transaction_id;
            END
        ''')
        c.execute('''
            CREATE TABLE runtime_orders (
             order_id INTEGER PRIMARY KEY,
             transaction_id INTEGER,
             status TEXT,
             size INTEGER,
             filled_qty INTEGER,
             price REAL,
             price_avg REAL,
             timestamp INTEGER,
             creation_time TEXT DEFAULT (DATETIME('now','localtime')),
             last_update_time TEXT DEFAULT (DATETIME('now','localtime'))
            );
        ''')
        c.execute('''
            CREATE TRIGGER runtime_orders_last_update
                AFTER UPDATE ON runtime_orders
            BEGIN
                UPDATE runtime_orders SET
                    last_update_time = DATETIME('now','localtime')
                WHERE order_id = old.order_id;
            END
        ''')


def _update_transaction(transaction_id, stauts):
    with DbCursor() as c:
        c.execute('''
            INSERT OR REPLACE INTO runtime_transactions(
                transaction_id,
                status
            )
            VALUES (?, ?);
        ''', (int(transaction_id), str(stauts)))


def _update_order(order_id,
                  transaction_id,
                  status,
                  size,
                  filled_qty,
                  price,
                  price_avg,
                  timestamp):
    with DbCursor() as c:
        c.execute('''
            INSERT OR REPLACE INTO runtime_orders(
                order_id,
                transaction_id,
                status,
                size,
                filled_qty,
                price,
                price_avg,
                timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        ''', (
            int(order_id),
            int(transaction_id),
            str(status),
            int(size),
            int(filled_qty),
            float(price),
            float(price_avg),
            int(timestamp)
        ))


def async_update_transaction(*argv, **kwargs):
    _executor.submit(_update_transaction, *argv, **kwargs)


def async_update_order(*argv, **kwargs):
    _executor.submit(_update_order, *argv, **kwargs)


def _testing(_):
    _use_dev_db()
    _delete_tables()
    _create_database()
    async_update_transaction(123, 'unknown')
    async_update_order(456, 123, 'unknown', 2, 1, 3000, 2900, 1548497845)
    _executor.shutdown(wait=True)


if __name__ == '__main__':
    app.run(_testing)
