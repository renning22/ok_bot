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
        self.conn = sqlite3.connect(_DB_IN_USE)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


def use_dev_db():
    global _DB_IN_USE
    _DB_IN_USE = _DEV_DB


def reset_database():
    with DbCursor() as c:
        c.execute('DROP TABLE IF EXISTS runtime_transactions;')
        c.execute('DROP TABLE IF EXISTS runtime_orders;')

    _create_database()


def _create_database():
    with DbCursor() as c:
        c.execute('''
            CREATE TABLE runtime_transactions (
             transaction_id TEXT PRIMARY KEY,
             status TEXT,
             last_update_time TEXT DEFAULT (DATETIME('now','localtime'))
            );
        ''')
        c.execute('''
            CREATE TABLE runtime_orders (
             order_id TEXT PRIMARY KEY,
             transaction_id TEXT,
             comment TEXT,
             status TEXT,
             size INTEGER,
             filled_qty INTEGER,
             price TEXT,
             price_avg TEXT,
             fee TEXT,
             type INTEGER,
             timestamp TEXT,
             last_update_time TEXT DEFAULT (DATETIME('now','localtime'))
            );
        ''')


def _update_transaction(transaction_id, stauts):
    with DbCursor() as c:
        c.execute('''
            INSERT OR REPLACE INTO runtime_transactions(
                transaction_id,
                status
            )
            VALUES (?, ?);
        ''', (transaction_id, stauts))


def async_update_transaction(*argv, **kwargs):
    _executor.submit(_update_transaction, *argv, **kwargs)


def _update_order(order_id,
                  transaction_id,
                  comment,
                  status,
                  size,
                  filled_qty,
                  price,
                  price_avg,
                  fee,
                  type,
                  timestamp):
    with DbCursor() as c:
        c.execute('''
            INSERT OR REPLACE INTO runtime_orders(
                order_id,
                transaction_id,
                comment,
                status,
                size,
                filled_qty,
                price,
                price_avg,
                fee,
                type,
                timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ''', (
            order_id,
            transaction_id,
            comment,
            status,
            size,
            filled_qty,
            price,
            price_avg,
            fee,
            type,
            timestamp
        ))


def async_update_order(*argv, **kwargs):
    _executor.submit(_update_order, *argv, **kwargs)


def _testing(_):
    use_dev_db()
    reset_database()
    async_update_transaction('transaction-id-123', 'ended')
    async_update_order('2217655012660224',
                       transaction_id=None,
                       comment='comment',
                       status=-1,
                       size=2,
                       filled_qty=1,
                       price=3000,
                       price_avg=None,
                       fee=0.01,
                       type=None,
                       timestamp=None)
    _executor.shutdown(wait=True)


if __name__ == '__main__':
    app.run(_testing)
