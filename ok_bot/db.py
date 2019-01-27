import sqlite3
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from absl import app, logging

DEV_DB = 'dev.db'
PROD_DB = 'prod.db'


def _update_transaction(cursor_creator,
                        transaction_id,
                        stauts):
    with cursor_creator() as c:
        c.execute('''
            INSERT OR REPLACE INTO runtime_transactions(
                transaction_id,
                status
            )
            VALUES (?, ?);
        ''', (transaction_id, stauts))


def _update_order(cursor_creator,
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
                  timestamp):
    with cursor_creator() as c:
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


class _DbCursor:
    def __init__(self, db_path):
        self._db_path = db_path
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self._db_path)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class _BaseDb:
    def __init__(self, db_path=PROD_DB):
        self._db_path = db_path
        self._executor = ProcessPoolExecutor(max_workers=1)
        self._cursor_creator = partial(_DbCursor, self._db_path)

    def create_tables_if_not_exist(self):
        with self._cursor_creator() as c:
            c.execute('''
                CREATE TABLE IF NOT EXISTS runtime_transactions (
                 transaction_id TEXT PRIMARY KEY,
                 status TEXT,
                 last_update_time TEXT DEFAULT (DATETIME('now','localtime'))
                );
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS runtime_orders (
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

    def async_update_transaction(self, *argv, **kwargs):
        self._executor.submit(
            _update_transaction,
            self._cursor_creator,
            *argv, **kwargs)

    def async_update_order(self, *argv, **kwargs):
        self._executor.submit(
            _update_order,
            self._cursor_creator,
            *argv, **kwargs)


class ProdDb(_BaseDb):
    def __init__(self):
        super().__init__(db_path=PROD_DB)


class DevDb(_BaseDb):
    """DevDB creates an empty dataset at DEV_DB every time."""

    def __init__(self):
        super().__init__(db_path=DEV_DB)
        self.reset_database()

    def reset_database(self):
        """Never have the reset method on ProdDb."""
        with self._cursor_creator() as c:
            c.execute('DROP TABLE IF EXISTS runtime_transactions;')
            c.execute('DROP TABLE IF EXISTS runtime_orders;')

        self.create_tables_if_not_exist()


def _testing(_):
    db = DevDb()
    db.async_update_transaction('transaction-id-123', 'ended')
    db.async_update_order('2217655012660224',
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
    db._executor.shutdown(wait=True)


if __name__ == '__main__':
    app.run(_testing)
