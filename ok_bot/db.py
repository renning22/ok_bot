# TODO: convert _testing to unittest

import logging
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from .quant import Quant

DEV_DB = 'dev.db'
PROD_DB = 'prod.db'


def _sql_type_safe_filter(kwargs):
    ret = {}
    for k, v in kwargs.items():
        if isinstance(v, Quant):
            ret[k] = str(v)
        else:
            ret[k] = v
    return ret


def _update_transaction(cursor_creator, **kwargs):
    kwargs = _sql_type_safe_filter(kwargs)
    try:
        with cursor_creator() as c:
            c.execute('''
                INSERT OR REPLACE INTO runtime_transactions(
                    transaction_id,
                    vol,
                    slow_price,
                    fast_price,
                    close_price_gap,
                    start_time_sec,
                    end_time_sec,
                    status
                )
                VALUES (
                    :transaction_id,
                    :vol,
                    :slow_price,
                    :fast_price,
                    :close_price_gap,
                    :start_time_sec,
                    :end_time_sec,
                    :status
                );
            ''', kwargs)
    except sqlite3.OperationalError:
        logging.error('exception in _update_transaction', exc_info=True)


def _update_order(cursor_creator, **kwargs):
    kwargs = _sql_type_safe_filter(kwargs)
    try:
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
                VALUES (
                    :order_id,
                    :transaction_id,
                    :comment,
                    :status,
                    :size,
                    :filled_qty,
                    :price,
                    :price_avg,
                    :fee,
                    :type,
                    :timestamp
                );
            ''', kwargs)
    except sqlite3.OperationalError:
        logging.error('exception in _update_order', exc_info=True)


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
        try:
            with self._cursor_creator() as c:
                c.execute('''
                CREATE TABLE IF NOT EXISTS runtime_transactions (
                    transaction_id     TEXT PRIMARY KEY,
                    vol                NUMERIC,
                    slow_price         NUMERIC,
                    fast_price         NUMERIC,
                    close_price_gap    NUMERIC,
                    start_time_sec     NUMERIC,
                    end_time_sec       NUMERIC,
                    status             TEXT,
                    last_update_time   TEXT DEFAULT (DATETIME('now','localtime'))
                );
                ''')
                c.execute('''
                CREATE TABLE IF NOT EXISTS runtime_orders (
                    order_id            INTEGER PRIMARY KEY,
                    transaction_id      TEXT,
                    comment             TEXT,
                    status              INTEGER,
                    size                INTEGER,
                    filled_qty          INTEGER,
                    price               NUMERIC,
                    price_avg           NUMERIC,
                    fee                 NUMERIC,
                    type                INTEGER,
                    timestamp           TEXT,
                    last_update_time    TEXT DEFAULT (DATETIME('now','localtime'))
                );
                ''')
        except sqlite3.OperationalError:
            logging.error('exception in _update_order', exc_info=True)

    def async_update_transaction(self, **kwargs):
        """Force to use kwargs explicitly."""
        self._executor.submit(
            _update_transaction,
            self._cursor_creator,
            **kwargs)

    def async_update_order(self, **kwargs):
        """Force to use kwargs explicitly."""
        self._executor.submit(
            _update_order,
            self._cursor_creator,
            **kwargs)

    def shutdown(self, wait=True):
        return self._executor.shutdown(wait=True)


class ProdDb(_BaseDb):
    def __init__(self):
        super().__init__(db_path=PROD_DB)


class DevDb(_BaseDb):
    """DevDB creates an empty dataset at DEV_DB every time."""

    def __init__(self):
        super().__init__(db_path=DEV_DB)
        self._reset_database()

    def _reset_database(self):
        """Never have the reset method on ProdDb."""
        with self._cursor_creator() as c:
            c.execute('DROP TABLE IF EXISTS runtime_transactions;')
            c.execute('DROP TABLE IF EXISTS runtime_orders;')
        self.create_tables_if_not_exist()


def _testing():
    db = DevDb()
    db.async_update_transaction(transaction_id='transaction-id-123',
                                vol=5,
                                slow_price=10.111,
                                fast_price=Quant(20.001),
                                close_price_gap='1.01',
                                start_time_sec=time.time(),
                                end_time_sec=None,
                                status='ended')
    db.async_update_order(order_id='2217655012660224',
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
    _testing()
