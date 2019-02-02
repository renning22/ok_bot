"""
This module defines a crawler that periodically crawl completed orders from
OKEX. Then store the result to DB. OKEX only returns the most recent 100 orders.
This crawler will run outside of the arbitrage program.
"""
import time
import sqlite3
import signal
import sys

import logging
import argparse

from .rest_api_v3 import RestApiV3
from .logger import init_global_logger

SLEEP_TIME_IN_SECOND = 60 * 12  # 12 hours


class BillCrawler:
    def __init__(self, currency, db_file):
        self.api = RestApiV3()
        self.currency = currency
        self.db_conn = sqlite3.connect(db_file)
        self.all_instrument_ids = self.api.get_all_instrument_ids_blocking(
            currency)
        self.create_tables()

    def create_tables(self):
        cursor = self.db_conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reported_orders(
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
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reported_bills(
            ledger_id TEXT primary key,
            timestamp TEXT,
            amount REAL,
            balance INTEGER,
            currency TEXT,
            type TEXT,
            order_id INTEGER DEFAULT NULL,
            instrument_id TEXT DEFAULT NULL
        )          
        ''')
        self.db_conn.commit()

    def crawl_orders(self):
        orders = self.api.completed_orders(self.all_instrument_ids)
        logging.info(f'{len(orders)} orders crawled from OKEX')
        for order in orders:
            self.insert_order_to_db(order)
        logging.info('All orders synced to DB')

    def crawl_ledgers(self):
        ledgers = self.api.all_ledgers(self.currency)
        logging.info(f'{len(ledgers)} ledgers crawled from OKEX')
        for ledger in ledgers:
            self.insert_ledger_to_db(ledger)
        logging.info('All ledgers synced to DB')

    def crawl(self):
        while True:
            self.crawl_ledgers()
            self.crawl_orders()
            logging.info(f'will sleep for {SLEEP_TIME_IN_SECOND} seconds')
            time.sleep(SLEEP_TIME_IN_SECOND)

    def insert_order_to_db(self, order):
        sql = '''
            INSERT OR REPLACE INTO reported_orders(
                order_id,
                instrument_id,
                size,
                timestamp,
                filled_qty,
                fee,
                price,
                price_avg,
                status,
                type,
                contract_val,
                leverage            
            )
            VALUES (:order_id, :instrument_id, :size, :timestamp, :filled_qty,
            :fee, :price, :price_avg, :status, :type, :contract_val, :leverage)
        '''
        cursor = self.db_conn.cursor()
        cursor.execute(sql, {
            'order_id': int(order['order_id']),
            'instrument_id': order['instrument_id'],
            'size': int(order['size']),
            'timestamp': order['timestamp'],
            'filled_qty': int(order['filled_qty']),
            'fee': order['fee'],
            'price': float(order['price']),
            'price_avg': float(order['price_avg']),
            'status': int(order['status']),
            'type': int(order['type']),
            'contract_val': int(order['contract_val']),
            'leverage': int(order['leverage']),
        })
        self.db_conn.commit()

    def insert_ledger_to_db(self, ledger):
        def extract(field):
            if 'details' in ledger and field in ledger['details']:
                return ledger['details'][field]
            else:
                return None

        sql = '''
            INSERT OR REPLACE INTO reported_bills(
                ledger_id,
                timestamp,
                amount,
                balance,
                currency,
                type,
                order_id,
                instrument_id         
            )
            VALUES (:ledger_id, :timestamp, :amount, :balance, :currency, 
            :type, :order_id, :instrument_id)
        '''
        cursor = self.db_conn.cursor()
        assert ledger['type'] in [
            'transfer',  # funds transfer
            'match',  # open long/open short/close long/close short
            'fee',
            'settlement',
            'liquidation',  # forced close
        ]
        cursor.execute(sql, {
            'ledger_id': ledger['ledger_id'],
            'timestamp': ledger['timestamp'],
            'amount': float(ledger['amount']),
            'balance': int(ledger['balance']),
            'currency': ledger['currency'],
            'type': ledger['type'],
            'order_id': int(extract('order_id')),
            'instrument_id': extract('instrument_id'),

        })
        self.db_conn.commit()
        pass


if __name__ == '__main__':
    init_global_logger()
    args = argparse.ArgumentParser(
        description='Crawl completed order from OKEX')
    args.add_argument('--currency',
                      help='Currency to crawl, BTC, ETH, etc',
                      default='ETH',
                      required=True)
    args.add_argument('--db',
                      default='dev_db.db',
                      help='Sqlite3 DB file to store the crawled orders',
                      required=True)
    args = args.parse_args()

    signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))

    crawler = BillCrawler(args.currency, args.db)
    crawler.crawl()
