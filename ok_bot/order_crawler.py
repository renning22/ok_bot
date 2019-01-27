"""
This module defines a crawler that periodically crawl completed orders from
OKEX. Then store the result to DB. OKEX only returns the most recent 100 orders.
This crawler will run outside of the arbitrage program.
"""
import time
import sqlite3
from absl import logging
import argparse

from .rest_api_v3 import RestApiV3

SLEEP_TIME_IN_SECOND = 60


class OrderCrawler:
    def __init__(self, currency, db_file):
        self.api = RestApiV3()
        self.db_conn = sqlite3.connect(db_file)
        self.all_instrument_ids = self.api.all_instrument_ids(currency)

    def crawl(self):
        while True:
            orders = self.api.completed_orders(self.all_instrument_ids)
            logging.info(f'{len(orders)} orders crawled from OKEX')
            for order in orders:
                self.insert_order_to_db(order)
            logging.info(f'All orders to DB, will sleep '
                         f'for {SLEEP_TIME_IN_SECOND} seconds')
            time.sleep(SLEEP_TIME_IN_SECOND)

    def insert_order_to_db(self, order):
        sql = '''
            INSERT OR REPLACE INTO okex_reported_orders(
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


if __name__ == '__main__':
    logging.get_absl_logger().setLevel(logging.DEBUG)
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
    crawler = OrderCrawler(args.currency, args.db)
    crawler.crawl()
