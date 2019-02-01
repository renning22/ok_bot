import argparse
import datetime
import sqlite3

class Accounting:
    def __init__(self, db_file, start_date_str, end_date_str):
        self.db_conn = sqlite3.connect(db_file)
        self.db_conn.row_factory = sqlite3.Row
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str + ' 23:59:59'
        self.orders = self.fetch_orders()
        self.transactions = self.fetch_transactions()
        self.trans_order = {}
        self.order_trans = {}
        self.orphan_orders = []
        self.transaction_and_order_match()

    def check(self):
        pass

    def check_orphan_orders(self):
        print(f'{len(self.orphan_orders)} found')
        for order in self.orphan_orders:
            print('=' * 50)
            for col in order.keys():
                print(f'{col}: {order[col]}')
            print('=' * 50)
            print()

    def transaction_and_order_match(self):
        SQL = '''
            SELECT
                order_id
                , transaction_id
            FROM runtime_orders
            WHERE
                last_update_time >= :start_date
                AND last_update_time <= :end_date
        '''
        cursor = self.db_conn.cursor()
        cursor.execute(SQL, {
            'start_date': self.start_date_str,
            'end_date': self.end_date_str,
        })
        matches = cursor.fetchall()
        self.trans_order = {}
        self.order_trans = {}
        for m in matches:
            order_id = int(m['order_id'])
            trans_id = m['transaction_id']
            assert order_id not in self.order_trans or \
                self.order_trans[order_id] == trans_id, \
                f'Order({order_id}) ownership inconsistent, claimed by ' \
                f'{trans_id} and {self.order_trans[order_id]}'
            self.order_trans[order_id] = trans_id
            order_set = self.trans_order.setdefault(trans_id, set())
            order_set.add(order_id)

        for order in self.orders:
            if order['order_id'] not in self.order_trans:
                self.orphan_orders.append(order)

    def fetch_transactions(self):
        SQL = '''
            SELECT *
            FROM runtime_transactions
            WHERE last_update_time >= :start_date
              AND last_update_time <= :end_date
        '''
        cursor = self.db_conn.cursor()
        cursor.execute(SQL, {
            'start_date': self.start_date_str,
            'end_date': self.end_date_str,
        })
        return cursor.fetchall()

    def fetch_orders(self):
        """
        :return: Orders crawled from OKEX. Columns:
            ['order_id',
             'instrument_id',
             'size',
             'timestamp',
             'filled_qty',
             'fee',
             'price',
             'price_avg',
             'status',
             'type',
             'contract_val',
             'leverage']
        """
        SQL = '''
            SELECT *
            FROM reported_orders
            WHERE timestamp >= :start_date
              AND timestamp <= :end_date
        '''
        cursor = self.db_conn.cursor()
        cursor.execute(SQL, {
            'start_date': self.start_date_str,
            'end_date': self.end_date_str,
        })
        return cursor.fetchall()


if __name__ == '__main__':
    args = argparse.ArgumentParser(description='Offline account checking')
    args.add_argument('--db', default='dev.db',
                      help='Sqlite3 DB file', required=True)
    args.add_argument('--start', help='Start date string, e.x. 2018-12-01',
                      default='2018-09-01', required=True)
    args.add_argument('--end', help='End date string, e.x. 2018-12-01',
                      default=datetime.datetime.now().strftime('%Y-%m-%d'))
    args = args.parse_args()

    accounting = Accounting(args.db, args.start, args.end)
    accounting.check_orphan_orders()



