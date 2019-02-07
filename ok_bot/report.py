import pandas as pd

from . import constants, singleton


class Report:
    def __init__(self,
                 transaction_id,
                 slow_instrument_id,
                 fast_instrument_id,
                 logger):
        self.transaction_id = transaction_id
        self.logger = logger
        self.slow_instrument_id = slow_instrument_id
        self.slow_open_order_id = None
        self.slow_close_order_id = None
        self.fast_instrument_id = fast_instrument_id
        self.fast_open_order_id = None
        self.fast_close_order_id = None

        # Result table
        self.table = pd.DataFrame()

    async def report_profit(self):
        """Returns the net profit (in unit of coins)"""
        if self.slow_open_order_id:
            self.table = self.table.append(
                await self._retrieve_order_info_and_log_to_db(
                    'slow_open_order',
                    self.slow_open_order_id,
                    self.slow_instrument_id)
            )
        if self.slow_close_order_id:
            self.table = self.table.append(
                await self._retrieve_order_info_and_log_to_db(
                    'slow_close_order',
                    self.slow_close_order_id,
                    self.slow_instrument_id)
            )
        if self.fast_open_order_id:
            self.table = self.table.append(
                await self._retrieve_order_info_and_log_to_db(
                    'fast_open_order',
                    self.fast_open_order_id,
                    self.fast_instrument_id)
            )
        if self.fast_close_order_id:
            self.table = self.table.append(
                await self._retrieve_order_info_and_log_to_db(
                    'fast_close_order',
                    self.fast_close_order_id,
                    self.fast_instrument_id)
            )

        if len(self.table) == 0:
            self.logger.info('[REPORT] empty transaction')
            return 0

        self.table['contract_val'] = self.table['contract_val'].astype('int64')
        self.table['fee'] = self.table['fee'].astype('float64')
        self.table['filled_qty'] = self.table['filled_qty'].astype('int64')
        self.table['leverage'] = self.table['leverage'].astype('int64')
        self.table['price_avg'] = self.table['price_avg'].astype('float64')
        self.table['status'] = self.table['status'].astype('int64')
        self.table['type'] = self.table['type'].astype('int64')

        self.logger.info('[REPORT] orders:\n%s', self.table)

        all_types = set(self.table['type'])

        two_opposite_orders = (
            len(self.table) == 2 and (
                all_types == set([1, 3]) or all_types == set([2, 4])
            )
        )

        four_different_orders = (
            len(self.table) == 4 and all_types == set([1, 2, 3, 4])
        )

        if not two_opposite_orders and not four_different_orders:
            self.logger.fatal('[REPORT] ORPHAN ORDERS!')
            raise RuntimeError('[REPORT] ORPHAN ORDERS!')
        else:
            net_profit = 0.0
            for index, row in self.table.iterrows():
                contract_val = row['contract_val']
                fee = row['fee']
                filled_qty = row['filled_qty']
                leverage = row['leverage']
                price_avg = row['price_avg']
                type = row['type']

                # margin_coins (before leveraged)
                margin_coins = (
                    (contract_val / leverage) * filled_qty / price_avg
                )
                if type == constants.ORDER_TYPE_CODE__OPEN_LONG:
                    net_profit += margin_coins
                elif type == constants.ORDER_TYPE_CODE__CLOSE_LONG:
                    net_profit -= margin_coins
                elif type == constants.ORDER_TYPE_CODE__OPEN_SHORT:
                    net_profit -= margin_coins
                elif type == constants.ORDER_TYPE_CODE__CLOSE_SHORT:
                    net_profit += margin_coins
                net_profit += fee
            return net_profit


    async def _retrieve_order_info_and_log_to_db(self,
                                                 index,
                                                 order_id,
                                                 instrument_id):
        """Returns as a pandas table(row)"""
        ret = await singleton.rest_api.get_order_info(
            order_id, instrument_id)
        assert int(order_id) == int(ret.get('order_id'))
        singleton.db.async_update_order(
            order_id=ret.get('order_id'),
            transaction_id=self.transaction_id,
            comment='final',
            status=ret.get('status'),
            size=ret.get('size'),
            filled_qty=ret.get('filled_qty'),
            price=ret.get('price'),
            price_avg=ret.get('price_avg'),
            fee=ret.get('fee'),
            type=ret.get('type'),
            timestamp=ret.get('timestamp')
        )
        return pd.DataFrame(ret, index=[index])
