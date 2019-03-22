import pandas as pd

from . import constants, singleton

ORDER_TYPE_TO_STRING = {
    constants.ORDER_TYPE_CODE__OPEN_LONG: 'long+',
    constants.ORDER_TYPE_CODE__OPEN_SHORT: 'short+',
    constants.ORDER_TYPE_CODE__CLOSE_LONG: 'long-',
    constants.ORDER_TYPE_CODE__CLOSE_SHORT: 'short-',
}


def get_order_gain(order):
    val = order['filled_qty'] * order['contract_val'] / order['price_avg']
    if order['type'] in (constants.ORDER_TYPE_CODE__CLOSE_LONG,
                         constants.ORDER_TYPE_CODE__OPEN_SHORT):
        val *= -1.0
    return val + order['fee']


def get_price_slippage(order):
    val = ((order['price_avg'] - order['original_price']) /
           order['original_price'])
    if order['type'] in (constants.ORDER_TYPE_CODE__CLOSE_LONG,
                         constants.ORDER_TYPE_CODE__OPEN_SHORT):
        val *= -1.0
    return val


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

        self.slow_open_prices = []
        self.fast_open_prices = []
        self.slow_close_prices = []
        self.fast_close_prices = []

        # Result table
        self.table = pd.DataFrame()

    def __str__(self):
        if self.table.empty:
            return '[no orders]'
        ret = ''
        ret += f'slippage: {self.slippage * 100:.3f}%\n'
        if self.slow_open_prices:
            ret += 'slow+ {:6} {} -> {}\n'.format(
                self.table.loc['slow_open']['direction'],
                self.slow_open_prices,
                self.table.loc['slow_open']['price_avg'])
        if self.slow_close_prices:
            ret += 'slow- {:6} {} -> {}\n'.format(
                self.table.loc['slow_close']['direction'],
                self.slow_close_prices,
                self.table.loc['slow_close']['price_avg'])
        if self.fast_open_prices:
            ret += 'fast+ {:6} {} -> {}\n'.format(
                self.table.loc['fast_open']['direction'],
                self.fast_open_prices,
                self.table.loc['fast_open']['price_avg'])
        if self.fast_close_prices:
            ret += 'fast- {:6} {} -> {}\n'.format(
                self.table.loc['fast_close']['direction'],
                self.fast_close_prices,
                self.table.loc['fast_close']['price_avg'])
        ret += self.table.to_string()
        return ret

    @property
    def slippage(self):
        if self.table.empty:
            return 0
        return self.table['slippage'].sum()

    async def report_profit(self):
        """Returns the net profit (in unit of coins)"""
        if self.slow_open_order_id:
            order_info = await self._retrieve_order_info_and_log_to_db(
                'slow_open',
                self.slow_open_order_id,
                self.slow_instrument_id)
            order_info['original_price'] = self.slow_open_prices[0]
            self.table = self.table.append(order_info)
        if self.slow_close_order_id:
            order_info = await self._retrieve_order_info_and_log_to_db(
                'slow_close',
                self.slow_close_order_id,
                self.slow_instrument_id)
            order_info['original_price'] = self.slow_close_prices[0]
            self.table = self.table.append(order_info)
        if self.fast_open_order_id:
            order_info = await self._retrieve_order_info_and_log_to_db(
                'fast_open',
                self.fast_open_order_id,
                self.fast_instrument_id)
            order_info['original_price'] = self.fast_open_prices[0]
            self.table = self.table.append(order_info)
        if self.fast_close_order_id:
            order_info = await self._retrieve_order_info_and_log_to_db(
                'fast_close',
                self.fast_close_order_id,
                self.fast_instrument_id)
            order_info['original_price'] = self.fast_close_prices[0]
            self.table = self.table.append(order_info)

        if len(self.table) == 0:
            self.logger.info('[REPORT] empty transaction')
            return 0

        self.table['contract_val'] = self.table['contract_val'].astype('int64')
        self.table['fee'] = self.table['fee'].astype('float64')
        self.table['filled_qty'] = self.table['filled_qty'].astype('int64')
        self.table['leverage'] = self.table['leverage'].astype('int64')
        self.table['price'] = self.table['price'].astype('float64')
        self.table['price_avg'] = self.table['price_avg'].astype('float64')
        self.table['status'] = self.table['status'].astype('int64')
        self.table['type'] = self.table['type'].astype('int64')
        self.table['direction'] = self.table.apply(
            lambda order: ORDER_TYPE_TO_STRING[order['type']], axis=1)
        self.table['gain'] = self.table.apply(get_order_gain, axis=1)
        self.table['slippage'] = self.table.apply(get_price_slippage, axis=1)

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
            self.logger.critical('[REPORT] ORPHAN ORDERS!')
            raise RuntimeError('[REPORT] ORPHAN ORDERS!')
        else:
            return self.table['gain'].sum()

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
