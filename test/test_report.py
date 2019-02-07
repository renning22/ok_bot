import logging
import unittest
from unittest.mock import call

from ok_bot import constants, logger, singleton
from ok_bot.mock import AsyncMock
from ok_bot.report import Report


def expected_net_profit_way_1():
    long_margin = 10 / 10 / 101.111
    short_margin = 10 / 10 / 103.333
    long_margin_after = 10 / 10 / 102.222
    short_margin_after = 10 / 10 / 104.444
    total_fee = 4 * -0.01
    return sum([
        long_margin - long_margin_after,
        short_margin_after - short_margin,
        total_fee,
    ])


def expected_net_profit_way_2():
    long_margin = 10 / 10 / 101.111
    short_margin = 10 / 10 / 103.333
    long_gain_rate = (102.222 - 101.111) / 101.111
    short_gain_rate = -(104.444 - 103.333) / 103.333
    total_fee = 4 * -0.01
    return sum([
        long_margin * long_gain_rate,
        short_margin * short_gain_rate,
        total_fee,
    ])


class TestArbitrageExecution(unittest.TestCase):
    def setUp(self):
        logger.init_global_logger(log_level=logging.INFO)
        singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
        singleton.rest_api = AsyncMock()
        singleton.rest_api.get_order_info.side_effect = [
            {'contract_val': '10',
             'fee': '-0.01',
             'filled_qty': '1',
             'instrument_id': 'ETH-USD-190201',
             'leverage': '10',
             'order_id': '10001',
             'price': '101.111',
             'price_avg': '101.111',
             'size': '1',
             'status': str(constants.ORDER_STATUS_CODE__FULFILLED),
             'timestamp': '2019-02-06T03:44:01.000Z',
             'type': str(constants.ORDER_TYPE_CODE__OPEN_LONG)},
            {'contract_val': '10',
             'fee': '-0.01',
             'filled_qty': '1',
             'instrument_id': 'ETH-USD-190201',
             'leverage': '10',
             'order_id': '10002',
             'price': '102.222',
             'price_avg': '102.222',
             'size': '1',
             'status': str(constants.ORDER_STATUS_CODE__FULFILLED),
             'timestamp': '2019-02-06T03:44:02.000Z',
             'type': str(constants.ORDER_TYPE_CODE__CLOSE_LONG)},
            {'contract_val': '10',
             'fee': '-0.01',
             'filled_qty': '1',
             'instrument_id': 'ETH-USD-190329',
             'leverage': '10',
             'order_id': '10003',
             'price': '103.333',
             'price_avg': '103.333',
             'size': '1',
             'status': str(constants.ORDER_STATUS_CODE__FULFILLED),
             'timestamp': '2019-02-06T03:44:03.000Z',
             'type': str(constants.ORDER_TYPE_CODE__OPEN_SHORT)},
            {'contract_val': '10',
             'fee': '-0.01',
             'filled_qty': '1',
             'instrument_id': 'ETH-USD-190329',
             'leverage': '10',
             'order_id': '10004',
             'price': '104.444',
             'price_avg': '104.444',
             'size': '1',
             'status': str(constants.ORDER_STATUS_CODE__FULFILLED),
             'timestamp': '2019-02-06T03:44:04.000Z',
             'type': str(constants.ORDER_TYPE_CODE__CLOSE_SHORT)},
        ]

    def tearDown(self):
        singleton.db.shutdown(wait=True)

    def test_net_profit(self):
        async def _testing_coroutine():
            report = Report(
                transaction_id='11111111-1111-1111-1111-111111111111',
                slow_instrument_id='ETH-USD-190201',
                fast_instrument_id='ETH-USD-190329',
                logger=logging)
            report.slow_open_order_id = 10001
            report.slow_close_order_id = 10002
            report.fast_open_order_id = 10003
            report.fast_close_order_id = 10004
            net_profit = await report.report_profit()

            # Cross-validation (there's floating point error)
            self.assertAlmostEqual(
                expected_net_profit_way_1(),
                expected_net_profit_way_2(),
                places=6
            )
            self.assertAlmostEqual(
                net_profit,
                expected_net_profit_way_1(),
                places=6
            )

            singleton.rest_api.get_order_info.assert_has_calls([
                call(10001, 'ETH-USD-190201'),
                call(10002, 'ETH-USD-190201'),
                call(10003, 'ETH-USD-190329'),
                call(10004, 'ETH-USD-190329'),
            ])

        singleton.loop.run_until_complete(_testing_coroutine())


if __name__ == '__main__':
    unittest.main()
