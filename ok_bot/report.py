import logging
import pprint
from collections import namedtuple

from . import constants, logger, singleton


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

    def generate(self):
        pass

    async def _retrieve_order_info_and_log_to_db(self, order_id, instrument_id):
        ret = await singleton.rest_api.get_order_info(
            self._order_id, self._instrument_id)
        self.logger.info(
            '[POSTMORTEM] order info from rest api:\n%s', pprint.pformat(ret))

        status = int(ret.get('status', None))
        if status == constants.ORDER_STATUS_CODE__CANCELLED:
            self.logger.info(
                '[POSTMORTEM] %s order has been cancelled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PENDING:
            self.logger.info(
                '[POSTMORTEM] %s order is still pending', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__PARTIALLY_FILLED:
            self.logger.info(
                '[POSTMORTEM] %s order is partially filled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__FULFILLED:
            self.logger.info(
                '[POSTMORTEM] %s order is fulfilled', self._order_id)
        elif status == constants.ORDER_STATUS_CODE__CANCEL_IN_PROCESS:
            self.logger.info(
                '[POSTMORTEM] %s order is being cancelled in progress',
                order_id)
        else:
            self.logger.critical('unknown status code: %s', status)

        assert int(self._order_id) == int(ret.get('order_id'))
        singleton.db.async_update_order(
            order_id=ret.get('order_id'),
            transaction_id=self._transaction_id,
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


def _testing():
    from ok_bot.mock import AsyncMock
    logger.init_global_logger(log_level=logging.INFO)
    singleton.initialize_objects_with_mock_trader_and_dev_db('ETH')
    singleton.rest_api = AsyncMock()

    report = Report(transaction_id=self.id,
                    slow_instrument_id=slow_leg.instrument_id,
                    fast_instrument_id=fast_leg.instrument_id,
                    logger=logging)
    report.generate()


if __name__ == '__main__':
    _testing()
