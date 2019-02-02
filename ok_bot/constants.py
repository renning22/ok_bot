MOVING_AVERAGE_TIME_WINDOW_IN_SECOND = 60 * 10  # 10 minutes
TRADING_VOLUME = 1  # 1 "张"
SINGLE_UNIT_IN_USD = {
    'BTC': 100.0,
    'ETH': 10.0,
}

# Trader
MIN_TIME_WINDOW_IN_SECOND = 60 * 10  # 10 minutes
MIN_ESTIMATE_PROFIT = 1e-5
INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 60 * 10  # 10 minutes

# Arbitrage
# According to https://www.okex.com/pages/products/fees.html, for Lv1
# the fee is either 0.02% or 0.03%. We use 0.03% as estimate.
FEE_RATE = 0.0003
SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND = 20
FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND = 20
OPEN_THRESHOLDS = {
    ('this_week', 'next_week'): 0.25,
    ('next_week', 'quarter'): 0.1,
    ('this_week', 'quarter'): 0.1,
}
CLOSE_THRESHOLDS = {
    ('this_week', 'next_week'): 0.1,
    ('next_week', 'quarter'): 0.025,
    ('this_week', 'quarter'): 0.025,
}
# When to close arbitrage
MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE = 5
MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE = 5
PRICE_CONVERGE_TIMEOUT_IN_SECOND = 60 * 60  # 60 minutes

LONG = 'LONG'
SHORT = 'SHORT'

# API code
REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH = 32016
REST_API_ERROR_CODE__PENDING_ORDER_NOT_EXIST = 32004

# -1.撤单成功；0:等待成交 1:部分成交 2:全部成交
ORDER_STATUS_CODE__CANCELLED = -1
ORDER_STATUS_CODE__PENDING = 0
ORDER_STATUS_CODE__PARTIALLY_FILLED = 1
ORDER_STATUS_CODE__FULFILLED = 2
ORDER_STATUS_CODE__CANCEL_IN_PROCESS = 4
