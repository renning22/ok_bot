MOVING_AVERAGE_TIME_WINDOW_IN_SECOND = 60 * 4  # 4 minutes

TRADING_VOLUME = 4  # 4 "张"
SINGLE_UNIT_IN_USD = {
    'BTC': 100.0,
    'ETH': 10.0,
}

# Strategy
MIN_TIME_WINDOW_IN_SECOND = 60 * 1  # 1 minutes
MIN_ESTIMATE_PROFIT = 1e-5
INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 60 * 10  # 10 minutes
AMOUNT_SHRINK = 0.33

ORDER_EXECUTOR_SAFE_PRICE_RATE = 0.0004
PRICE_PREDICTION_WINDOW_SECOND = 5

# seconds
TICK_STALENESS_THRESHOLD = 1.0

SIMPLE_STRATEGY_RETURN_RATE_THRESHOLD = 0

# X >= 1 (standard deviation) is 84% percentiles in standard gaussian
# distribution, 34% deviated from center.
SIMPLE_STRATEGY_ZSCORE_THRESHOLD = 1.0

# Bounds back distance.
SIMPLE_STRATEGY_RESILIANCE = 0.65

# Arbitrage
# According to https://www.okex.com/pages/products/fees.html, for Lv1
# the fee is either 0.02% or 0.03%. We use 0.03% as estimate.
FEE_RATE = 0.0003
# Slow side should be must tight because when it timeout, there's no fee. Also
# there are many cases when the order is fulfilled but OKEX didn't send update
# via websocket. We don't know until we try revoke the order. Then we found it's
# actually fulfilled and then the opportunity is gone for the fast leg. We end
# up losing the fee in open/close the slow side.
SLOW_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND = 5
FAST_LEG_ORDER_FULFILLMENT_TIMEOUT_SECOND = 10
CLOSE_POSITION_ORDER_TIMEOUT_SECOND = 10

CLOSE_THRESHOLDS = {
    ('this_week', 'next_week'): 0.1,
    ('next_week', 'quarter'): 0.025,
    ('this_week', 'quarter'): 0.025,
}
# When to close arbitrage
MIN_AVAILABLE_AMOUNT_FOR_CLOSING_ARBITRAGE = 5
MIN_AVAILABLE_AMOUNT_FOR_OPENING_ARBITRAGE = 5
PRICE_CONVERGE_TIMEOUT_IN_SECOND = 60 * 10  # 10 minutes

LONG = 'LONG'
SHORT = 'SHORT'

# API code
REST_API_ERROR_CODE__MARGIN_NOT_ENOUGH = 32016
REST_API_ERROR_CODE__PENDING_ORDER_NOT_EXIST = 32004
REST_API_ERROR_CODE__NOT_ENOUGH_POSITION_TO_CLOSE = 32014

# 订单状态(-1.撤单成功；0:等待成交 1:部分成交 2:全部成交
ORDER_STATUS_CODE__CANCELLED = -1
ORDER_STATUS_CODE__PENDING = 0
ORDER_STATUS_CODE__PARTIALLY_FILLED = 1
ORDER_STATUS_CODE__FULFILLED = 2
ORDER_STATUS_CODE__CANCEL_IN_PROCESS = 4

# 订单类型(1:开多 2:开空 3:平多 4:平空)
ORDER_TYPE_CODE__OPEN_LONG = 1
ORDER_TYPE_CODE__OPEN_SHORT = 2
ORDER_TYPE_CODE__CLOSE_LONG = 3
ORDER_TYPE_CODE__CLOSE_SHORT = 4
