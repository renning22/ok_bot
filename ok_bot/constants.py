MOVING_AVERAGE_TIME_WINDOW_IN_SECOND = 60 * 10  # 10 minutes

# Trader
MIN_TIME_WINDOW_IN_SECOND = 60 * 10  # 10 minutes
INSUFFICIENT_MARGIN_COOL_DOWN_SECOND = 60 * 10  # 10 minutes

# Arbitrage
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
