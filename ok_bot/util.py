from .order_book import AvailableOrder


def calculate_amount_margin(ask_stack, bid_stack, condition):
    # TODO(luanjunyi): add unittest
    available_amount = 0
    bid_copy = [AvailableOrder(price=order.price, volume=order.volume)
                for order in bid_stack]
    for ask_order in ask_stack:
        ask_price, ask_volume = ask_order.price, ask_order.volume
        for i, bid_order in enumerate(bid_copy):
            bid_price, bid_volume = bid_order.price, bid_order.volume
            if not condition(ask_price,
                             bid_price) or ask_volume <= 0 or bid_volume <= 0:
                continue
            amount = min(ask_volume, bid_volume)
            ask_volume -= amount
            bid_copy[i].volume -= amount
            available_amount += amount
    return available_amount
