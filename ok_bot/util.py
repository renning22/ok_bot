def amount_margin(ask_stack, bid_stack, condition):
    # TODO(luanjunyi): add unittest
    available_amount = 0
    ask_stack = list(ask_stack)
    bid_stack = list(bid_stack)
    bid_copy = [[t[0], t[1]] for t in bid_stack]
    for ask_price, ask_volume in ask_stack:
        for i, (bid_price, bid_volume) in enumerate(bid_copy):
            if not condition(ask_price, bid_price) or ask_volume <= 0 or bid_volume <= 0:
                continue
            amount = min(ask_volume, bid_volume)
            ask_volume -= amount
            bid_copy[i][1] -= amount
            available_amount += amount
    return available_amount
