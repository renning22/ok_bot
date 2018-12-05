contract_types = ['this_week', 'next_week', 'quarter']

columns = ['timestamp', 'source']
for contract in contract_types:
    for side in ['bid', 'ask']:
        for depth in ['', '2', '3']:
            columns.append(f'{contract}_{side}{depth}_price')
            columns.append(f'{contract}_{side}{depth}_vol')

columns_best_asks = [col for col in columns if 'ask_price' in col]
columns_best_bids = [col for col in columns if 'bid_price' in col]

columns_cross = []
for ask in columns_best_asks:
    for bid in columns_best_bids:
        if ask[:4] != bid[:4]:  # if not same contract_type
            columns_cross.append(f'{ask}-{bid}')

if __name__ == '__main__':
    import pprint
    pprint.pprint(columns)
    pprint.pprint(columns_best_asks)
    pprint.pprint(columns_best_bids)
    pprint.pprint(columns_cross)
