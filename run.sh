COINS=(btc eth ltc bch xrp)
for coin in "${COINS[@]}"
do
    python3 __main__.py --symbol=$coin
done
