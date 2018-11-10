COINS=(btc eth ltc bch xrp)
for coin in "${COINS[@]}"
do
    python3 main.py --symbol=$coin
done
