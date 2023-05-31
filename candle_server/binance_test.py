import requests
import json

market = 'BTCUSDT'
tick_interval = '1h'

url = 'https://api.binance.com/api/v3/klines?symbol='+market+'&interval='+tick_interval
data = requests.get(url).json()

for candle in data:
    print(candle[0])
# print(json.dumps(data))
