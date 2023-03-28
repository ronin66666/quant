# main.py

import ccxt

from data import fetch_exchange_data

# 创建交易所实例
exchange = ccxt.binance({
    'rateLimit': 1000,
    'enableRateLimit': True,
})

# 获取历史数据
# data = fetch_exchange_data.fetch_and_save_data(exchange, symbol='BTC/USDT', timeframe='15m', days_ago=10)
data = fetch_exchange_data.fetch_data_by_date(exchange, start_date='2021-01-01',  symbol='BTC/USDT', timeframe='5m')
# 输出数据
print(data.head(5))

# 获取1小时周期的历史数据
# data_1h = fetch_exchange_data.fetch_and_save_data(exchange, symbol='BTC/USDT', timeframe='1h')
#
# # 获取4小时周期的历史数据
# data_4h = fetch_exchange_data.fetch_and_save_data(exchange, symbol='BTC/USDT', timeframe='4h')
#
# # 获取1天周期的历史数据
# data_1d = fetch_exchange_data.fetch_and_save_data(exchange, symbol='BTC/USDT', timeframe='1d')