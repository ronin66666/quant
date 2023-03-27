import os

import pandas as pd
import datetime


# 获取历史数据
# exchange: 交易所对象
# symbol: 交易对
# timeframe: 时间周期
# since: 数据起始时间(过去的某一时间)，比如获取2021年1月1日之后的数据，可以使用int(datetime.datetime(2021, 1, 1).timestamp()) * 1000
# limit: 获取数据的数量，最大为1000
# return: DataFrame对象，包含以下字段：'timestamp', 'open', 'high', 'low', 'close', 'volume', 'date
def get_exchange_data(exchange, symbol, timeframe, since, limit):
    data = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    # df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms') + pd.Timedelta(hours=8)  # 转换为 UTC+8 时区

    return df


# 获取指定日期之后的历史数据
# exchange: 交易所对象
# start_date: 开始日期，格式为'YYYY-MM-DD'
# symbol: 交易对
# timeframe: 时间周期
# csv_file_prefix: CSV文件名前缀
# return: DataFrame对象，包含以下字段：'timestamp', 'open', 'high', 'low', 'close', 'volume', 'date
def fetch_data_by_date(exchange, start_date, symbol='BTC/USDT', timeframe='1h', csv_file_prefix=''):
    # 将输入的日期转换为datetime对象
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    # 计算从输入日期到现在的天数
    days_ago = (datetime.datetime.now() - start_date).days

    # 调用fetch_and_save_data函数，传入相应的参数
    return fetch_and_save_data(exchange, symbol, timeframe, csv_file_prefix, days_ago)


# 获取历史数据，并保存到CSV文件中
# exchange: 交易所对象
# symbol: 交易对
# timeframe: 时间周期 格式为'1h'，'4h'，'1d', '15m' 等
# csv_file_prefix: CSV文件名前缀
# days_ago: 从过去的多少天开始获取数据
# return: DataFrame对象，包含以下字段：'timestamp', 'open', 'high', 'low', 'close', 'volume', 'date
def fetch_and_save_data(exchange, symbol='BTC/USDT', timeframe='1h', csv_file_prefix='', days_ago=30):
    csv_file = f'{csv_file_prefix}{symbol.replace("/", "_")}_{timeframe}_data.csv'

    if os.path.exists(csv_file):
        # 如果CSV文件已经存在，则从CSV中加载数据
        data = pd.read_csv(csv_file)

        # 获取本地数据的最新时间
        since = int(data.iloc[-1]['timestamp']) + exchange.parse_timeframe(timeframe) * 1000

        # 获取本地数据的最新日期
        last_date = data.iloc[-1]['date']
        print("本地最新数据时间为：", last_date)

    else:
        # 如果CSV文件不存在，则创建一个新的空的DataFrame
        data = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'date'])

        # 设置初始since值为过去30天
        since = int((datetime.datetime.now() - datetime.timedelta(days=days_ago)).timestamp()) * 1000

    limit = 1000  # 获取数据的数量，最大为1000

    while True:
        # 获取新数据
        new_data = get_exchange_data(exchange, symbol, timeframe, since, limit)

        if not new_data.empty:
            # 将新数据拼接到data中
            data = pd.concat([data, new_data], ignore_index=True)

            # 去重
            data.drop_duplicates(subset=['timestamp'], inplace=True)

            # 更新since值
            since = int(new_data.iloc[-1]['timestamp']) + exchange.parse_timeframe(timeframe) * 1000

            # 当获取到的数据量小于limit时，表示已经获取到最新数据
            if len(new_data) < limit:
                break
        else:
            break

    # 将数据保存到CSV文件
    data.to_csv(csv_file, index=False)

    # 返回数据
    return data
