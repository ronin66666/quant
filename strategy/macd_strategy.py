import ccxt
import pandas as pd
import talib
import numpy as np

from data.fetch_exchange_data import fetch_data_by_date


# 创建交易所实例
# data 历史数据 = fetch_data_by_date(ccxt.binance(), start_date='2021-01-01', symbol='BTC/USDT', timeframe='5m')
# return data
def generate_macd_signals(data):
    data["MACD"], data["MACD_signal"], data["MACD_hist"] = talib.MACD(data["close"], fastperiod=12, slowperiod=26,
                                                                      signalperiod=9)

    # 计算MACD的金叉和死叉
    # 1. 当MACD从下往上突破MACD Signal时，为金叉，买入信号
    # 2. 当MACD从上往下跌破MACD Signal时，为死叉，卖出信号
    data['signal'] = 0
    # 生成交易信号
    data['signal'] = np.where(data['MACD'].shift(1) < data['MACD_signal'].shift(1), 1, 0)
    data['signal'] = np.where(data['MACD'].shift(1) > data['MACD_signal'].shift(1), -1, data['signal'])
    data.to_csv('data_with_signals.csv', index=False)
    return data

def backtest(data, strategy, leverage=1, initial_balance=10000, fee_rate=0.001):
    balance = initial_balance
    position = 0
    position_size = 0
    total_fee = 0
    total_profit = 0
    total_loss = 0

    trade_history = []
    data_with_signals = strategy(data)

    for _, row in data_with_signals.iterrows():
        signal = row['signal']
        current_price = row['close'] # 收盘价格

        if signal == 1 and position == 0:  # 买入信号
            position_size = balance / (current_price * leverage)
            fee = position_size * current_price * fee_rate
            balance -= fee
            total_fee += fee
            position = position_size
            buy_price = current_price
            buy_time = row['date']

        elif signal == -1 and position != 0:  # 卖出信号
            sell_price = current_price
            position_value = position * sell_price
            fee = position_value * fee_rate
            balance += position_value - fee
            total_fee += fee
            profit = (position_value - position_size * buy_price) * leverage

            if profit > 0:
                total_profit += profit
            else:
                total_loss += abs(profit)

            trade_history.append({
                'buy_time': buy_time,
                'sell_time': row['date'],
                'buy_price': buy_price,
                'sell_price': sell_price,
                'profit': profit,
                'fee': fee
            })
            position = 0

    win_rate = len([trade for trade in trade_history if trade['profit'] > 0]) / len(trade_history) * 100

    trade_history_df = pd.DataFrame(trade_history)
    trade_history_df.to_csv('trade_history.csv', index=False)

    print(f'Final balance: {balance}')
    print(f'Total fee: {total_fee}')
    print(f'Win rate: {win_rate}%')
    print(f'Total profit: {total_profit}')
    print(f'Total loss: {total_loss}')


# 使用之前的函数获取历史数据
exchange = ccxt.okx({
    'rateLimit': 1000,
    'enableRateLimit': True,
})
symbol = 'BTC/USDT'
timeframe = '15m'
leverage = 1
data = fetch_data_by_date(exchange, '2021-01-01', symbol, timeframe, '../storage/')
generate_macd_signals(data)

# backtest(data, generate_macd_signals, leverage=leverage)


