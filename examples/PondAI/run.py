# encoding: UTF-8

from datetime import datetime
from time import time
from engine import BacktestingEngine
import csv
import os
import pandas as pd
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import DIR_SYMBOL
import csv
import shutil


def backtesting(
    signal_file: str, from_dt: str = "2009-01-01", to_dt: str = "2024-12-31"
):
    # 读取文件，生成回测合约参数
    print(f"{datetime.now()}\t开始信号数据读取")
    file_name = f"data{DIR_SYMBOL}{signal_file}"
    symbol_set = set()
    setting_list = []
    symbol_signal_dict = {}
    file_start = time()
    log_time_gap = 60
    log_time = 0
    file_read_count = 0
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            file_read_count += 1
            time_cost = int(time() - file_start)
            if time_cost > log_time:
                log_time += log_time_gap
                print(f"信号数据读取统计：{file_read_count}")

            # 交易参数设置
            symbol = row["symbol"]
            if symbol not in symbol_set:
                symbol_set.add(symbol)
                symbol_data = {
                    "symbol": symbol,
                    "priceTick": 0,
                    "min_volume": 0.000001,
                    "variableCommission": 0.0005,
                    "slippage": 1,
                }
                setting_list.append(symbol_data)

            # 交易信号统计
            symbol_signal_list = symbol_signal_dict.get(symbol, [])
            symbol_signal_data = row.copy()
            symbol_signal_data.pop("symbol")
            dt_str = symbol_signal_data["datetime"]
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            symbol_signal_data["datetime"] = dt
            symbol_signal_list.append(symbol_signal_data)
            symbol_signal_dict[symbol] = symbol_signal_list

    time_cost = time() - file_start
    print(f"信号数据读取总耗时：{time_cost}")
    if not setting_list:
        return

    # 信号数据按时间排序
    for symbol, signal_list in symbol_signal_dict.items():
        signal_df = pd.DataFrame(signal_list)
        df_sorted = signal_df.sort_values(by="datetime")
        symbol_signal_dict[symbol] = df_sorted

    # 回测引擎参数设置
    from_dt = datetime.strptime(from_dt, "%Y-%m-%d")
    to_dt = datetime.strptime(to_dt, "%Y-%m-%d")
    engine = BacktestingEngine()
    engine.setPeriod(from_dt, to_dt)
    engine.symbol_signal_dict = symbol_signal_dict
    figSavedName = "result_figure"
    if figSavedName:
        fig_saved_path = f"figSaved{DIR_SYMBOL}"
        if os.path.exists(fig_saved_path):
            shutil.rmtree(fig_saved_path)
            os.makedirs(fig_saved_path)
        else:
            os.makedirs(fig_saved_path)
        figSavedName = f"{fig_saved_path}{figSavedName}"

    # 开始回测
    backtesting_start = time()
    engine.initListPortfolio(setting_list, 10000000)
    engine.loadData()
    engine.runBacktesting()
    time_cost = time() - backtesting_start
    print(f"回测总耗时：{time_cost}")

    # 获取合约交易数据
    print(f"{datetime.now()}\t开始保存交易数据")
    trades_save_start = time()
    symbol_trade_dic = {}
    for symbol in engine.symbolList:
        symbol_trade_list = symbol_trade_dic.get(symbol, [])
        trade_data_list = engine.getTradeData(symbol)
        for trade in trade_data_list:
            direction = (
                "LONG"
                if trade.direction == Direction.LONG
                else ("SHORT" if trade.direction == Direction.SHORT else "NET")
            )
            offset = (
                "OPEN"
                if trade.offset == Offset.OPEN
                else ("NONE" if trade.offset == Offset.NONE else "CLOSE")
            )
            trade_data = {
                "symbol": trade.symbol,
                "datetime": trade.dt,
                "direction": direction,
                "offset": offset,
                "volume": trade.volume,
                "price": trade.price,
            }
            symbol_trade_list.append(trade_data)
        symbol_trade_dic[symbol] = symbol_trade_list

    # 保存合约交易数据
    symbol_trade_dir_path = f"data{DIR_SYMBOL}symbol_trades{DIR_SYMBOL}"
    if os.path.exists(symbol_trade_dir_path):
        shutil.rmtree(symbol_trade_dir_path)
        os.makedirs(symbol_trade_dir_path)
    else:
        os.makedirs(symbol_trade_dir_path)
    for symbol, trade_list in symbol_trade_dic.items():
        if len(trade_list):
            fieldNames = [
                "datetime",
                "symbol",
                "direction",
                "offset",
                "volume",
                "price",
            ]
            filePath = f"{symbol_trade_dir_path}{symbol}.csv"
            with open(filePath, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                writer.writerows(trade_list)

    time_cost = time() - trades_save_start
    print(f"保存交易数据总耗时：{time_cost}")

    # 展示图表
    engine.showResult(figSavedName)


if __name__ == "__main__":
    backtesting(signal_file="naive_prediction.csv", from_dt="2021-01-01", to_dt="2024-12-31")
