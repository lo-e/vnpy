# encoding: UTF-8

from datetime import datetime
from time import time
import numpy as np
import matplotlib.pyplot as plt
from engine import BacktestingEngine
from csv import DictReader
import csv
import os
from collections import OrderedDict
import re
from pymongo import MongoClient, ASCENDING
from vnpy.app.cta_strategy.base import DAILY_DB_NAME
import pandas as pd
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import DIR_SYMBOL
import csv

def one():
    # 读取文件，生成回测合约参数
    file_name = f"data{DIR_SYMBOL}naive_prediction.csv"
    symbol_set = set()
    setting_list = []
    symbol_signal_dict = {}
    file_start = time()
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
    print(f"信号数据读取时间：{time_cost}")
    if not setting_list:
        return
    
    # 信号数据按时间排序
    for symbol, signal_list in symbol_signal_dict.items():
        signal_df = pd.DataFrame(signal_list)
        df_sorted = signal_df.sort_values(by='datetime')
        symbol_signal_dict[symbol] = df_sorted

    # 回测引擎参数设置
    engine = BacktestingEngine()
    engine.setPeriod(datetime(2021, 1, 1), datetime(2024, 12, 31))
    engine.symbol_signal_dict = symbol_signal_dict
    figSavedName = ""
    if figSavedName:
        figSavedName = f"figSaved{DIR_SYMBOL}{figSavedName}"

    # 开始回测
    engine.initListPortfolio(setting_list, 10000000)
    engine.loadData()
    engine.runBacktesting()
    engine.showResult(figSavedName)

if __name__ == "__main__":
    one()
