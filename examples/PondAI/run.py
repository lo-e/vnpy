# encoding: UTF-8

from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import copy

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
    file_name = "naive_prediction.csv"
    symbol_set = set()
    symbol_data_list = []
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
                symbol_data_list.append(symbol_data)
    if not symbol_data_list:
        return

    # 回测引擎参数设置
    engine = BacktestingEngine()
    engine.setPeriod(datetime(2021, 9, 15), datetime(2023, 12, 31))
    engine.tradingStart = datetime(2022, 1, 1)
    figSavedName = ""
    if figSavedName:
        figSavedName = f"figSaved{DIR_SYMBOL}{figSavedName}"

    # 开始回测
    engine.initListPortfolio(symbol_data_list, 60000)
    engine.loadData()
    engine.runBacktesting()
    engine.showResult(figSavedName)

if __name__ == "__main__":
    one()
