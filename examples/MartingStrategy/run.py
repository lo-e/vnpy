# encoding: UTF-8

from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import copy

from martingEngine import BacktestingEngine
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
import shutil


def one():
    engine = BacktestingEngine()
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime(2023, 5, 27)
    engine.setPeriod(start_dt, end_dt)
    figSavedName = ""
    if figSavedName:
        figSavedName = f"figSaved{DIR_SYMBOL}{figSavedName}"

    filename = "setting.csv"
    symbolList = []
    with open(filename, errors="ignore") as f:
        r = DictReader(f)
        for d in r:
            symbolList.append(d)
    if not symbolList:
        return
    engine.initListPortfolio(symbolList, 100000)
    engine.loadData()
    engine.runBacktesting()
    engine.showResult(figSavedName)

    # 输出并保存交易数据
    symbol_trade_dic = {}
    for symbol in engine.symbolList:
        symbol_trade_list = symbol_trade_dic.get(symbol, [])
        trade_data_list = engine.getTradeData(symbol)
        # print(f"\n****** {symbol} ******")
        for trade in trade_data_list:
            # print(
            #     "%s\t%s\t%s\t%s\t%s\t%s"
            #     % (
            #         trade.symbol,
            #         trade.dt,
            #         trade.direction.value,
            #         trade.offset.value,
            #         trade.volume,
            #         trade.price,
            #     )
            # )
            trade_data = {
                "symbol": trade.symbol,
                "datetime": trade.dt,
                "direction": trade.direction.value,
                "offset": trade.offset.value,
                "volume": trade.volume,
                "price": trade.price,
            }
            symbol_trade_list.append(trade_data)
        symbol_trade_dic[symbol] = symbol_trade_list

    # 保存合约交易数据
    symbol_trade_dir_path = f"symbol_trades{DIR_SYMBOL}"
    # 先删除原有文件夹，包括其中所有内容
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
            # 文件路径
            filePath = f"{symbol_trade_dir_path}{symbol}.csv"
            with open(filePath, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                # 写入csv文件
                writer.writerows(trade_list)

    # 保存信号交易数据
    signal_trade_dir_path = f"signal_trades{DIR_SYMBOL}"
    # 先删除原有文件夹，包括其中所有内容
    if os.path.exists(signal_trade_dir_path):
        shutil.rmtree(signal_trade_dir_path)
        os.makedirs(signal_trade_dir_path)
    else:
        os.makedirs(signal_trade_dir_path)
    for signal_key, trade_list in engine.portfolio.signalTradesDict.items():
        if len(trade_list):
            # ====== fake ======
            """
            loss_tips = False
            for trade_data in trade_list:
                max_loss_value_ = trade_data["max_loss_value"]
                if max_loss_value_ <= -10000:
                    if not loss_tips:
                        print(f"\n{signal_key} 最大亏损提示")
                        loss_tips = True
                    dt_ = trade_data["datetime"]
                    symbol_ = trade_data["symbol"]
                    direction_ = trade_data["direction"]
                    offset_ = trade_data["offset"]
                    signal_position_value_ = trade_data["signal_position_value"]
                    max_loss_rate_ = trade_data["max_loss_rate"]
                    print(
                        f"{dt_}\t{symbol_}\t{direction_}\t{offset_}\t{signal_position_value_}\t{max_loss_value_}\t{max_loss_rate_}"
                    )
            """

            fieldNames = [
                "datetime",
                "symbol",
                "direction",
                "offset",
                "volume",
                "price",
                "signal_position",
                "signal_position_price",
                "signal_position_value",
                "max_loss_value",
                "max_loss_rate",
            ]
            # 文件路径
            filePath = f"{signal_trade_dir_path}{signal_key}.csv"
            with open(filePath, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                # 写入csv文件
                writer.writerows(trade_list)

    if engine.portfolio.trending_open:
        # 输出趋势追踪列表
        print(f"\n****** 趋势追踪列表 ******")
        step_required = 3
        continuous_open_dict = {}
        continuous_saved_dict = {}
        for signal_key, trending_list in engine.portfolio.trending_history_dict.items():
            continuous_open = 0
            continuous_cached_list = []
            for trending_data in trending_list:
                trending = trending_data["trending"]
                trending_desc = "加仓" if trending else "平仓"
                trending_data["trending"] = trending_desc
                continuous_cached_list.append(trending_data)
                if trending:
                    continuous_open += 1

                else:
                    continuous_key = str(continuous_open)
                    count = continuous_open_dict.get(continuous_key, 0)
                    count += 1
                    continuous_open_dict[continuous_key] = count

                    # 将指定趋势强度的追踪记录添加到将要保存的列表
                    if len(continuous_cached_list) >= step_required + 1:
                        # 空数据作为分割线
                        continuous_cached_list.append(
                            {
                                "datetime": "",
                                "signal": "",
                                "position_price": "",
                                "position_value": "",
                                "max_loss_value": "",
                                "max_loss_rate": "",
                                "trending": "",
                            }
                        )
                        signal_continuous_saved_list = continuous_saved_dict.get(signal_key, [])
                        signal_continuous_saved_list = (
                            signal_continuous_saved_list + continuous_cached_list
                        )
                        continuous_saved_dict[signal_key] = signal_continuous_saved_list

                    continuous_open = 0
                    continuous_cached_list = []

        print(f"\n****** 趋势追踪连续统计 ******")
        continuous_keys = list(continuous_open_dict.keys())
        continuous_keys = sorted(continuous_keys)
        for continuous_key in continuous_keys:
            count = continuous_open_dict[continuous_key]
            print(f"{continuous_key}\t{count}")

        # 趋势追踪列表保存到csv
        for signal, signal_continuous_saved_list in continuous_saved_dict.items():
            trending_dir_path = f"trending_continuous{DIR_SYMBOL}{signal}{DIR_SYMBOL}"
            if not os.path.exists(trending_dir_path):
                os.makedirs(trending_dir_path)

            start_dt_str = start_dt.strftime("%Y-%m-%d")
            end_dt_str = end_dt.strftime("%Y-%m-%d")
            filePath = f"{trending_dir_path}{signal}_{start_dt_str}_{end_dt_str}.csv"
            fieldNames = [
                "datetime",
                "signal",
                "position_price",
                "position_value",
                "max_loss_value",
                "max_loss_rate",
                "trending",
            ]
            with open(filePath, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                # 写入csv文件
                writer.writerows(signal_continuous_saved_list)


def two():
    filename = "setting.csv"
    count = 0
    resultList = []
    with open(filename, errors="ignore") as f:
        r = DictReader(f)
        for d in r:
            engine = BacktestingEngine()
            engine.setPeriod(datetime(2010, 9, 15), datetime(2021, 1, 1))
            engine.tradingStart = datetime(2011, 1, 1)

            engine.initSinglePortfolio(d, 200000)

            engine.loadData()
            engine.runBacktesting()
            if not len(engine.resultList):
                continue

            timeseries, result = engine.calculateResult()
            print("Sharpe Ratio：\t%s" % result["sharpeRatio"])
            count += 1
            print("count：\t%s\n" % count)

            temp = d.copy()
            temp.pop("is_crypto")
            temp.pop("min_volume")
            temp["sharpeRatio"] = result["sharpeRatio"]
            temp["totalReturn"] = result["totalReturn"]
            temp["annualizedReturn"] = result["annualizedReturn"]
            temp["maxDrawdown"] = result["maxDrawdown"]
            temp["maxDdPercent"] = result["maxDdPercent"]
            resultList.append(temp)

    if len(resultList):
        fieldNames = [
            "symbol",
            "size",
            "priceTick",
            "variableCommission",
            "fixedCommission",
            "slippage",
            "name",
            "sharpeRatio",
            "totalReturn",
            "annualizedReturn",
            "maxDrawdown",
            "maxDdPercent",
        ]
        # 文件路径
        filePath = "result.csv"
        with open(filePath, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)


def three():
    resultDic = OrderedDict()
    dirPath = "resultList"
    for root, subdirs, files in os.walk(dirPath):
        for theFile in files:
            filePath = f"{root}{DIR_SYMBOL}{theFile}"
            with open(filePath) as f:
                r = DictReader(f)
                for d in r:
                    symbol = d["symbol"]
                    if not symbol in resultDic:
                        resultDic[symbol] = d
                    else:
                        hisResult = resultDic[symbol]
                        hisResult["result"] = str(
                            float(hisResult["result"]) + float(d["result"])
                        )

    resultList = resultDic.values()
    if len(resultList):
        fieldNames = [
            "symbol",
            "size",
            "priceTick",
            "variableCommission",
            "fixedCommission",
            "slippage",
            "name",
            "result",
        ]
        # 文件路径
        filePath = f"resultList{DIR_SYMBOL}result_all.csv"
        with open(filePath, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)


def four():
    filename = "setting.csv"
    symbolList = []
    with open(filename, errors="ignore") as f:
        r = DictReader(f)
        for d in r:
            symbolList.append(d)
    if not symbolList:
        return

    combineList = combine(symbolList, 6)
    count = 0
    resultList = []
    for l in combineList:
        engine = BacktestingEngine()
        engine.setPeriod(datetime(2010, 9, 15), datetime(2021, 1, 1))
        engine.tradingStart = datetime(2011, 1, 1)
        engine.initListPortfolio(l, 200000)

        engine.loadData()
        engine.runBacktesting()
        if not len(engine.resultList):
            continue

        timeseries, result = engine.calculateResult()
        dic = {
            "symbolList": engine.symbolList,
            "sharpe": result["sharpeRatio"],
            "totalPnl": result["totalReturn"],
            "annualizedPnl": result["annualizedReturn"],
        }
        resultList.append(dic)

        count += 1
        print("count：\t%s\n" % count)

    if len(resultList):
        fieldNames = ["symbolList", "sharpe", "totalPnl", "annualizedPnl"]
        # 文件路径
        filePath = "result.csv"
        with open(filePath, "w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

    print("=" * 20)
    print("组合数：%s" % count)


# 年度成交量排名
def volumeSorted():
    startDt = datetime(2010, 1, 1)
    endDt = datetime(2010, 12, 31)
    underlyingList = [
        "RB",
        "CU",
        "NI",
        "ZN",
        "RU",
        "AL",
        "HC",
        "J",
        "I",
        "PP",
        "AP",
        "TA",
        "A",
        "AG",
        "AU",
        "B",
        "BB",
        "BU",
        "C",
        "CF",
        "CS",
        "CY",
        "EG",
        "FB",
        "FG",
        "FU",
        "JD",
        "JM",
        "JR",
        "L",
        "LR",
        "M",
        "MA",
        "OI",
        "P",
        "PB",
        "PM",
        "RI",
        "RM",
        "RS",
        "SC",
        "SF",
        "SM",
        "SN",
        "SP",
        "SR",
        "V",
        "WH",
        "WR",
        "Y",
        "ZC",
        "IF",
        "IC",
        "IH",
    ]

    volumeDic = {}
    # 数据库
    mc = MongoClient()
    db = mc[DAILY_DB_NAME]
    for underlyingSymbol in underlyingList:
        totalVolume = 0
        symbol = underlyingSymbol + "99"
        cl = db[symbol]
        cl.ensure_index([("datetime", ASCENDING)], unique=True)
        flt = {"datetime": {"$gte": startDt, "$lte": endDt}}

        cursor = cl.find(flt).sort("datetime")
        for d in cursor:
            totalVolume += d["volume"]
        volumeDic[underlyingSymbol] = totalVolume
    resultDic = {"volume": volumeDic}
    df = pd.DataFrame(resultDic).sort_values("volume", ascending=False)
    print(df.head(10))


# 随机组合，l是数组，n是组合的元素数量
def combine(l, n):
    answers = []
    one = [0] * n

    def next_c(li=0, ni=0):
        if ni == n:
            answers.append(copy.copy(one))
            return
        for lj in range(li, len(l)):
            one[ni] = l[lj]
            next_c(lj + 1, ni + 1)

    next_c()
    return answers


if __name__ == "__main__":
    one()
