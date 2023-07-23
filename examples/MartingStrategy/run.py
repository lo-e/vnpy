# encoding: UTF-8

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import copy

from sqlalchemy import true
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
import json
from vnpy.trader.utility import round_to


def backtesting():
    # 回测起始日期
    engine = BacktestingEngine()
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 7, 22)
    engine.setPeriod(start_dt, end_dt)
    figSavedName = ""
    if figSavedName:
        figSavedName = f"figSaved{DIR_SYMBOL}{figSavedName}"

    # 回测合约
    marting_type = input('选择类型（默认1）【反转：1 趋势追踪：2】')
    if not marting_type:
        marting_type = "1"

    if marting_type == "2":
        marting_type = "FORWARD"

    else:
        marting_type = "INVERSE"

    exchange = input('选择交易所（默认1）【Binance：1 OKX：2 Bybit：3】')
    if exchange == "2":
        exchange = "OKX"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_okx.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_okx.csv"

    elif exchange == "3":
        exchange = "BYBIT"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_bybit.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_bybit.csv"

    else:
        exchange = "BINANCE"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_binance.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_binance.csv"

    # 回测历史数据文件
    history_from = ""
    backtesting_history_file = ""

    # history_from = f"from_history_2022-10-01_2022-11-02"
    # backtesting_history_file = "2022-01-01_2023-07-02.json"

    history_file_path = f"{history_from}{DIR_SYMBOL}{backtesting_history_file}" if history_from else backtesting_history_file

    # 开始回测
    symbolList = []
    with open(filename, errors="ignore") as f:
        r = DictReader(f)
        for d in r:
            symbolList.append(d)
    if not symbolList:
        return

    engine.initListPortfolio(symbolList, marting_type=marting_type, exchange=exchange, portfolioValue=10000, history_file=history_file_path)
    engine.loadData()
    engine.runBacktesting(daily_mode=False)
    engine.showResult(figSavedName)

    # 输出并保存交易数据
    close_trade_count = 0
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

            # 统计平仓次数
            if trade.offset != Offset.OPEN:
                close_trade_count += 1

            # 提取成交信息
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
    print(f"总平仓次数：{close_trade_count}")

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
        step_required = 1
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
                    for i in range(3):
                        step_required = i + 1

                        # 将指定趋势强度的追踪记录添加到将要保存的列表
                        if len(continuous_cached_list) >= step_required + 2:
                            required_continuous_saved_dict = continuous_saved_dict.get(step_required, {})
                            signal_continuous_saved_list = required_continuous_saved_dict.get(
                                signal_key, []
                            )
                            signal_continuous_saved_list = (
                                signal_continuous_saved_list + continuous_cached_list
                            )
                            required_continuous_saved_dict[signal_key] = signal_continuous_saved_list
                            continuous_saved_dict[step_required] = required_continuous_saved_dict

                    continuous_open = 0
                    continuous_cached_list = []

        print(f"\n****** 趋势追踪连续统计 ******")
        continuous_keys = list(continuous_open_dict.keys())
        continuous_keys_int = [int(item) for item in continuous_keys]
        continuous_keys_int = sorted(continuous_keys_int)
        for continuous_key_int in continuous_keys_int:
            count = continuous_open_dict[f"{continuous_key_int}"]
            print(f"{continuous_key_int}\t{count}")


        if marting_type == "FORWARD":
            print(f"\n****** 趋势追踪二次开仓统计 ******")
            symbol_second_dict = {}
            for symbol, signal_list in engine.portfolio.signalDict.items():
                second_open_count = 0
                for signal in signal_list:
                    second_open_count += signal.second_open_count
                symbol_second_dict[symbol] = second_open_count
            # 排序
            sorted_list = sorted(symbol_second_dict.items(), key=lambda x: x[1], reverse=True)
            for symbol, second_open_count in sorted_list:
                if second_open_count:
                    print(f"{symbol}\t{second_open_count}")

        # 趋势追踪列表保存到csv
        for i in range(3):
            step_required = i + 1
            start_dt_str = start_dt.strftime("%Y-%m-%d")
            end_dt_str = end_dt.strftime("%Y-%m-%d")
            if backtesting_history_file:
                history_dt = backtesting_history_file.split(".")[0]
                trending_dir_path = (
                    f"trending_continuous{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}{DIR_SYMBOL}min_continuous_{step_required}{DIR_SYMBOL}from_history_{history_dt}{DIR_SYMBOL}{start_dt_str}_{end_dt_str}{DIR_SYMBOL}"
                )

            else:
                trending_dir_path = (
                    f"trending_continuous{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}{DIR_SYMBOL}min_continuous_{step_required}{DIR_SYMBOL}{start_dt_str}_{end_dt_str}{DIR_SYMBOL}"
                )

            required_continuous_saved_dict = continuous_saved_dict.get(step_required, {})
            for signal, signal_continuous_saved_list in required_continuous_saved_dict.items():
                if not os.path.exists(trending_dir_path):
                    os.makedirs(trending_dir_path)

                filePath = f"{trending_dir_path}{signal}.csv"
                fieldNames = [
                    "datetime",
                    "signal",
                    "position_price",
                    "position_value",
                    "close_pnl",
                    "max_loss_value",
                    "max_loss_rate",
                    "trending",
                ]
                with open(filePath, "w") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldNames)
                    writer.writeheader()
                    # 写入csv文件
                    writer.writerows(signal_continuous_saved_list)

        # 策略状态、回测截止时间保存到json
        if backtesting_history_file:
            history_dt = backtesting_history_file.split(".")[0]
            backtesting_history_dir = f"backtesting_history{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}{DIR_SYMBOL}from_history_{history_dt}{DIR_SYMBOL}"
        
        else:
            backtesting_history_dir = f"backtesting_history{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}{DIR_SYMBOL}"

        if not os.path.exists(backtesting_history_dir):
            os.makedirs(backtesting_history_dir)
        backtesting_history_json = f"{backtesting_history_dir}{start_dt_str}_{end_dt_str}.json"

        # 先从文件导入已经保存的回测数据
        backtesting_data = {}
        if os.path.exists(backtesting_history_json):
            with open(backtesting_history_json, mode="r", encoding="UTF-8") as f:
                backtesting_data = json.load(f)
        
        # 本次回测结果更新
        trade_setting = {} # 实盘设置参数
        signal_trade_setting = {} # 策略的实盘设置参数
        trade_setting_file = f"trade_setting{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}.json"
        if os.path.exists(trade_setting_file):
            with open(trade_setting_file, mode="r", encoding="UTF-8") as f:
                trade_setting = json.load(f)
        for setting in trade_setting.get("signal", []):
            name = setting["strategy_name"]
            signal_trade_setting[name] = setting

        signal_trending_step_dict = {} # 回测结果中信号的趋势追踪信息
        for _, signal_list in engine.portfolio.signalDict.items():
            for signal in signal_list:
                # 信号的状态
                symbol = signal.symbol
                pure_symbol = symbol[:symbol.index('USDT')] 
                direction = signal.direction
                signal_key = f"MARTING_{exchange}_{pure_symbol}_{direction.value}"
                backtesting_data[signal_key] = signal.inverse_signal.saved_sync_data

                # 信号组合的最新趋势追踪信息
                signal_status = signal.inverse_signal.saved_sync_data.get("backtesting_status", {})
                if signal_status:
                    trade_setting = signal_trade_setting.get(signal_key, {})
                    signal_bottom = trade_setting.get("bottom_step", 0)
                    trending_step = signal_status["trending_step"]
                    data_list = signal_trending_step_dict.get(trending_step, [])
                    
                    direction_v = 1 if signal.direction == Direction.LONG else -1
                    position_pnl = (
                        (signal.inverse_signal.bar.close_price / signal.inverse_signal.position_price) - 1
                    ) * 100 * direction_v
                    position_pnl = round_to(position_pnl, 0.01)
                    position_pnl = f"{position_pnl}%"

                    data_list.append([signal_key, signal_bottom, position_pnl, signal_status])
                    signal_trending_step_dict[trending_step] = data_list

        # 保存回测结果的信号状态到json文件
        with open(backtesting_history_json, "w", encoding="utf-8") as file:
            file.write(json.dumps(backtesting_data, ensure_ascii=False))
        print(f"\n已保存回测历史到{start_dt_str}_{end_dt_str}.json\t总数：{len(backtesting_data)}")
        
        # 输出回测结果的趋势追踪信号信息
        trending_step_list = sorted(list(signal_trending_step_dict.keys()))
        for trending_step in trending_step_list:
            if trending_step:
                print(f"\n趋势追踪{trending_step}")
                data_list = signal_trending_step_dict[trending_step]
                for signal_data in data_list:
                    signal_key, signal_bottom, position_pnl, signal_status = signal_data
                    p = signal_status["position_price"]
                    r = signal_status["position_reduce_price"]
                    i = signal_status["position_increase_price"]
                    print(f"{signal_key}_bottom_{signal_bottom}\t\tp：{p}\tr：{r}\ti：{i}\tpnl：{position_pnl}")
        print("\n")

def combine_backtesting():
    # 选择回测策略类型
    marting_type = input('选择类型（默认1）【反转：1 趋势追踪：2】')
    if marting_type == "2":
        marting_type = "FORWARD"

    else:
        marting_type = "INVERSE"

    # 选择合约交易所
    exchange = input('选择交易所（默认1）【Binance：1 OKX：2 Bybit：3】')
    if exchange == "2":
        exchange = "OKX"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_okx.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_okx.csv"

    elif exchange == "3":
        exchange = "BYBIT"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_bybit.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_bybit.csv"

    else:
        exchange = "BINANCE"
        if marting_type == "FORWARD":
            filename = f"setting_forward{DIR_SYMBOL}setting_binance.csv"

        else:
            filename = f"setting_inverse{DIR_SYMBOL}setting_binance.csv"
    
    # 筛选的合约列表
    target_symbol_list = []
    if marting_type == "FORWARD":
        if exchange == "BINANCE":
            # 交集：25
            # target_symbol_list = ['ANKRUSDT.BINANCE', 'AXSUSDT.BINANCE', 'BELUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DASHUSDT.BINANCE', 'DOGEUSDT.BINANCE', 'EGLDUSDT.BINANCE', 'ENJUSDT.BINANCE', 'ETCUSDT.BINANCE', 'ETHUSDT.BINANCE', 'FTMUSDT.BINANCE', 'GALAUSDT.BINANCE', 'MKRUSDT.BINANCE', 'OGNUSDT.BINANCE', 'OMGUSDT.BINANCE', 'RLCUSDT.BINANCE', 'SFPUSDT.BINANCE', 'SKLUSDT.BINANCE', 'STORJUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'SXPUSDT.BINANCE', 'WAVESUSDT.BINANCE', 'YFIUSDT.BINANCE', 'ZILUSDT.BINANCE', 'ZRXUSDT.BINANCE']
            # 并集：82
            target_symbol_list = ['1000SHIBUSDT.BINANCE', '1000XECUSDT.BINANCE', 'AAVEUSDT.BINANCE', 'ADAUSDT.BINANCE', 'ALGOUSDT.BINANCE', 'ALPHAUSDT.BINANCE', 'ANKRUSDT.BINANCE', 'ARPAUSDT.BINANCE', 'ARUSDT.BINANCE', 'ATAUSDT.BINANCE', 'ATOMUSDT.BINANCE', 'AUDIOUSDT.BINANCE', 'AVAXUSDT.BINANCE', 'AXSUSDT.BINANCE', 'BAKEUSDT.BINANCE', 'BATUSDT.BINANCE', 'BCHUSDT.BINANCE', 'BELUSDT.BINANCE', 'BLZUSDT.BINANCE', 'BNBUSDT.BINANCE', 'C98USDT.BINANCE', 'CELRUSDT.BINANCE', 'CHRUSDT.BINANCE', 'COTIUSDT.BINANCE', 'CRVUSDT.BINANCE', 'CTKUSDT.BINANCE', 'CTSIUSDT.BINANCE', 'DASHUSDT.BINANCE', 'DENTUSDT.BINANCE', 'DGBUSDT.BINANCE', 'DOGEUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'EGLDUSDT.BINANCE', 'ENJUSDT.BINANCE', 'EOSUSDT.BINANCE', 'ETCUSDT.BINANCE', 'ETHUSDT.BINANCE', 'FILUSDT.BINANCE', 'FLMUSDT.BINANCE', 'FTMUSDT.BINANCE', 'GALAUSDT.BINANCE', 'GTCUSDT.BINANCE', 'IOSTUSDT.BINANCE', 'IOTAUSDT.BINANCE', 'KAVAUSDT.BINANCE', 'KNCUSDT.BINANCE', 'LINAUSDT.BINANCE', 'LITUSDT.BINANCE', 'LRCUSDT.BINANCE', 'MANAUSDT.BINANCE', 'MASKUSDT.BINANCE', 'MATICUSDT.BINANCE', 'MKRUSDT.BINANCE', 'NEARUSDT.BINANCE', 'NEOUSDT.BINANCE', 'OGNUSDT.BINANCE', 'OMGUSDT.BINANCE', 'ONEUSDT.BINANCE', 'PEOPLEUSDT.BINANCE', 'RENUSDT.BINANCE', 'RLCUSDT.BINANCE', 'RSRUSDT.BINANCE', 'RUNEUSDT.BINANCE', 'SFPUSDT.BINANCE', 'SKLUSDT.BINANCE', 'SOLUSDT.BINANCE', 'STORJUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'SXPUSDT.BINANCE', 'TRXUSDT.BINANCE', 'UNFIUSDT.BINANCE', 'UNIUSDT.BINANCE', 'WAVESUSDT.BINANCE', 'XEMUSDT.BINANCE', 'XLMUSDT.BINANCE', 'XRPUSDT.BINANCE', 'XTZUSDT.BINANCE', 'YFIUSDT.BINANCE', 'ZECUSDT.BINANCE', 'ZENUSDT.BINANCE', 'ZILUSDT.BINANCE', 'ZRXUSDT.BINANCE']

    else:
        if exchange == "BINANCE":
            # （TRENDING_INCREASE_RATE 0.04）最大连续趋势追踪6：8
            target_symbol_list = ['ALGOUSDT.BINANCE', 'ATOMUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'TRXUSDT.BINANCE']
            # target_symbol_list = ['ALGOUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE']
            # target_symbol_list = ['ALGOUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE']
            # target_symbol_list = ['ALGOUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE']
            target_symbol_list = ['BTCUSDT.BINANCE', 'ETHUSDT.BINANCE', 'LINKUSDT.BINANCE', 'XRPUSDT.BINANCE', 'XLMUSDT.BINANCE', 'SOLUSDT.BINANCE', 'DOGEUSDT.BINANCE', 'MKRUSDT.BINANCE', 'BCHUSDT.BINANCE', 'LTCUSDT.BINANCE', 'COMPUSDT.BINANCE', 'MATICUSDT.BINANCE', 'OPUSDT.BINANCE', 'SNXUSDT.BINANCE', 'ARBUSDT.BINANCE', 'BANDUSDT.BINANCE', 'FILUSDT.BINANCE', 'BNBUSDT.BINANCE', 'DOTUSDT.BINANCE', 'APEUSDT.BINANCE']

        elif exchange == "OKX":
            target_symbol_list = ['ALGO-USDT-SWAP.OKX', 'DYDX-USDT-SWAP.OKX', 'ENS-USDT-SWAP.OKX', 'EOS-USDT-SWAP.OKX', 'SUSHI-USDT-SWAP.OKX']

    # 获取合约列表
    symbolList = []
    with open(filename, errors="ignore") as f:
        r = DictReader(f)
        for d in r:
            symbolList.append(d)
    
    if target_symbol_list:
        temp = []
        for symbol_data in symbolList:
            symbol = symbol_data["symbol"]
            if symbol in target_symbol_list:
                temp.append(symbol_data)

        symbolList = temp

    if not symbolList:
        return

    # 随机组合合约列表
    combineList = combine(symbolList, 3)
    print(f"\n随机组合总数：{len(combineList)}\n")
    
    count = 0
    resultList = []
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 7, 22)
    for l in combineList:
        # 开始回测
        engine = BacktestingEngine()
        engine.setPeriod(start_dt, end_dt)
        engine.initListPortfolio(l, marting_type=marting_type, exchange=exchange, portfolioValue=10000)
        engine.loadData()
        engine.runBacktesting()
        if not len(engine.resultList):
            continue
        
        # 计算回测结果
        timeseries, result = engine.calculateResult()

        # 统计回撤数据
        drawdown_series = timeseries["drawdownSeries"]
        period_drawdown_dict = {}
        last_drawdown = 0
        last_drawdown_dt = ""
        for dt, drawdown in drawdown_series.items():
            dt = str(dt)
            if drawdown >= 0:
                if last_drawdown < 0:
                    period_drawdown_dict[last_drawdown_dt] = round_to(last_drawdown, 0.01)
                last_drawdown = drawdown
                last_drawdown_dt = dt.split(" ")[0]
                
            elif drawdown < last_drawdown:
                last_drawdown = drawdown
                last_drawdown_dt = dt.split(" ")[0]

        if last_drawdown < 0:
            period_drawdown_dict[last_drawdown_dt] = round_to(last_drawdown, 0.01)

        # 超出本金的回撤（爆仓）统计
        over_drawdown_dict = {}
        for dt, period_drawdown in period_drawdown_dict.items():
            if period_drawdown <= engine.portfolio.portfolioValue * -1:
                over_drawdown_dict[dt] = period_drawdown

        # 保存组合回测结果所需的内容
        totalPnl = round_to(result["totalReturn"], 0.01)
        dic = {
            "symbolList": engine.symbolList,
            "totalPnl": f"{totalPnl}%",
            "max_drawdown": round_to(result["maxDrawdown"], 0.01),
            "over_drawdown": over_drawdown_dict,
            "over_drawdown_count": len(over_drawdown_dict),
        }
        resultList.append(dic)
        count += 1
        print("count：\t%s\n" % count)

        # 组合回测结果保存到文件
        if len(resultList):
            start_dt_str = start_dt.strftime("%Y-%m-%d")
            end_dt_str = end_dt.strftime("%Y-%m-%d")
            fieldNames = ["symbolList", "totalPnl", "max_drawdown", "over_drawdown", "over_drawdown_count"]
            # 文件路径
            file_dir = f"combine_backtesting_result{DIR_SYMBOL}{marting_type}{DIR_SYMBOL}{exchange}{DIR_SYMBOL}"
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            file_path = f"{file_dir}{start_dt_str}_{end_dt_str}.csv"
            with open(file_path, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldNames)
                writer.writeheader()
                # 写入csv文件
                writer.writerows(resultList)

    print("=" * 20)
    print("组合数：%s" % count)

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
    # 合约列表回测
    # backtesting()

    # 随机组合合约列表回测
    combine_backtesting()
