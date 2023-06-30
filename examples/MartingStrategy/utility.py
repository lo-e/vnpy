import os
from vnpy.trader.utility import DIR_SYMBOL
import csv
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from copy import copy
import json
import io
import math
# 将上一级目录添加到模块搜索路径中
import sys

sys.path.append("..")
from vn_trader.App.Turtle_crypto.dataservice.BybitDataService import (
    bybit_get_symbol_list,
    BybitSymbolType,
)

from vn_trader.App.Turtle_crypto.dataservice.BinanceDataService import binance_get_symbol_list

# 计算每个加仓阶段的亏损状态
def calculate_phase_loss(phase_count: int, increase_type: int = 1):
    phase_value_list = [
        500.0,
        1500.0,
        3500.0,
        7500.0,
        15500.0,
        31500.0,
        63500.0,
        127500.0,
        255500.0,
        511500.0,
        1023500.0,
        2047500.0,
    ]
    if increase_type == 0:
        increase_rate_list = [0.01 * 2 * i for i in range(len(phase_value_list))]

    elif increase_type == 1:
        increase_rate_list = [0.01 * 2**i for i in range(len(phase_value_list))]
    else:
        exit("检查代码！")
    init_price = 100

    value_before = phase_value_list[0]
    price_before = init_price
    phase_list = range(phase_count)[1:]
    print(
        f"\n第{1}阶段\n持仓价值：{value_before}\n持仓数量：{value_before/price_before}\n持仓均价：{price_before}"
    )
    for i in phase_list:
        value_after = phase_value_list[i]
        increase_rate = increase_rate_list[i]
        increase_rate = min(increase_rate, 0.32)
        trade_price = price_before * (1 + increase_rate)

        trade_volume = (value_after - value_before) / trade_price
        price_after = value_after / ((value_before / price_before) + trade_volume)

        loss_before = ((trade_price / price_before) - 1) * 100
        loss_before = round(loss_before, 2)
        loss_before = f"{loss_before}%"

        loss_after = ((trade_price / price_after) - 1) * 100
        loss_after = round(loss_after, 2)
        loss_after = f"{loss_after}%"

        print(
            f"\n第{i+1}阶段\n持仓价值：{value_after}\n持仓数量：{value_after/price_after}\n持仓均价：{price_after}\n成交价格：{trade_price}\n加仓前亏损：{loss_before}\n加仓后亏损：{loss_after}"
        )

        value_before = value_after
        price_before = price_after

    symbol_price_changed = ((trade_price / init_price) - 1) * 100
    symbol_price_changed = round(symbol_price_changed, 2)
    symbol_price_changed = f"{symbol_price_changed}%"

    position_price_changed = ((price_before / init_price) - 1) * 100
    position_price_changed = round(position_price_changed, 2)
    position_price_changed = f"{position_price_changed}%"
    print(
        f"\n\n============\n总共经历{phase_count}个阶段加仓\n初始持仓价值：{phase_value_list[0]}\t初始持仓价格：{init_price}\n最后持仓价值：{value_before}\t最后持仓价格：{price_before}\n持仓价格变化：{position_price_changed}\n合约价格变化：{symbol_price_changed}"
    )


# 分析trending_continuous下的趋势追踪结果
def analyse_trending_continuous(
     exchange:str, target_dir: str, min_continuous: str, by_month: bool = False, for_trade_setting: bool = False
):
    # 趋势追踪程度
    continuous_open_dict = {}
    continuous_open_symbol_dict = {}
    continuous_symbol_open_dict = {}
    continuous_open_overload_dict = {}
    month_open_symbol_dict = {}
    month_symbol_open_dict = {}
    month_open_overload_dict = {}

    start_end = target_dir.split("_")
    if start_end and len(start_end) == 2:
        start = start_end[0]
        end = start_end[1]
        print(f"\n====== 起止日期：{start} - {end} ======")

    else:
        exit("参数【target_dir】错误")

    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    main_dir_path = path.rstrip(file_name) + f"trending_continuous{DIR_SYMBOL}{exchange}{DIR_SYMBOL}min_continuous_{min_continuous}{DIR_SYMBOL}{target_dir}{DIR_SYMBOL}"
    for root, _, files in os.walk(main_dir_path):
        for theFile in files:
            # 排除不合法文件
            if theFile.startswith("."):
                continue

            if ".csv" in theFile:
                # 读取文件
                filePath = root + DIR_SYMBOL + theFile
                with open(filePath, "r") as f:
                    continuous_open = 0

                    # 开始导入数据
                    reader = csv.DictReader(f)
                    open_close_rows = []
                    for row in reader:
                        dt = row["datetime"]
                        signal = row["signal"]
                        symbol = signal[0 : len(signal) - 2]
                        # 分割行
                        if not dt:
                            continue

                        trending = row["trending"]
                        if trending == f"加仓":
                            continuous_open += 1
                            open_close_rows.append(row)
                        else:
                            open_close_rows.append(row)

                            # 记录趋势追踪程度
                            continuous_key = str(continuous_open)
                            count = continuous_open_dict.get(continuous_key, 0)
                            count += 1
                            continuous_open_dict[continuous_key] = count

                            # 趋势追踪的信号统计1
                            signal_dict = continuous_open_symbol_dict.get(
                                continuous_key, {}
                            )
                            signal_list = signal_dict.get(symbol, [])
                            signal_list += open_close_rows
                            signal_dict[symbol] = signal_list
                            continuous_open_symbol_dict[continuous_key] = signal_dict

                            # 趋势追踪的信号统计2
                            open_dict = continuous_symbol_open_dict.get(symbol, {})
                            open_list = open_dict.get(continuous_key, [])
                            open_list += open_close_rows
                            open_dict[continuous_key] = open_list
                            continuous_symbol_open_dict[symbol] = open_dict

                            # 趋势追踪的信号统计3
                            for i in range(len(open_close_rows)):
                                row = open_close_rows[i]
                                trending = row["trending"]
                                if trending == "加仓" and i <= 2:
                                    max_loss_rate = row["max_loss_rate"].replace("%", "")
                                    max_loss_rate = float(max_loss_rate)

                                    if (max_loss_rate <= -15.0 and int(continuous_key) >= 3):
                                        overload_dict = continuous_open_overload_dict.get(
                                            continuous_key, {}
                                        )
                                        overload_list = overload_dict.get(symbol, [])
                                        overload_list += open_close_rows
                                        overload_dict[symbol] = overload_list
                                        continuous_open_overload_dict[continuous_key] = overload_dict
                                        break

                            # 月份趋势追踪的信号统计1
                            month_str = datetime.strptime(
                                dt, "%Y-%m-%d %H:%M:%S"
                            ).strftime("%Y-%m")
                            month_dict = month_open_symbol_dict.get(month_str, {})
                            signal_dict = month_dict.get(continuous_key, {})
                            signal_list = signal_dict.get(symbol, [])
                            signal_list += open_close_rows
                            signal_dict[symbol] = signal_list
                            month_dict[continuous_key] = signal_dict
                            month_open_symbol_dict[month_str] = month_dict

                            # 月份趋势追踪的信号统计2
                            month_dict = month_symbol_open_dict.get(month_str, {})
                            open_dict = month_dict.get(symbol, {})
                            open_list = open_dict.get(continuous_key, [])
                            open_list += open_close_rows
                            open_dict[continuous_key] = open_list
                            month_dict[symbol] = open_dict
                            month_symbol_open_dict[month_str] = month_dict

                            # 月份趋势追踪的信号统计3
                            for i in range(len(open_close_rows)):
                                row = open_close_rows[i]
                                trending = row["trending"]
                                if trending == "加仓" and i <= 2:
                                    max_loss_rate = row["max_loss_rate"].replace("%", "")
                                    max_loss_rate = float(max_loss_rate)

                                    if (max_loss_rate <= -15.0 and int(continuous_key) >= 3):
                                        month_dict = month_open_overload_dict.get(month_str, {})
                                        overload_dict = month_dict.get(continuous_key, {})
                                        overload_list = overload_dict.get(symbol, [])
                                        overload_list += open_close_rows
                                        overload_dict[symbol] = overload_list
                                        month_dict[continuous_key] = overload_dict
                                        month_open_overload_dict[month_str] = month_dict
                                        break

                            # 连续加仓重置
                            continuous_open = 0
                            open_close_rows = []

    # 连续趋势追踪程度统计
    print(f"\n****** 连续趋势追踪程度统计 ******")
    continuous_keys = list(continuous_open_dict.keys())
    continuous_keys = sorted(continuous_keys)
    for continuous_key in continuous_keys:
        count = continuous_open_dict[continuous_key]
        print(f"{continuous_key}\t{count}")

    if by_month:
        month_keys = list(month_open_symbol_dict.keys())
        month_keys = sorted(month_keys)
        for month in month_keys:
            print(f"\n======================== 按月统计：{month} ========================")

            # 连续趋势追踪信号统计
            # open_symbol_dict = month_open_symbol_dict[month]
            # output_open_symbol_result(open_symbol_dict)

            # 信号连续趋势追踪统计
            # symbol_open_dict = month_symbol_open_dict[month]
            # output_symbol_open_result(symbol_open_dict, exchange=exchange)

            # 连续趋势追踪<强势>信号统计
            open_overload_dict = month_open_overload_dict[month]
            output_open_overload_result(open_overload_dict)

    else:
        # 连续趋势追踪信号统计
        # output_open_symbol_result(continuous_open_symbol_dict)

        # 信号连续趋势追踪统计
        # output_symbol_open_result(continuous_symbol_open_dict, exchange=exchange)

        # 连续趋势追踪<强势>信号统计
        output_open_overload_result(continuous_open_overload_dict)

    # 生成实盘setting.json
    if for_trade_setting:
        all_symbol_set = set()
        setting_file_path = f"setting_{exchange.lower()}.csv"
        with open(setting_file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_symbol_set.add(row["symbol"])

        # 添加最大连续趋势追踪2的合约
        max_2_symbol_set = all_symbol_set - set(list(continuous_symbol_open_dict.keys()))
        total_dict = copy(continuous_symbol_open_dict)
        for max_2_symbol in max_2_symbol_set:
            total_dict[max_2_symbol] = {"2":[]}
        generate_setting(total_dict, exchange=exchange)

def output_open_symbol_result(open_symbol_dict: dict):
    continuous_keys = list(open_symbol_dict.keys())
    continuous_keys = sorted(continuous_keys)
    for continuous_key in continuous_keys:
        print(f"\n****** 连续趋势追踪{continuous_key}信号统计 ******")
        symbol_dict = open_symbol_dict[continuous_key]
        # 进行次数倒叙排序
        symbol_times_list = []
        for symbol, symbol_data_list in symbol_dict.items():
            symbol_times_list.append({"symbol": symbol, "times": len(symbol_data_list)})
        df = pd.DataFrame(symbol_times_list)
        df = df.sort_values("times", ascending=False)
        for _, row in df.iterrows():
            symbol = row["symbol"]
            symbol_data_list = symbol_dict[symbol]
            close_count = int(len(symbol_data_list) / (int(continuous_key) + 1))
            print(f"\n{symbol}有{close_count}次记录")
            for i in range(len(symbol_data_list)):
                symbol_data = symbol_data_list[i]
                dt = symbol_data["datetime"]
                signal = symbol_data["signal"]
                position_price = symbol_data["position_price"]
                position_value = symbol_data["position_value"]
                max_loss_value = symbol_data["max_loss_value"]
                max_loss_rate = symbol_data["max_loss_rate"]
                trending = symbol_data["trending"]
                print(
                    f"{dt}\t{signal}\t{position_price}\t{position_value}\t{max_loss_value}\t{max_loss_rate}\t{trending}"
                )
                if trending == "平仓" and i != len(symbol_data_list) - 1:
                    print("\n")

def output_symbol_open_result(symbol_open_dict: dict, exchange:str):
    all_symbol_set = set()
    setting_file_path = f"setting_{exchange.lower()}.csv"
    with open(setting_file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_symbol_set.add(row["symbol"])

    max_2_symbol_set = all_symbol_set - set(list(symbol_open_dict.keys()))
    max_3_symbol_set = set()
    max_4_symbol_set = set()
    over_4_symbol_set = set()
    for symbol, open_dict in symbol_open_dict.items():
        # print(f"\n{symbol}的连续趋势追踪记录")
        max_3 = True
        max_4 = True
        for continuous_key, data_list in open_dict.items():
            if int(continuous_key) > 3:
                max_3 = False

            if int(continuous_key) > 4:
                max_4 = False

            # print(f"追踪{continuous_key}\t{len(data_list)}")

        if max_3:
            max_3_symbol_set.add(symbol)

        elif max_4:
            max_4_symbol_set.add(symbol)
        
        else:
            over_4_symbol_set.add(symbol)

    # print(f"\n最大连续趋势追踪2的合约总数：{len(max_2_symbol_set)}")
    # for symbol in max_2_symbol_set:
    #     print(symbol)

    print(f"\n最大连续趋势追踪3的合约总数：{len(max_3_symbol_set)}")
    for symbol in max_3_symbol_set:
        print(symbol)

    print(f"\n最大连续趋势追踪4的合约总数：{len(max_4_symbol_set)}")
    for symbol in max_4_symbol_set:
        print(symbol)

    print(f"\n连续趋势追踪5以上的合约总数：{len(over_4_symbol_set)}")
    for symbol in over_4_symbol_set:
        print(symbol)
    print("\n")

def output_open_overload_result(open_overload_dict: dict):
    # 筛选合约列表
    target_symbols = []
    # target_symbols = ['ETHUSDT', 'WAVESUSDT', 'OGNUSDT', 'GALAUSDT', 'ANKRUSDT', 'SXPUSDT', 'ZILUSDT', 'CHRUSDT', 'BCHUSDT', 'ETCUSDT', 'AXSUSDT', 'ZRXUSDT', 'RLCUSDT', 'AVAXUSDT', 'DASHUSDT', 'YFIUSDT', 'STORJUSDT', 'DOGEUSDT', 'ALPHAUSDT', 'FTMUSDT', 'SKLUSDT', 'OMGUSDT', 'SUSHIUSDT', 'SFPUSDT', 'EGLDUSDT', '1000SHIBUSDT', 'MKRUSDT', 'ATOMUSDT', 'BELUSDT', 'ADAUSDT', 'ENJUSDT']
    # target_symbols = ['GALAUSDT', 'ZRXUSDT', 'BAKEUSDT', 'SFPUSDT', 'LINAUSDT', 'OMGUSDT', 'RENUSDT', 'KNCUSDT', 'BATUSDT', 'BELUSDT', 'WAVESUSDT', 'ZENUSDT', 'SXPUSDT', 'RLCUSDT', 'PEOPLEUSDT', 'CHRUSDT', 'ARUSDT', 'ARPAUSDT', 'ATAUSDT', 'UNFIUSDT', 'DYDXUSDT', 'OGNUSDT', 'DASHUSDT', 'AUDIOUSDT', 'LRCUSDT', 'SKLUSDT', 'ETHUSDT', 'AXSUSDT', 'MASKUSDT', 'AAVEUSDT', 'ZILUSDT', 'SUSHIUSDT', 'STORJUSDT', 'FTMUSDT', 'ETCUSDT', 'CTSIUSDT', 'KAVAUSDT', 'DOGEUSDT', 'EGLDUSDT', 'SOLUSDT', 'C98USDT', 'CRVUSDT', 'YFIUSDT', 'ALGOUSDT', 'RSRUSDT', 'MKRUSDT', 'ENJUSDT']
    target_symbols = ['ZILUSDT', 'ATAUSDT', 'CTSIUSDT', 'EGLDUSDT', 'DYDXUSDT', 'AUDIOUSDT', '1000SHIBUSDT', 'LINAUSDT', 'OMGUSDT', 'WAVESUSDT', 'ARUSDT', 'ALPHAUSDT', 'ZENUSDT', 'PEOPLEUSDT', 'KAVAUSDT', 'FTMUSDT', 'DOGEUSDT', 'STORJUSDT', 'UNFIUSDT', 'BATUSDT', 'SXPUSDT', 'CHRUSDT', 'ARPAUSDT', 'BAKEUSDT', 'RSRUSDT', 'AXSUSDT', 'ETCUSDT', 'SFPUSDT', 'BCHUSDT', 'ETHUSDT', 'YFIUSDT', 'LRCUSDT', 'RENUSDT', 'AVAXUSDT', 'ATOMUSDT', 'GALAUSDT', 'KNCUSDT', 'AAVEUSDT', 'SUSHIUSDT', 'CRVUSDT', 'OGNUSDT', 'ADAUSDT', 'DASHUSDT', 'SOLUSDT', 'SKLUSDT', 'MASKUSDT', 'ALGOUSDT', 'BELUSDT', 'C98USDT', 'ANKRUSDT', 'ZRXUSDT', 'RLCUSDT', 'ENJUSDT', 'MKRUSDT']

    # 优选合约列表
    continuous_4_symbols = set()
    continuous_5_symbols = set()
    over_5_symbols = set()

    continuous_keys = list(open_overload_dict.keys())
    continuous_keys = sorted(continuous_keys)
    for continuous_key in continuous_keys:
        print(f"\n****** 连续趋势追踪{continuous_key}<强势>信号统计 ******")
        continuous_total = 0
        symbol_dict = open_overload_dict[continuous_key]
        # 进行次数倒叙排序
        symbol_times_list = []
        for symbol, symbol_data_list in symbol_dict.items():
            symbol_times_list.append({"symbol": symbol, "times": len(symbol_data_list)})
        df = pd.DataFrame(symbol_times_list)
        df = df.sort_values("times", ascending=False)
        for _, row in df.iterrows():
            symbol = row["symbol"]
            pure_symbol = symbol.split(".")[0]
            # 如果有筛选合约，做出筛选
            if target_symbols and pure_symbol not in target_symbols:
                continue
            
            # 统计优选合约
            if int(continuous_key) == 4:
                continuous_4_symbols.add(pure_symbol)
            if int(continuous_key) == 5:
                continuous_5_symbols.add(pure_symbol)
            if int(continuous_key) > 5:
                over_5_symbols.add(pure_symbol)

            symbol_data_list = symbol_dict[symbol]
            close_count = int(len(symbol_data_list) / (int(continuous_key) + 1))
            print(f"\n{symbol}有{close_count}次记录")
            continuous_total += close_count
            for i in range(len(symbol_data_list)):
                symbol_data = symbol_data_list[i]
                dt = symbol_data["datetime"]
                signal = symbol_data["signal"]
                position_price = symbol_data["position_price"]
                position_value = symbol_data["position_value"]
                max_loss_value = symbol_data["max_loss_value"]
                max_loss_rate = symbol_data["max_loss_rate"]
                trending = symbol_data["trending"]
                print(
                    f"{dt}\t{signal}\t{position_price}\t{position_value}\t{max_loss_value}\t{max_loss_rate}\t{trending}"
                )
                if trending == "平仓" and i != len(symbol_data_list) - 1:
                    print("\n")
        print(f"总计：{continuous_total}")
    
    if not target_symbols:
        total_symbols = list(set(continuous_4_symbols) | set(continuous_5_symbols) | set(over_5_symbols))
        total_symbols_little = list(set(continuous_5_symbols) | set(over_5_symbols))
        print(f"\n强势追踪4的合约数量{len(continuous_4_symbols)}：\n{continuous_4_symbols}")
        print(f"\n强势追踪5的合约数量{len(continuous_5_symbols)}：\n{continuous_5_symbols}")
        print(f"\n强势追踪5以上的合约数量{len(over_5_symbols)}：\n{over_5_symbols}")
        print(f"\n统计【4 5 6】：{len(total_symbols)}：\n{total_symbols}\n")
        print(f"\n统计【5 6】：{len(total_symbols_little)}：\n{total_symbols_little}\n")

def generate_setting(symbol_open_dict: dict, exchange:str):
    # 获取合约最小交易价值
    symbol_min_value_dict = {}
    if exchange == "BYBIT":
        usdt_symbol_list, data = bybit_get_symbol_list(
            type=BybitSymbolType.USDT, need_data=True
        )
    
    elif exchange == "BINANCE":
        usdt_symbol_list, data = binance_get_symbol_list(need_data=True)
    
    else:
        exit("未知交易所，检查参数是否正确")

    for symbol in usdt_symbol_list:
        d = data[symbol]

        # 交易所合约
        full_symbol = f"{symbol}.{exchange}"

        # 最小交易数量
        if exchange == "BYBIT":
            min_volume = float(d["lot_size_filter"]["min_trading_qty"])
        
        elif exchange == "BINANCE":
            min_volume = float(d["filters"][2]["minQty"])

        # 数据库获取起始日期
        client = MongoClient("localhost", 27017)
        db = client[MINUTE_DB_NAME]
        collection = db[full_symbol]
        end_data = collection.find_one(sort=[("datetime", DESCENDING)])
        if end_data:
            # 获取最新的价格
            price = end_data["close_price"] # 数据库获取最新价格

            # 最小交易价值
            value = min_volume * price
            if exchange == "BINANCE":
                value = max(value, 5)

            # 存入字典
            symbol_min_value_dict[full_symbol] = value

    # 获取合约最大连续追踪等级
    symbol_max_open_dict = {}
    for symbol, data in symbol_open_dict.items():
        continuous_open_list = list(data.keys())
        max_open = max(continuous_open_list)
        symbol_max_open_dict[symbol] = max_open

    # 生成setting参数
    portfolioValue = 100
    setting_dict = {
        "signal": [],
        "portfolio": {"name": f"MARTING_{exchange}", "portfolioValue": portfolioValue},
    }
    max_leverage = 10
    strategy_min_value = 1
    strategy_max_value = portfolioValue * max_leverage
    strategy_trending_value_list = []
    v = strategy_min_value
    while v <= strategy_max_value:
        strategy_trending_value_list.append(v)
        v *= 10

    symbol_setting_list = []
    result_symbol_count = 0
    sorted_symbol_list = sorted(list(symbol_max_open_dict.keys()))
    for symbol in sorted_symbol_list:
        max_open = symbol_max_open_dict[symbol]
        # 趋势追踪最高等级
        top_step = max(int(max_open), 4)
        # 根据最小交易量决定的趋势追踪最大次数
        step_length = 0
        # 初始趋势追踪的持仓价值
        init_value = 0
        # 合约最小交易量
        min_value = symbol_min_value_dict.get(symbol, 0)
        if min_value:
            for i in range(len(strategy_trending_value_list)):
                v = strategy_trending_value_list[i]
                if min_value * 1.5 <= v:
                    init_value = v
                    step_length = len(strategy_trending_value_list) - i
                    if step_length > top_step:
                        step_length = top_step
                        init_value = strategy_trending_value_list[len(strategy_trending_value_list) - step_length]
                    break
                
        if step_length:
            init_value_rate = init_value / portfolioValue
            bottom_step = top_step - (step_length - 1)
            #"""
            # 固定参数使用
            top_step = 9
            init_value = max(10, init_value)
            bottom_step = int(math.log10(init_value)) + 2
            init_value_rate = init_value / portfolioValue
            #"""
            pure_symbol = symbol[:symbol.index('USDT')] 
            data_long = {
                "strategy_name": f"MARTING_{exchange}_{pure_symbol}_多",
                "class_name": "MartingStrategy",
                "vt_symbol": symbol,
                "direction": "多",
                "init_value_rate": init_value_rate,
                "bottom_step": bottom_step,
                "top_step": top_step,
                "start": True
                }
            symbol_setting_list.append(data_long)

            data_short = {
                "strategy_name": f"MARTING_{exchange}_{pure_symbol}_空",
                "class_name": "MartingStrategy",
                "vt_symbol": symbol,
                "direction": "空",
                "init_value_rate": init_value_rate,
                "bottom_step": bottom_step,
                "top_step": top_step,
                "start": True
                }
            symbol_setting_list.append(data_short)
            result_symbol_count += 1
    
    # 完成setting参数
    setting_dict["signal"] = symbol_setting_list

    # 保存到json文件
    file_dir = f"trade_setting{DIR_SYMBOL}"
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

    json_file = f"{file_dir}{exchange}.json"
    with io.open(json_file, "w", encoding='utf-8') as file:
        file.write(json.dumps(setting_dict, ensure_ascii=False))
    print(f"\n生成的实盘参数已保存到{json_file}\n总计合约数：{len(symbol_open_dict)}\n成功生成实盘参数合约数：{result_symbol_count}")

if __name__ == "__main__":
    # 计算每个加仓阶段的亏损状态
    # calculate_phase_loss(phase_count=6, increase_type=1)

    # 分析trending_continuous下的趋势追踪结果，并生成实盘参数
    analyse_trending_continuous(
        exchange="BINANCE", min_continuous="1", target_dir="2022-01-01_2023-12-28", by_month=False, for_trade_setting=False
    )
