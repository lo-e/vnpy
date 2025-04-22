# -- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL
import socket
from enum import Enum
from vnpy.trader.object import ContractData, Exchange, Product
from typing import Set
import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from .utility import get_csv_path

hostname = socket.gethostname()
main_url = "https://api.bybit.com"


class BybitSymbolType(Enum):
    """
    产品类型
    """

    SPOT = "现货"
    USDT = "USDT永续合约"
    USDC = "USDC永续合约"
    SWAP = "反向永续合约"
    FUTURE = "反向交割合约"


# ====== 获取bar数据 ======
# symbol：'BTCUSD'
# interval：'1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W', 'Y'
# from：'%Y-%m-%d %H:%M:%S'
# limit：<= 200
def bybit_get_bar_data(symbol: str, interval: str, from_time: str, limit: int = 200, save_to:str=""):
    timeArray = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    if "USDT" in symbol:
        url = f"{main_url}/public/linear/kline?symbol={symbol}&interval={interval}&from={timeStamp}&limit={limit}"
    else:
        url = f"{main_url}/v2/public/kline/list?symbol={symbol}&interval={interval}&from={timeStamp}&limit={limit}"
    resp = requests.get(url, headers={}, params={})
    data = resp.json()
    bar_data = data.get("result", [])
    if not bar_data:
        bar_data = []

    # 数据整理
    result_list = []
    since = ""
    until = ""
    for dic in bar_data:
        # 转换时间戳
        the_timestamp = dic["open_time"]
        datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
        if not since:
            since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
        until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))

        if "open_time" in dic:
            dic.pop("open_time")
        if "interval" in dic:
            dic.pop("interval")
        if "turnover" in dic:
            dic.pop("turnover")
        if "id" in dic:
            dic.pop("id")
        if "period" in dic:
            dic.pop("period")
        if "start_at" in dic:
            dic.pop("start_at")
        dic["datetime"] = datetime_str
        result_list.append(dic)

    if not len(result_list):
        return None

    # 写入csv
    contract = f"BYBIT.{symbol}"
    csv_path = get_csv_path(target_dir=save_to)
    dir_path = csv_path + f"{contract}{DIR_SYMBOL}{interval}{DIR_SYMBOL}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = dir_path + f"{since}__{until}.csv"
    field_names = ["datetime", "symbol", "open", "high", "low", "close", "volume"]
    with open(file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        # 写入csv文件
        writer.writerows(result_list)

    return datetime.strptime(until, "%Y-%m-%d-%H%M%S")

def bybit_get_first_bar_datetime(symbol: str, interval: str, from_time: str = "2020-01-01 00:00:00"):
    first_bar_dt = None

    # 获取Bar列表
    timeArray = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArray)) * 1000
    url = f"{main_url}/v5/market/kline?symbol={symbol}&interval={interval}&start={timestamp}"
    resp = requests.get(url, headers={}, params={})
    result = resp.json().get("result", {})
    data = result.get("list", [])
    if data:
        start_time, openPrice, highPrice, lowPrice, closePrice, volume, turnover = data[-1]
        first_bar_dt = datetime.fromtimestamp(int(int(start_time) / 1000))
        
    return first_bar_dt

def bybit_get_latest_price(symbol: str):
    from_time = datetime.now() - timedelta(hours=1)
    from_time = from_time.strftime("%Y-%m-%d %H:%M:%S")
    timeArray = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    if "USDT" in symbol:
        url = f"{main_url}/public/linear/kline?symbol={symbol}&interval=1&from={timeStamp}&limit=100"
    else:
        url = f"{main_url}/v2/public/kline/list?symbol={symbol}&interval=1&from={timeStamp}&limit=100"
    resp = requests.get(url, headers={}, params={})
    data = resp.json()
    bar_data = data.get("result", [])
    latest_price = 0
    if bar_data:
        latest_bar = bar_data[-1]
        latest_price = latest_bar["close"]
    return latest_price


def bybit_get_symbol_list(need_data: bool = False):
    symbol_list: Set[str] = set()
    symbol_data_dict = {}

    cursor = ""
    init = True
    while init or cursor:
        init = False
        url = f"{main_url}/v5/market/instruments-info"
        params = {"category": "linear",
                  "cursor": cursor}
        resp = requests.get(url, headers={}, params=params)
        result = resp.json().get("result", {})
        cursor = result.get("nextPageCursor", "")
        data = result.get("list", [])
        for d in data:
            # contract: ContractData = ContractData(
            #     symbol=d["name"],
            #     exchange=Exchange.BYBIT,
            #     name=d["name"],
            #     product=Product.FUTURES,
            #     size=1,
            #     pricetick=float(d["price_filter"]["tick_size"]),
            #     min_volume=d["lot_size_filter"]["min_trading_qty"],
            #     history_data=True,
            #     gateway_name='BYBIT'
            # )
            quote_coin = d["quoteCoin"]
            status = d["status"]
            if quote_coin == "USDT" and status == "Trading":
                symbol = d["symbol"]
                symbol_list.add(symbol)
                symbol_data_dict[symbol] = d
            

    symbol_list = sorted(list(symbol_list))
    if need_data:
        return symbol_list, symbol_data_dict

    else:
        return symbol_list


def bybit_get_min_value(filter: float):
    # 获取交易对最小交易价值
    usdt_symbol_list, data = bybit_get_symbol_list(
        type=BybitSymbolType.USDT, need_data=True
    )
    print(f"\n所有USDT永续交易对：{len(usdt_symbol_list)}\n")
    print(f"满足筛选条件的交易对")
    count = 0
    rusult_list = []
    for symbol in usdt_symbol_list:
        d = data[symbol]

        # 交易所合约
        full_symbol = f"{symbol}.BYBIT"

        # 最小交易数量
        min_volume = d["lot_size_filter"]["min_trading_qty"]

        # 数据库获取起始日期
        client = MongoClient("localhost", 27017)
        db = client[MINUTE_DB_NAME]
        collection = db[full_symbol]
        start_data = collection.find_one(sort=[("datetime", ASCENDING)])
        db_start_dt = start_data["datetime"] if start_data else None
        end_data = collection.find_one(sort=[("datetime", DESCENDING)])

        # 获取最新的价格
        # price = bybit_get_latest_price(symbol=symbol) # 接口获取实时最新价格
        price = end_data["close_price"] if end_data else None  # 数据库获取最新价格

        # 最小交易价值
        value = min_volume * price

        if value <= filter:
            print(f"{symbol}\t\t最新价格：{price}\t\t最小交易数量：{min_volume}\t\t价值：{value}")
            count += 1
            rusult_list.append(
                {
                    "symbol": full_symbol,
                    "price": price,
                    "min_volume": min_volume,
                    "value": value,
                    "start_dt": db_start_dt,
                }
            )
    print(f"总计：{count}")

    # 写入CSV
    csv_path = get_csv_path()
    # csv文件路径
    dir_path = csv_path + f"min_value{DIR_SYMBOL}"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    csv_file_path = f"{dir_path}filter_{filter}.csv"
    results_sorted = pd.DataFrame(rusult_list)
    results_sorted = results_sorted.sort_values("start_dt", ascending=True)
    results_sorted.to_csv(csv_file_path, index=False)

    return rusult_list


def bybit_marting_setting(min_value_filter: float = 0):
    # 获取交易对最小交易价值
    usdt_symbol_list, data = bybit_get_symbol_list(
        type=BybitSymbolType.USDT, need_data=True
    )
    print(f"\n所有USDT永续交易对：{len(usdt_symbol_list)}")

    filter_count = 0
    rusult_list = []
    for symbol in usdt_symbol_list:
        d = data[symbol]

        # 交易所合约
        full_symbol = f"{symbol}.BYBIT"

        # 最小价格变动
        price_tick = d["price_filter"]["tick_size"]

        # 最小交易数量
        min_volume = d["lot_size_filter"]["min_trading_qty"]

        # 数据库起始日期
        client = MongoClient("localhost", 27017)
        db = client[MINUTE_DB_NAME]
        collection = db[full_symbol]
        start_data = collection.find_one(sort=[("datetime", ASCENDING)])
        db_start_dt = start_data["datetime"] if start_data else None

        if min_value_filter:
            # 最新的价格
            # latest_price = bybit_get_latest_price(symbol=symbol) # 接口获取实时最新价格
            end_data = collection.find_one(sort=[("datetime", DESCENDING)])  # 数据库获取最新价格
            latest_price = end_data["close_price"] if end_data else None
            if not latest_price:
                continue

            # 最小交易价值
            latest_min_value = min_volume * latest_price

            if latest_min_value <= min_value_filter:
                # print(f"{symbol}\t\t最新价格：{latest_price}\t\t最小交易数量：{min_volume}\t\t价值：{value}")
                filter_count += 1
                rusult_list.append(
                    {
                        "symbol": full_symbol,
                        "priceTick": price_tick,
                        "variableCommission": 0.0006,
                        "slippage": 1,
                        "min_volume": min_volume,
                        "latest_price": latest_price,
                        "latest_min_value": latest_min_value,
                        "start_dt": db_start_dt,
                    }
                )

        else:
            rusult_list.append(
                {
                    "symbol": full_symbol,
                    "priceTick": price_tick,
                    "variableCommission": 0.0006,
                    "slippage": 1,
                    "min_volume": min_volume,
                    "start_dt": db_start_dt,
                }
            )

    if min_value_filter:
        print(f"满足筛选条件的交易对：{filter_count}")

    # 写入CSV
    csv_path = get_csv_path()
    if not os.path.exists(csv_path):
        os.makedirs(csv_path)
    if min_value_filter:
        csv_file_path = f"{csv_path}bybit_marting_backtesting_setting_filter_{min_value_filter}.csv"
    else:
        csv_file_path = f"{csv_path}bybit_marting_backtesting_setting.csv"
    results_sorted = pd.DataFrame(rusult_list)
    results_sorted = results_sorted.sort_values("start_dt", ascending=True)
    results_sorted.to_csv(csv_file_path, index=False)

    return rusult_list

if __name__ == "__main__":
    """
    # 获取Bar数据
    symbol = 'BTCUSD'
    interval = '1'
    from_time = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    bybit_get_bar_data(symbol=symbol, interval=interval, from_time=from_time)
    print('completed！')
    """

    """
    # 获取交易对列表
    spot_symbol_list = bybit_get_symbol_list(type=BybitSymbolType.SPOT)
    usdt_symbol_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)

    spot_only_list = []
    for spot_symbol in spot_symbol_list:
        if spot_symbol not in usdt_symbol_list:
            spot_only_list.append(spot_symbol)

    usdt_only_list = []
    for usdt_symbol in usdt_symbol_list:
        if usdt_symbol not in spot_symbol_list:
            usdt_only_list.append(usdt_symbol)

    print(f"====== USDT永续交易 ======")
    for symbol in usdt_symbol_list:
        print(symbol)
    print(f"BYBIT_USDT永续合约总计：{len(usdt_symbol_list)}")

    # print(f"====== 现货独享交易 ======")
    # for symbol in spot_only_list:
    #     print(symbol)

    # print(f"====== USDT永续独享交易 ======")
    # for symbol in usdt_only_list:
    #     print(symbol)
    """

    # # 获取所有USDT永续合约最小交易价值，并筛选
    # bybit_get_min_value(filter=0.5)

    # 生成马丁策略回测参数
    bybit_marting_setting(min_value_filter=0)
