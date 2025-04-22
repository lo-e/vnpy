# -- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL, LOCAL_IP
from enum import Enum
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
import pandas as pd
import socket
from .utility import get_csv_path

# 域名
main_url_spot = "https://api.binance.com"
main_url_inverse = "https://dapi.binance.com"
main_url_usdt = "https://fapi.binance.com"

# 代理
proxy = f"{LOCAL_IP}:10811"
proxies = {
    "http": proxy,
    "https": proxy,
}

class BinanceType(Enum):
    SPOT = "spot"
    INVERSE = "inverse"
    USDT = "usdt"


# ====== 获取bar数据 ======
# symbol：'BTC-PERP'
# interval：'1m', '1h', '1d'
# start：'%Y-%m-%d %H:%M:%S'
# end：'%Y-%m-%d %H:%M:%S'
# limit：<= 1500
def binance_get_bar_data(
    symbol: str,
    interval: str,
    symbol_type: BinanceType,
    start_time: str = "",
    end_time: str = "",
    limit: int = 1500,
    save_to:str=""
):
    params: dict = {"symbol": symbol, "interval": interval, "limit": limit}

    since = ""
    until = ""
    result_list = []
    if symbol_type == BinanceType.SPOT:
        api = "/api/v3/klines"
        base_url = f"{main_url_spot}{api}"

    elif symbol_type == BinanceType.INVERSE:
        api = "/dapi/v1/klines"
        base_url = f"{main_url_inverse}{api}"

    elif symbol_type == BinanceType.USDT:
        api = "/fapi/v1/klines"
        base_url = f"{main_url_usdt}{api}"
    else:
        return

    url = base_url
    if start_time:
        timeArray = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = int(time.mktime(timeArray))
        params["startTime"] = start_time * 1000

    if end_time:
        timeArray = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        end_time = int(time.mktime(timeArray))
        params["endTime"] = end_time * 1000

    client = socket.gethostname()
    if "MI-PRO" in client:
        resp = requests.get(url, headers={}, params=params, proxies=proxies)
    
    else:
        resp = requests.get(url, headers={}, params=params)

    if "until" in resp.text:
        i_until = resp.text.index("until")
        i_please = resp.text.index(". Please")
        timestamp = float(resp.text[i_until + 6 : i_please])
        banned_to = datetime.fromtimestamp(timestamp / 1000)
        print(f"访问受限：{banned_to}")
        time.sleep(2)
        raise ("访问受限")

    bar_data_list = resp.json()
    if bar_data_list:
        # 数据整理
        for data in bar_data_list:
            ts, o, h, l, c, vol, endt, _, _, _, _, _ = data
            data_dic = {}
            # 转换时间戳
            the_timestamp = int(ts) / 1000
            datetime_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp)
            )
            if not since:
                since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
            until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))

            data_dic["datetime"] = datetime_str
            if symbol_type == BinanceType.SPOT:
                data_dic["symbol"] = symbol + "_SPOT"

            elif symbol_type == BinanceType.INVERSE:
                data_dic["symbol"] = symbol + "_INV"

            else:
                data_dic["symbol"] = symbol

            data_dic["open"] = str(o)
            data_dic["high"] = str(h)
            data_dic["low"] = str(l)
            data_dic["close"] = str(c)
            data_dic["volume"] = str(vol)
            result_list.append(data_dic)

        # print(f"{symbol} {since} -> {until}")

    if not len(result_list):
        return None

    # 写入csv
    contract = f"BINANCE.{symbol}"
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


def binance_get_first_bar_datetime(
    symbol: str, interval: str, symbol_type: BinanceType, start_time: str = ""
):
    result = None
    params: dict = {"symbol": symbol, "interval": interval, "limit": 10}

    if symbol_type == BinanceType.SPOT:
        api = "/api/v3/klines"
        base_url = f"{main_url_spot}{api}"

    elif symbol_type == BinanceType.INVERSE:
        api = "/dapi/v1/klines"
        base_url = f"{main_url_inverse}{api}"

    elif symbol_type == BinanceType.USDT:
        api = "/fapi/v1/klines"
        base_url = f"{main_url_usdt}{api}"
    else:
        return

    url = base_url
    if start_time:
        timeArray = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        start_time = int(time.mktime(timeArray))
        params["startTime"] = start_time * 1000
    
    client = socket.gethostname()
    if "MI-PRO" in client:
        resp = requests.get(url, headers={}, params=params, proxies=proxies)
    
    else:
        resp = requests.get(url, headers={}, params=params)
    bar_data_list = resp.json()

    if bar_data_list:
        # 数据整理
        for data in bar_data_list:
            ts = data[0]
            result = datetime.fromtimestamp(int(ts) / 1000)
            break
    return result


def binance_get_symbol_list(need_data: bool = False):
    # ====== 只支持USDT正向合约 ======
    symbol_list = set()
    symbol_data_dict = {}

    # 发起请求
    url = f"{main_url_usdt}/fapi/v1/exchangeInfo"
    client = socket.gethostname()
    if "MI-PRO" in client:
        resp = requests.get(url, headers={}, params={}, proxies=proxies)
    
    else:
        resp = requests.get(url, headers={}, params={})
    data = resp.json()
    data = data.get("symbols", [])
    for d in data:
        symbol = d["symbol"]
        asset = d["quoteAsset"]
        type = d["contractType"]
        status = d["status"]
        if asset == "USDT" and type == "PERPETUAL" and status == "TRADING":
            symbol_list.add(symbol)
            symbol_data_dict[symbol] = d

    # 排序
    symbol_list = sorted(list(symbol_list))

    # 返回结果
    if need_data:
        return symbol_list, symbol_data_dict

    else:
        return symbol_list


def binance_marting_setting(min_value_filter: float = 0):
    # 获取交易对最小交易价值
    usdt_symbol_list, data = binance_get_symbol_list(need_data=True)
    print(f"\n所有USDT永续交易对：{len(usdt_symbol_list)}")

    filter_count = 0
    rusult_list = []
    for symbol in usdt_symbol_list:
        d = data[symbol]

        # 交易所合约
        full_symbol = f"{symbol}.BINANCE"

        # 最小价格变动
        price_tick = d["filters"][0]["tickSize"]

        # 最小交易数量
        min_volume = d["filters"][2]["minQty"]

        # 数据库起始日期
        client = MongoClient("localhost", 27017)
        db = client[MINUTE_DB_NAME]
        collection = db[full_symbol]
        start_data = collection.find_one(sort=[("datetime", ASCENDING)])
        db_start_dt = start_data["datetime"] if start_data else None

        if min_value_filter:
            # 最新的价格
            # latest_price = binance_get_latest_price(symbol=symbol) # 接口获取实时最新价格
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
                        "variableCommission": 0.0004,
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
                    "variableCommission": 0.0004,
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
        csv_file_path = f"{csv_path}binance_marting_backtesting_setting_filter_{min_value_filter}.csv"
    else:
        csv_file_path = f"{csv_path}binance_marting_backtesting_setting.csv"
    results_sorted = pd.DataFrame(rusult_list)
    results_sorted = results_sorted.sort_values("start_dt", ascending=True)
    results_sorted.to_csv(csv_file_path, index=False)

    return rusult_list



if __name__ == "__main__":
    """
    # 下载分钟Bar数据
    symbol = 'BTCUSDT'
    interval = '1m'
    start_time = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d") + ' 00:00:00'
    end_time = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d") + ' 00:00:00'
    #start_time = ''
    #end_time = ''

    start_time = '2020-01-01 00:00:00'
    end_time = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d") + ' 00:00:00'

    binance_get_bar_data(symbol=symbol, interval=interval, symbol_type=BinanceType.USDT, start_time=start_time, end_time=end_time, limit=1500)
    print('completed！')
    """

    #"""
    # 获取正向永续合约列表
    symbol_list = binance_get_symbol_list(need_data=False)
    for symbol in symbol_list:
        print(symbol)
    print(f"BINANCE_USDT永续合约总计：{len(symbol_list)}")

    # 筛选新币种
    print(f"\n------ 符合筛选条件的合约 ------")
    flt_symbol_list = []
    for symbol in symbol_list:
        first_bar_dt = None
        trying = True
        while trying:
            try:
                first_bar_dt = binance_get_first_bar_datetime(symbol=symbol, interval="1m", symbol_type=BinanceType.USDT, start_time="2020-12-01 00:00:00")
                trying = False

            except Exception:
                trying = True

        if first_bar_dt:
            flt_from = datetime.now().replace(second=0) - timedelta(days=90)
            if first_bar_dt >= flt_from:
                flt_symbol_list.append(symbol)
                print(f"{symbol}\t\t{first_bar_dt}")
        
        else:
            print(f"无法获取合约上市日期：{symbol}")
    print(f"总计：{len(flt_symbol_list)}")
    #"""

    """
    # 获取指定时间段的合约最初交易时间
    symbol_list = ['GALAUSDT', 'ZRXUSDT', 'BAKEUSDT', 'SFPUSDT', 'LINAUSDT', 'OMGUSDT', 'RENUSDT', 'KNCUSDT', 'BATUSDT', 'BELUSDT', 'WAVESUSDT', 'ZENUSDT', 'SXPUSDT', 'RLCUSDT', 'PEOPLEUSDT', 'CHRUSDT', 'ARUSDT', 'ARPAUSDT', 'ATAUSDT', 'UNFIUSDT', 'DYDXUSDT', 'OGNUSDT', 'DASHUSDT', 'AUDIOUSDT', 'LRCUSDT', 'SKLUSDT', 'ETHUSDT', 'AXSUSDT', 'MASKUSDT', 'AAVEUSDT', 'ZILUSDT', 'SUSHIUSDT', 'STORJUSDT', 'FTMUSDT', 'ETCUSDT', 'CTSIUSDT', 'KAVAUSDT', 'DOGEUSDT', 'EGLDUSDT', 'SOLUSDT', 'C98USDT', 'CRVUSDT', 'YFIUSDT', 'ALGOUSDT', 'RSRUSDT', 'MKRUSDT', 'ENJUSDT']
    for symbol in symbol_list:
        start_dt = binance_get_first_bar_datetime(symbol=symbol, interval="1m", symbol_type=BinanceType.USDT, start_time="2020-12-01 00:00:00")
        print(f"{symbol}\t{start_dt}")
    """

    """
    # 生成马丁策略回测参数
    binance_marting_setting(min_value_filter=0)
    """
