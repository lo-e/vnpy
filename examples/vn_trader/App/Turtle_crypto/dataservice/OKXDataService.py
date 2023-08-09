#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL
from enum import Enum
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
import pandas as pd

main_url = 'https://www.okex.com'

class OKXType(Enum):
    USDT = "usdt"
    USDC = "usdc"
    INVERSE = "inverse"

# ====== 获取bar数据 ======
# symbol：'BT-CUSD-SWAP'
# interval：'1m/3m/5m/15m/30m/1H/2H/4H 香港时间开盘价k线：[6H/12H/1D/1W/1M/3M/6M/1Y] UTC时间开盘价k线：[/6Hutc/12Hutc/1Dutc/1Wutc/1Mutc/3Mutc/6Mutc/1Yutc]'
# from：'%Y-%m-%d %H:%M:%S'
def okx_get_bar_data(symbol:str, interval:str, from_time:str='', limit:int=100, save_to:str=""):
    # 获取from_time时间点往前的历史数据，每次请求获取100条，limit为总数据量，
    api = '/api/v5/market/history-candles'

    since = ''
    since_ts = ''
    until = ''  
    result_list = []
    first_bar_dt = None
    base_url = f'{main_url}{api}?instId={symbol}&bar={interval}&limit=100'

    if from_time:
        if interval == "1m":
            since = (datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=limit)).strftime("%Y-%m-%d %H:%M:%S")
        
        elif interval == "1D":
            since = (datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=limit)).strftime("%Y-%m-%d %H:%M:%S")

        timeArray = time.strptime(since, "%Y-%m-%d %H:%M:%S")
        since_ts = int(time.mktime(timeArray)) * 1000

    while True:
        if since_ts:
            url = base_url + f'&after={since_ts}'
        else:
            url = base_url
        resp = requests.get(url, headers={}, params={})
        data = resp.json()
        bar_data_list = data.get('data', [])

        if bar_data_list:
            # 数据整理
            end = False
            for data in bar_data_list:
                ts, o, h, l, c, vol, _, __, ___ = data
                data_dic = {}
                # 转换时间戳
                the_timestamp = int(ts) / 1000
                datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
                if datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S") < datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S"):
                    end = True
                    break

                if not until:
                    until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
                since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
                since_ts = ts

                data_dic['datetime'] = datetime_str
                data_dic['symbol'] = symbol
                data_dic['open'] = str(o)
                data_dic['high'] = str(h)
                data_dic['low'] = str(l)
                data_dic['close'] = str(c)
                data_dic['volume'] = str(vol)
                result_list.insert(0, data_dic)

            if end:
                break

        else:
            first_bar_dt = okx_get_first_bar_datetime(symbol=symbol, from_time=from_time)
            if first_bar_dt and first_bar_dt > datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S"):
                from_time = first_bar_dt.strftime("%Y-%m-%d %H:%M:%S")
                if interval == "1m":
                    since = (datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=limit)).strftime("%Y-%m-%d %H:%M:%S")
                
                elif interval == "1D":
                    since = (datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=limit)).strftime("%Y-%m-%d %H:%M:%S")

                timeArray = time.strptime(since, "%Y-%m-%d %H:%M:%S")
                since_ts = int(time.mktime(timeArray)) * 1000

            else:
                break
    
    if not len(result_list):
        if interval == "1m" and first_bar_dt and datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S") < datetime.now() - timedelta(minutes=1):
            raise("数据下载空")
        return None

    # 数据起止时间
    print(f'======  {symbol} {since} -> {until} ======')

    # 写入csv
    contract = f'OKX.{symbol}'
    csv_path = get_csv_path(target_dir=save_to)
    dir_path = csv_path + f'{contract}{DIR_SYMBOL}{interval}{DIR_SYMBOL}'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = dir_path + f'{since}__{until}.csv'
    field_names = ['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        # 写入csv文件
        writer.writerows(result_list)

    return datetime.strptime(until, "%Y-%m-%d-%H%M%S")

def okx_get_symbol_list(type:OKXType=OKXType.USDT, need_data: bool = False):
    symbol_list = set()
    symbol_data_dict = {}

    # 发起请求
    url = f"{main_url}/api/v5/public/instruments"
    resp = requests.get(url, headers={}, params={"instType": "SWAP"})
    data = resp.json()
    data = data["data"]
    for d in data:
        symbol: str = d["instId"]
        if d["ctType"] == "linear":
            if d["settleCcy"] == "USDT" and type == OKXType.USDT:
                symbol_list.add(symbol)
                symbol_data_dict[symbol] = d

            elif d["settleCcy"] == "USDC" and type == OKXType.USDC:
                symbol_list.add(symbol)
                symbol_data_dict[symbol] = d
        
        elif d["ctType"] == "inverse" and type == OKXType.INVERSE:
            symbol_list.add(symbol)
            symbol_data_dict[symbol] = d

    # 排序
    symbol_list = sorted(list(symbol_list))

    # 返回结果
    if need_data:
        return symbol_list, symbol_data_dict

    else:
        return symbol_list

def okx_get_first_bar_datetime(symbol:str, from_time:str=''):
    result = None
    # 获取合约上市日期
    api = '/api/v5/public/instruments'
    url = f'{main_url}{api}?instType=SWAP&instId={symbol}'
    resp = requests.get(url, headers={}, params={})
    data = resp.json()["data"]
    if data:
        contract_data = data[0]
        list_time = contract_data["listTime"]
        list_dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(list_time) / 1000))
        list_dt = datetime.strptime(list_dt_str, "%Y-%m-%d %H:%M:%S")
        list_dt = (list_dt + timedelta(minutes=1)).replace(second=0)
        if from_time:
            from_dt = datetime.strptime(from_time, "%Y-%m-%d %H:%M:%S")
            result = max(list_dt, from_dt)
        else:
            result = list_dt
    return result

def okx_marting_setting(min_value_filter: float = 0):
    # 获取交易对最小交易价值
    usdt_symbol_list, data = okx_get_symbol_list(type=OKXType.USDT, need_data=True)
    print(f"\n所有USDT永续交易对：{len(usdt_symbol_list)}")

    filter_count = 0
    rusult_list = []
    for symbol in usdt_symbol_list:
        d = data[symbol]

        # 交易所合约
        full_symbol = f"{symbol}.OKX"

        # 最小价格变动
        price_tick = float(d["tickSz"])

        # 最小下单数量，合约的数量单位是“张”，现货的数量单位是“交易货币”量
        minSz = float(d["minSz"])

        # 合约面值
        ctValue = d["ctVal"]
        ctValue = float(ctValue) if ctValue else 0

        # 最小下单数量（按合约面值计价币种）
        min_volume = minSz * ctValue if ctValue else minSz

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
                        "variableCommission": 0.0005,
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
        csv_file_path = f"{csv_path}okx_marting_backtesting_setting_filter_{min_value_filter}.csv"
    else:
        csv_file_path = f"{csv_path}okx_marting_backtesting_setting.csv"
    results_sorted = pd.DataFrame(rusult_list)
    results_sorted = results_sorted.sort_values("start_dt", ascending=True)
    results_sorted.to_csv(csv_file_path, index=False)

    return rusult_list

def get_csv_path(target_dir: str = ""):
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    if target_dir:
        csv_path = path.rstrip(file_name) + f"CSVs_{DIR_SYMBOL}" + f"{target_dir}{DIR_SYMBOL}"
    else:
        csv_path = path.rstrip(file_name) + f"CSVs{DIR_SYMBOL}"
    return csv_path

def delete_okex():
    symbol_list = okx_get_symbol_list(type=OKXType.USDT)
    # for symbol in symbol_list:
    #     print(symbol)
    # print(f"总计：{len(symbol_list)}")

    valid_count = 0
    client = MongoClient("localhost", 27017)
    db = client[MINUTE_DB_NAME]
    index = 0
    for symbol in symbol_list:
        index += 1
        collection_name = f"{symbol}.OKEX"
        collection = db[collection_name]
        cursor = collection.find().sort("datetime", ASCENDING)
        count = len(list(cursor))
        if count:
            valid_count += 1
        print(f"\n{collection_name}：{count}")
        collection.drop()
        print(f"删除成功{index}！")

    print(f"\n有效删除统计：{valid_count}")

    # collection_name = f"1INCH-USDT-SWAP.OKEX"
    # collection = db[collection_name]
    # cursor = collection.find().sort("datetime", ASCENDING)
    # print(len(list(cursor)))
    # result = collection.drop()
    # print(result)

if __name__ == '__main__':
    """
    symbol = 'BTC-USDT-SWAP'
    interval = '1m'
    from_time = ''
    from_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    okx_get_bar_data(symbol=symbol, interval=interval, from_time=from_time, limit=100)
    print('completed！')
    """

    """
    # 获取所有合约列表
    symbol_list = okx_get_symbol_list(type=OKXType.USDT)
    for symbol in symbol_list:
        print(symbol)
    print(f"总计：{len(symbol_list)}")
    """

    """
    # 获取合约从某个时间开始最早的交易时间
    symbol = 'BTC-USDT-SWAP'
    from_time = "2019-01-01 00:00:00"
    okx_get_first_bar_datetime(symbol=symbol, from_time=from_time)
    """

    # 生成马丁策略回测参数
    okx_marting_setting(min_value_filter=0)