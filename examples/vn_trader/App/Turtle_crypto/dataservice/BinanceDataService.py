#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL
from enum import Enum

main_url_spot = 'https://api.binance.com'
main_url_inverse = 'https://dapi.binance.com'
main_url_usdt = 'https://fapi.binance.com'


class Binancetype(Enum):
    """
    Interval of bar data.
    """
    SPOT = "spot"
    INVERSE = "inverse"
    USDT = "usdt"

# ====== 获取bar数据 ======
# symbol：'BTC-PERP'
# interval：'1m', '1h', '1d'
# start：'%Y-%m-%d %H:%M:%S'
# end：'%Y-%m-%d %H:%M:%S'
# limit：<= 1500
def binance_get_bar_data(symbol:str, interval:str, symbol_type:Binancetype, start_time:str='', end_time:str='', limit:int=1500):
    params: dict = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    since = ''
    until = ''
    result_list = []
    if symbol_type == Binancetype.SPOT:
        api = "/api/v3/klines"
        base_url = f'{main_url_spot}{api}'

    elif symbol_type == Binancetype.INVERSE:
        api = '/dapi/v1/klines'
        base_url = f'{main_url_inverse}{api}'

    elif symbol_type == Binancetype.USDT:
        api = "/fapi/v1/klines"
        base_url = f'{main_url_usdt}{api}'
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

    resp = requests.get(url, headers={}, params=params)
    bar_data_list = resp.json()

    if bar_data_list:
        # 数据整理
        for data in bar_data_list:
            ts, o, h, l, c, vol, endt, _, _, _, _, _ = data
            data_dic = {}
            # 转换时间戳
            the_timestamp = int(ts) / 1000
            datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
            if not since:
                since = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))
            until = time.strftime("%Y-%m-%d-%H%M%S", time.localtime(the_timestamp))

            data_dic['datetime'] = datetime_str
            if symbol_type == Binancetype.SPOT:
                data_dic['symbol'] = symbol + '_SPOT'

            elif symbol_type == Binancetype.INVERSE:
                data_dic['symbol'] = symbol + '_INV'


            else:
                data_dic['symbol'] = symbol

            data_dic['open'] = str(o)
            data_dic['high'] = str(h)
            data_dic['low'] = str(l)
            data_dic['close'] = str(c)
            data_dic['volume'] = str(vol)
            result_list.append(data_dic)

        print(f'======  {symbol} {since} -> {until} ======')

    if not len(result_list):
        return None

    # 写入csv
    contract = f'BINANCE.{symbol}'
    csv_path = get_csv_path()
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

def binance_get_symbol_list(need_data: bool = False):
    # ====== 只支持USDT正向合约 ======
    symbol_list = set()
    symbol_data_dict = {}

    # 发起请求
    url = f"{main_url_usdt}/fapi/v1/exchangeInfo"
    resp = requests.get(url, headers={}, params={})
    data = resp.json()
    data = data.get("symbols", [])
    for d in data:
        symbol = d["symbol"]
        asset = d["quoteAsset"]
        if asset == "USDT":
            symbol_list.add(symbol)
            symbol_data_dict[symbol] = d

    # 排序
    symbol_list = sorted(list(symbol_list))

    # 返回结果
    if need_data:
        return symbol_list, symbol_data_dict

    else:
        return symbol_list

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path

if __name__ == '__main__':
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

    binance_get_bar_data(symbol=symbol, interval=interval, symbol_type=Binancetype.USDT, start_time=start_time, end_time=end_time, limit=1500)
    print('completed！')
    """

    #"""
    # 获取正向永续合约列表
    symbol_list = binance_get_symbol_list(need_data=False)
    for symbol in symbol_list:
        print(symbol)
    print(f"BINANCE_USDT正向永续合约总计：{len(symbol_list)}")
    #"""
