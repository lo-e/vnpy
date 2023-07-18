#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL
from enum import Enum

main_url = 'https://www.okex.com'

class OKXType(Enum):
    USDT = "usdt"
    USDC = "usdc"
    INVERSE = "inverse"

# ====== 获取bar数据 ======
# symbol：'BT-CUSD-SWAP'
# interval：'1m/3m/5m/15m/30m/1H/2H/4H 香港时间开盘价k线：[6H/12H/1D/1W/1M/3M/6M/1Y] UTC时间开盘价k线：[/6Hutc/12Hutc/1Dutc/1Wutc/1Mutc/3Mutc/6Mutc/1Yutc]'
# from：'%Y-%m-%d %H:%M:%S'
def okx_get_bar_data(symbol:str, interval:str, from_time:str='', limit:int=100):
    # 获取from_time时间点往前的历史数据，每次请求获取100条，limit为总数据量，
    api = '/api/v5/market/history-candles'

    since = ''
    since_ts = ''
    until = ''  
    result_list = []
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
            break
    
    if not len(result_list):
        return None

    # 数据起止时间
    print(f'======  {symbol} {since} -> {until} ======')

    # 写入csv
    contract = f'OKX.{symbol}'
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

def okx_get_first_bar_datetime(symbol:str, interval:str, from_time:str=''):
    result = None

    if not from_time:
        from_time = "2020-01-01 00:00:00"
    
    api = '/api/v5/market/history-candles'
    base_url = f'{main_url}{api}?instId={symbol}&bar={interval}&limit=100'

    until = from_time
    until_ts = ''
    while True:
        if interval == "1m":
            until = (datetime.strptime(until, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=100)).strftime("%Y-%m-%d %H:%M:%S")
        
        elif interval == "1D":
            until = (datetime.strptime(until, "%Y-%m-%d %H:%M:%S") + timedelta(days=100)).strftime("%Y-%m-%d %H:%M:%S")
        timeArray = time.strptime(until, "%Y-%m-%d %H:%M:%S")
        until_ts = int(time.mktime(timeArray)) * 1000

        url = base_url + f'&after={until_ts}'
        resp = requests.get(url, headers={}, params={})
        data = resp.json()
        bar_data_list = data.get('data', [])
        if bar_data_list:
            # 转换时间戳
            ts = bar_data_list[0][0]
            the_timestamp = int(ts) / 1000
            datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
            result = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            break

    return result

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path

if __name__ == '__main__':
    """
    symbol = 'BTC-USDT-SWAP'
    interval = '1m'
    from_time = ''
    from_time = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    okx_get_bar_data(symbol=symbol, interval=interval, from_time=from_time, limit=100)
    print('completed！')
    """

    #"""
    # 获取合约从某个时间开始最早的交易时间
    symbol = 'BTC-USDT-SWAP'
    interval = '1m'
    from_time = "2019-01-01 00:00:00"
    okx_get_first_bar_datetime(symbol=symbol, interval=interval, from_time=from_time)
    #"""
