#-- coding: utf-8 --

import requests
import time
import os
import csv
from datetime import datetime, timedelta
from vnpy.trader.utility import DIR_SYMBOL

main_url = 'https://www.okex.com'

# ====== 获取bar数据 ======
# symbol：'BTCUSD'
# interval：'1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W', 'Y'
# from：'%Y-%m-%d %H:%M:%S'
# limit：<= 200
def okex_get_bar_data(symbol:str, interval:str, from_time:str='', limit:int=1000):
    # 获取from_time时间点往前的历史数据，每次请求获取100条，limit为总数据量，
    api = '/api/v5/market/candles'
    api = '/api/v5/market/history-candles'

    since = ''
    since_ts = ''
    until = ''
    result_list = []
    base_url = f'{main_url}{api}?instId={symbol}&bar={interval}&limit=100'

    if from_time:
        timeArray = time.strptime(from_time, "%Y-%m-%d %H:%M:%S")
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
            for data in bar_data_list:
                ts, o, h, l, c, vol, _ = data
                data_dic = {}
                # 转换时间戳
                the_timestamp = int(ts) / 1000
                datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(the_timestamp))
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

            sub = len(result_list) - limit
            if sub >= 0:
                del result_list[:sub]
                break

            print(f'======  {symbol} {since} -> {until} ======')
        else:
            break

    if not len(result_list):
        return None

    # 写入csv
    contract = f'OKEX.{symbol}'
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

    return datetime.strptime(since, "%Y-%m-%d-%H%M%S")

def get_csv_path():
    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    csv_path = path.rstrip(file_name) + f'CSVs{DIR_SYMBOL}'
    return csv_path

if __name__ == '__main__':
    #"""
    symbol = 'BTC-USD-211105'
    interval = '1D'
    #from_time = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    from_time = ''
    okex_get_bar_data(symbol=symbol, interval=interval, from_time=from_time, limit=200)
    print('completed！')
    #"""
