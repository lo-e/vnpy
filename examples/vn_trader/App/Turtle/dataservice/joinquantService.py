#-- coding: utf-8 --

import tushare as ts
from vnpy.trader.object import BarData, TickData
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from vnpy.app.cta_strategy.base import (MINUTE_DB_NAME,
                                        DAILY_DB_NAME,
                                        TICK_DB_NAME)
from vnpy.trader.constant import Exchange
from copy import copy
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, DOMINANT_DB_NAME
import traceback
from time import sleep
import re
from vnpy.app.cta_strategy.base import TRANSFORM_SYMBOL_LIST
from jqdatasdk import *

# 聚宽账号登陆
auth('18521705317', '970720699')

EXCHANGE_SYMBOL_MAP = {'XSGE':['RB', 'HC'],
                       'XZCE':['SM', 'ZC', 'TA'],
                       'XDCE':['J']}

client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=600)
client.server_info()
dbDominant = client[DOMINANT_DB_NAME]
dbDaily = client[DAILY_DB_NAME]

class open_interest_data:
    symbol = ''
    datetime:datetime = None
    open_interest = 0

# 下载Bar数据【包括日线、分钟线】
# symbol：'RB2005'
# start & end：'2020-01-01'
# frequency：'1d'、'1m'
# skip_paused：是否跳过停牌的时间, 如果不跳过, 停牌时会使用停牌前的数据填充(具体请看[SecurityUnitData]的paused属性)
# fq：'pre'前复权[默认]、'None'不复权, 返回实际价格、'post'后复权
def download_bar_data(symbol:str, start:str, end:str, frequency:str='1d', to_database:bool=False):
    jq_symbol = symbol
    start_symbol = re.sub("\d", "", symbol)
    for key in EXCHANGE_SYMBOL_MAP.keys():
        underlying_list = EXCHANGE_SYMBOL_MAP[key]
        if start_symbol in underlying_list:
            jq_symbol = symbol + f'.{key}'
            break

    data = get_price(jq_symbol, start_date=start, end_date=end, frequency=frequency, fields=None, skip_paused=True, fq='pre')
    bar_list = []
    for index, row in data.iterrows():
        datetime_str = index.strftime('%Y-%m-%d %H:%M')
        bar = generateBar(row=row, symbol=symbol, datetime_str=datetime_str)
        bar_list.append(bar)
    return bar_list

def download_open_interest(symbol_list:list, start:str, end:str):
    jq_symbol_list = []
    for symbol in symbol_list:
        jq_symbol = symbol
        start_symbol = re.sub("\d", "", symbol)
        for key in EXCHANGE_SYMBOL_MAP.keys():
            underlying_list = EXCHANGE_SYMBOL_MAP[key]
            if start_symbol in underlying_list:
                jq_symbol = symbol + f'.{key}'
                break
        jq_symbol_list.append(jq_symbol)

    data = get_extras(info='futures_positions', security_list=jq_symbol_list, start_date=start, end_date=end, df=True)
    result_dict = {}
    for index, row in data.iterrows():
        the_datetime = datetime.strptime(index.strftime('%Y-%m-%d'), '%Y-%m-%d')
        row_dict = dict(row)
        the_list = []
        for key, value in row_dict.items():
            data = open_interest_data()
            data.symbol = key
            data.datetime = the_datetime
            data.open_interest = value
            the_list.append(data)
        result_dict[the_datetime] = the_list
    return result_dict


# 生成BarData
def generateBar(row, symbol, datetime_str):
    """生成K线"""
    # 指数代码进行转换
    end_symbol = re.sub('\D', "", symbol)
    if end_symbol == '8888':
        start_symbol = re.sub("\d", "", symbol)
        symbol = f'{start_symbol}99'

    bar = BarData(gateway_name = '', symbol = symbol, exchange = None, datetime = None, endDatetime = None)
    bar.datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
    bar.open_price = row['open']
    bar.high_price = row['high']
    bar.low_price = row['low']
    bar.close_price = row['close']
    bar.volume = row['volume']
    bar.exchange = 'JQ'
    return bar

if __name__ == '__main__':
    """
    # 下载持仓量
    symbol_list = ['RB2005', 'RB2001']
    download_open_interest(symbol_list=symbol_list, start='2019-12-16', end='2019-12-31')
    """

    """
    # 下载合约Bar数据【日线、分钟线】
    symbol = 'RB2005'
    bar_list = download_bar_data(symbol=symbol, start = '2019-12-16', end='2019-12-20', frequency='1d')
    """

    #"""
    # 下载指数Bar数据【日线、分钟线】
    symbol = 'RB8888'
    bar_list = download_bar_data(symbol=symbol, start = '2019-12-16', end='2019-12-20', frequency='1d')
    a = 2
    #"""

