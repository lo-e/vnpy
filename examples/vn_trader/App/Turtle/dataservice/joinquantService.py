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

class jqSymbolData:
    display_name = ''
    symbol = ''
    start_date:datetime = None
    end_date:datetime = None
    type = ''

def jq_get_all_trading_symbol_list():
    info = get_all_securities(types=['futures'])
    today = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
    result_list = []
    for index, row in info.iterrows():
        start = datetime.strptime(row['start_date'].strftime('%Y-%m-%d'), '%Y-%m-%d')
        end = datetime.strptime(row['end_date'].strftime('%Y-%m-%d'), '%Y-%m-%d')
        if start <= today <= end:
            data = jqSymbolData()
            data.display_name = row['display_name']
            data.symbol = row['name'].upper()
            data.start_date = start
            data.end_date = end
            data.type = row['type']
            result_list.append(data)
    return result_list

def transform_jqcode(symbol:str):
    jq_code = symbol.upper()
    startSymbol = re.sub("\d", "", jq_code)
    for key in EXCHANGE_SYMBOL_MAP.keys():
        underlying_list = EXCHANGE_SYMBOL_MAP[key]
        if startSymbol in underlying_list:
            jq_code = jq_code + f'.{key}'
            break
    return jq_code

# 下载Bar数据【包括日线、分钟线】
# symbol：'RB2005'
# start & end：'2020-01-01' '2020-01-01 09:00'
# frequency：'1d'、'1m'
def download_bar_data(symbol:str, start:str, end:str, frequency:str='1d', to_database:bool=False):
    jq_symbol = transform_jqcode(symbol=symbol)

    data = get_price(jq_symbol, start_date=start, end_date=end, frequency=frequency, fields=None, skip_paused=True, fq='pre')
    bar_list = []
    for index, row in data.iterrows():
        datetime_str = index.strftime('%Y-%m-%d %H:%M')
        bar = generateBar(row=row, symbol=symbol, datetime_str=datetime_str)
        bar_list.append(bar)
    return bar_list

# 下载持仓量
# symbol_list：['RB2005', 'HC2005]
# date：'2020-01-01'
def download_open_interest(symbol_list:list, date:str):
    jq_symbol_list = []
    for symbol in symbol_list:
        jq_symbol = transform_jqcode(symbol)
        jq_symbol_list.append(jq_symbol)

    data = get_extras(info='futures_positions', security_list=jq_symbol_list, start_date=date, end_date=date, df=True)
    the_list = []
    for index, row in data.iterrows():
        the_datetime = datetime.strptime(index.strftime('%Y-%m-%d'), '%Y-%m-%d')
        row_dict = dict(row)
        for key, value in row_dict.items():
            data = open_interest_data()
            data.symbol = key.split('.')[0].upper()
            data.datetime = the_datetime
            data.open_interest = value
            the_list.append(data)
    return the_list

# 生成BarData
# symbol：'RB2005'
# datetime_str：'2020-01-01'
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

# 判断主力合约并存入数据库，指定起始日期
def jq_get_and_save_dominant_symbol_from(underlying_symbol:str, from_date:datetime):
    return_msg = ''
    today_date = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')
    target_date = from_date
    while target_date <= today_date:
        try:
            new_dominant, msg = jq_get_and_save_dominant_symbol(underlying_symbol=underlying_symbol, target_date=target_date)
            if new_dominant:
                # 有新的主力产生
                msg = f'{msg}\t新主力'
                # 下载新主力的历史数据
                #bar_list, download_msg = downloadDailyData(ts_code=trasform_tscode(new_dominant), start='', end='', to_database=True)
                #msg = f'{msg}\n{download_msg}'
            #print(msg)
            #return_msg += msg
            target_date += timedelta(days=1)
        except:
            msg = traceback.format_exc()
            return_msg += msg
            break

    return return_msg

# 判断主力合约并存入数据库，指定单个日期
def jq_get_and_save_dominant_symbol(underlying_symbol:str, target_date:datetime) -> (str, str):
    # 获取合约列表
    all_symbol_list = jq_get_all_trading_symbol_list()

    # 获取目标代码还在交易的所有合约数据
    target_symbol_data_list = []
    for symbol_data in all_symbol_list:
        if symbol_data.start_date and symbol_data.end_date:
            start_symbol = re.sub("\d", "", symbol_data.symbol).upper()
            if underlying_symbol == start_symbol and symbol_data.start_date <= target_date and symbol_data.end_date >= target_date:
                if '主力' in symbol_data.display_name or '指数' in symbol_data.display_name:
                    continue
                target_symbol_data_list.append(symbol_data)

    # 数据库获取最新主力合约代码
    collection = dbDominant[underlying_symbol]
    cursor = collection.find().sort('date', direction=DESCENDING)
    last_dominant_symbol = ''
    last_dominant_date = None
    for dic in cursor:
        last_dominant_symbol = dic['symbol']
        last_dominant_date = dic['date']
        break

    if last_dominant_date > target_date:
        return ('', f'{last_dominant_symbol} -> {target_date}')

    # 判断主力合约
    new_dominant_symbol = ''
    if last_dominant_symbol:
        # 若合约持仓量大于当前主力合约持仓量的1.1倍时，新主力产生
        target_symbol_list = []
        for symbol_data in target_symbol_data_list:
            target_symbol_list.append(symbol_data.symbol.upper())

        # 下载持仓数据
        target_date_str = datetime.strftime(target_date, '%Y-%m-%d')
        open_interest_list = download_open_interest(symbol_list=target_symbol_list, date=target_date_str)
        if not open_interest_list:
            return ('', f'{underlying_symbol}\t{target_date} 持仓量数据缺失，无法判断主力！')
        # 找到当前主力持仓量
        last_dominant_open_interest = 0
        for the_data in open_interest_list:
            if the_data.symbol == last_dominant_symbol:
                last_dominant_open_interest = the_data.open_interest
                break
        if not last_dominant_open_interest:
            return ('', f'{underlying_symbol}\t{target_date} 当前主力持仓量数据缺失，无法判断主力！')
        # 判断新主力
        for the_data in open_interest_list:
            if the_data.open_interest > last_dominant_open_interest * 1.1:
                if new_dominant_symbol:
                    return ('', f'{underlying_symbol}\t{target_date}出现不止一个新主力合约，检查代码！！')
                new_dominant_symbol = the_data.symbol

    else:
        # 持仓量最大的为下一交易日主力
        a = 2

    if new_dominant_symbol:
        # 产生新主力
        a = 2
    else:
        # 没有新主力
        a = 2

if __name__ == '__main__':
    #"""
    # 下载持仓量
    symbol_list = ['RB2005', 'RB2001']
    download_open_interest(symbol_list=symbol_list, date='2019-12-16')
    #"""

    """
    # 下载合约Bar数据【日线、分钟线】
    symbol = 'RB2005'
    bar_list = download_bar_data(symbol=symbol, start = '2019-12-16', end='2019-12-20', frequency='1d')
    """

    """
    # 下载指数Bar数据【日线、分钟线】
    symbol = 'RB8888'
    bar_list = download_bar_data(symbol=symbol, start = '2019-12-16', end='2019-12-20', frequency='1d')
    a = 2
    """

