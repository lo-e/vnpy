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
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, MINUTE_DB_NAME, DOMINANT_DB_NAME
import traceback
from time import sleep
import re
from vnpy.app.cta_strategy.base import TRANSFORM_SYMBOL_LIST
from jqdatasdk import *
from .tushareService import fetchNextTradeDate
from collections import defaultdict

# 聚宽账号登陆
if not is_auth():
    auth('18521705317', '970720699')

# 使用聚宽数据服务，添加新的品种必须这里添加代码
EXCHANGE_SYMBOL_MAP = {'XSGE':['RB', 'HC', 'RU', 'CU', 'PB', 'SN', 'SP', 'WR', 'ZN', 'RU'],
                       'XZCE':['SM', 'ZC', 'TA', 'CF', 'CJ', 'CY', 'OI', 'RM', 'SF', 'SM', 'SR', 'TA', 'ZC', 'FG', 'RI', 'SA'],
                       'XDCE':['J', 'A', 'I', 'CS', 'EG', 'JM'],
                       'XINE':['SC', 'NR'],
                       'CCFX':['TF', 'TS']}

client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=600)
client.server_info()
dbDominant = client[DOMINANT_DB_NAME]
dbDaily = client[DAILY_DB_NAME]
dbMinute = client[MINUTE_DB_NAME]

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

# 获取目标交易日所有正在交易的合约数据
# target_date：'2020-01-01'
# target_dat为空，不指定具体日期，获取所有历史交易过的合约数据
def jq_get_all_trading_symbol_list(target_date:str=''):
    info = get_all_securities(types=['futures'])
    result_list = []
    target_datetime = None
    if target_date:
        target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
    for index, row in info.iterrows():
        start = datetime.strptime(row['start_date'].strftime('%Y-%m-%d'), '%Y-%m-%d')
        end = datetime.strptime(row['end_date'].strftime('%Y-%m-%d'), '%Y-%m-%d')
        data = jqSymbolData()
        data.display_name = row['display_name']
        data.symbol = complete_symbol(row['name'].upper(), replace=str(end.year)[2])
        data.start_date = start
        data.end_date = end
        data.type = row['type']
        if target_datetime:
            if start <= target_datetime <= end:
                result_list.append(data)
        else:
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

def complete_symbol(symbol:str, replace:str):
    symbol = symbol.upper()
    startSymbol = re.sub("\d", "", symbol)
    result = symbol
    if startSymbol in TRANSFORM_SYMBOL_LIST.keys():
        endSymbol = re.sub("\D", "", symbol)
        result = startSymbol + str(replace) + endSymbol
    return result

# 下载Bar数据【包括日线、分钟线】
# symbol：'RB2005'
# start & end：'2020-01-01' '2020-01-01 09:00'
# frequency：'1d'、'1m'
def download_bar_data(symbol:str, start:str, end:str, frequency:str='1d', to_database:bool=False):
    jq_symbol = transform_jqcode(symbol=symbol)

    if not start or not end:
        # 获取合约上市起止时间
        symbol_data_list = jq_get_all_trading_symbol_list()
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # 获取目标代码还在交易的所有合约数据
        for symbol_data in symbol_data_list:
            if symbol_data.symbol == symbol.upper():
                end_date = symbol_data.end_date
                if end_date >= today:
                    end_date = today
                if frequency == '1d':
                    start = symbol_data.start_date.strftime('%Y-%m-%d')
                    end = end_date.strftime('%Y-%m-%d')
                elif frequency == '1m':
                    start = symbol_data.start_date.strftime('%Y-%m-%d %H:%M:%S')
                    end = end_date.strftime('%Y-%m-%d %H:%M:%S')
                break

    data = get_price(jq_symbol, start_date=start, end_date=end, frequency=frequency, fields=None, skip_paused=True, fq='pre')
    bar_list = []
    from_date = None
    to_date = None
    for index, row in data.iterrows():
        datetime_str = index.strftime('%Y-%m-%d %H:%M')
        bar = generateBar(row=row, symbol=symbol.upper(), datetime_str=datetime_str)
        bar_list.append(bar)

        if not from_date:
            from_date = bar.datetime
        if not to_date:
            to_date = bar.datetime

        if bar.datetime <= from_date:
            from_date = bar.datetime
        if bar.datetime >= to_date:
            to_date = bar.datetime

    return_msg = ''
    if len(bar_list):
        if to_database:
            # 保存数据库
            collection = None
            if frequency == '1d':
                collection = dbDaily[symbol.upper()]
            elif frequency == '1m':
                collection = dbMinute[symbol.upper()]
            if collection:
                valid = True
                for bar in bar_list:
                    if bar.check_valid():
                        collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert=True)
                    else:
                        valid = False
                return_msg = f'{symbol.upper()}\t{frequency.upper()} Bar数据下载并保存数据库成功【{len(bar_list)}】\t{from_date} - {to_date}'
                if not valid:
                    return_msg += '\tBar数据校验不通过，需要排查错误！！'
        else:
            return_msg = f'{symbol.upper()}\t{frequency.upper()} Bar数据下载成功【{len(bar_list)}】\t{from_date} - {to_date}'
    else:
        return_msg = f'{symbol.upper()}\t{frequency.upper()} Bar数据下载空!!\t{start} - {end}'
    return bar_list, return_msg

def download_bar_data_symbollist(symbollist:list, start:str, end:str, frequency:str='1d', to_database:bool=False):
    # 获取多只标的Bar数据【注意：起始时间必须指定，不能跳过停牌，单只标的停牌时会自动填充停牌前的数据】
    jq_symbollist = []
    for symbol in symbollist:
        jq_symbol = transform_jqcode(symbol=symbol)
        jq_symbollist.append(jq_symbol)

    #jq_symbollist = ['RB2011.XSGE', 'RB2012.XSGE']
    data = get_price(jq_symbollist, start_date=start, end_date=end, frequency=frequency, fields=None, skip_paused=False, fq='pre')
    bar_dict = defaultdict(list)
    from_date = None
    to_date = None
    open_list = data['open']
    high_list = data['high']
    low_list = data['low']
    close_list = data['close']
    volume_list = data['volume']
    for index, open_row in open_list.iterrows():
        for symbol in open_row.index:
            open = open_row[symbol]
            high = high_list[symbol][index]
            low = low_list[symbol][index]
            close = close_list[symbol][index]
            volume = volume_list[symbol][index]
            bar_row = {'open':open, 'high':high, 'low':low, 'close':close, 'volume':volume}
            datetime_str = index.strftime('%Y-%m-%d %H:%M')
            simple_symbol = symbol.split('.')[0]
            bar = generateBar(row=bar_row, symbol=simple_symbol.upper(), datetime_str=datetime_str)
            the_list = bar_dict[bar.symbol]
            the_list.append(bar)


        if not from_date:
            from_date = bar.datetime
        if not to_date:
            to_date = bar.datetime

        if bar.datetime <= from_date:
            from_date = bar.datetime
        if bar.datetime >= to_date:
            to_date = bar.datetime

    return_msg = ''
    for bar_symbol, symbol_bar_list in bar_dict.items():
        if len(symbol_bar_list):
            if to_database:
                # 保存数据库
                collection = None
                if frequency == '1d':
                    collection = dbDaily[bar_symbol.upper()]
                elif frequency == '1m':
                    collection = dbMinute[bar_symbol.upper()]
                if collection:
                    valid = True
                    for bar in symbol_bar_list:
                        if bar.check_valid():
                            collection.update_many({'datetime': bar.datetime}, {'$set': bar.__dict__}, upsert=True)
                        else:
                            valid = False
                    return_msg = return_msg + f'{bar_symbol.upper()}\t{frequency.upper()} Bar数据下载并保存数据库成功【{len(symbol_bar_list)}】\t{from_date} - {to_date}' + '\n'
                    if not valid:
                        return_msg += return_msg + f'{bar_symbol}\tBar数据校验不通过，需要排查错误！！' + '\n'
            else:
                return_msg = return_msg + f'{bar_symbol.upper()}\t{frequency.upper()} Bar数据下载成功【{len(symbol_bar_list)}】\t{from_date} - {to_date}' + '\n'
        else:
            return_msg = return_msg + f'{bar_symbol.upper()}\t{frequency.upper()} Bar数据下载空!!\t{start} - {end}' + '\n'

    if not return_msg:
        return_msg = f'Bar数据下载空！！\t{start} - {end}' + '\n'
    return bar_dict, return_msg

# 获取合约持仓量数据
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
    from_date = datetime.strptime(from_date.strftime('%Y%m%d'), '%Y%m%d')
    end_date = today_date
    # 下午五点前不进行当天的主力合约判断
    if datetime.now() < datetime.now().replace(hour=17, minute=0, second=0, microsecond=0):
        end_date = today_date - timedelta(days=1)
    target_date = from_date
    while target_date <= end_date:
        try:
            new_dominant, msg = jq_get_and_save_dominant_symbol(underlying_symbol=underlying_symbol, target_date=target_date)
            if new_dominant:
                # 有新的主力产生
                msg = f'{msg}\t新主力'
                # 下载新主力的历史数据
                bar_list, download_msg = download_bar_data(symbol=new_dominant, start='', end='', to_database=True)
                msg = f'{msg}\n{download_msg}'
            print(msg)
            return_msg += msg
            target_date += timedelta(days=1)
        except:
            msg = traceback.format_exc()
            print(msg)
            return_msg += msg
            break

    return return_msg

# 判断主力合约并存入数据库，指定单个日期
def jq_get_and_save_dominant_symbol(underlying_symbol:str, target_date:datetime) -> (str, str):
    # 获取合约列表
    all_trading_symbol_list = jq_get_all_trading_symbol_list(target_date=target_date.strftime('%Y-%m-%d'))

    # 获取目标代码还在交易的所有合约数据
    trading_symbol_list = []
    for symbol_data in all_trading_symbol_list:
        start_symbol = re.sub("\d", "", symbol_data.symbol).upper()
        if underlying_symbol == start_symbol:
            if '主力' in symbol_data.display_name or '指数' in symbol_data.display_name:
                continue
            trading_symbol_list.append(symbol_data.symbol)

    # 数据库获取最新主力合约代码
    collection = dbDominant[underlying_symbol]
    cursor = collection.find().sort('date', direction=DESCENDING)
    last_dominant_symbol = ''
    last_dominant_date = None
    for dic in cursor:
        last_dominant_symbol = dic['symbol']
        last_dominant_date = dic['date']
        break

    if last_dominant_date and last_dominant_date > target_date:
        return ('', f'{last_dominant_symbol} -> {target_date}')

    # 获取所以正在交易合约的持仓数据
    target_date_str = datetime.strftime(target_date, '%Y-%m-%d')
    open_interest_list = download_open_interest(symbol_list=trading_symbol_list, date=target_date_str)
    if not open_interest_list:
        return ('', f'{underlying_symbol}\t{target_date}\t持仓量数据缺失，无法判断主力！')

    # 判断主力合约
    new_dominant_symbol = ''
    if last_dominant_symbol:
        # 找到当前主力持仓量
        last_dominant_open_interest = 0
        for the_data in open_interest_list:
            if the_data.symbol == last_dominant_symbol:
                last_dominant_open_interest = the_data.open_interest
                break
        if not last_dominant_open_interest:
            return ('', f'{underlying_symbol}\t{target_date}\t当前主力持仓量数据缺失，无法判断主力！')
        # 判断新主力
        for the_data in open_interest_list:
            # 若合约持仓量大于当前主力合约持仓量的1.1倍时，新主力产生
            if the_data.open_interest > last_dominant_open_interest * 1.1:
                if new_dominant_symbol:
                    return ('', f'{underlying_symbol}\t{target_date}\t出现不止一个新主力合约，检查代码！！')
                new_dominant_symbol = the_data.symbol

    else:
        # 持仓量最大的为下一交易日主力
        max_open_interest = 0
        for the_data in open_interest_list:
            if the_data.open_interest > max_open_interest:
                max_open_interest = the_data.open_interest
                new_dominant_symbol = the_data.symbol

    if new_dominant_symbol:
        # 产生新主力，保存数据库
        next_trade_date = fetchNextTradeDate(exchange='SHFE', from_date=target_date)
        if next_trade_date:
            dominant_dict = {'date': next_trade_date,
                             'symbol': new_dominant_symbol}
            collection.update_many({'date': next_trade_date}, {'$set': dominant_dict}, upsert=True)
            return (new_dominant_symbol, f'{new_dominant_symbol} -> {target_date}')
        else:
            return ('', f'{underlying_symbol}\t{target_date}\t获取下一个交易日出错，检查代码！！')
    else:
        # 没有新主力
        if last_dominant_symbol:
            return ('', f'{last_dominant_symbol} -> {target_date}')
        else:
            return ('', f'{underlying_symbol}\t{target_date}\t数据库没有存档记录，并且找不到新主力合约，检查代码！！')

if __name__ == '__main__':
    """
    # 下载持仓量
    symbol_list = ['RB2005', 'RB2001']
    download_open_interest(symbol_list=symbol_list, date='2019-12-16')
    """

    """
    # 下载合约Bar数据【日线、分钟线】
    symbol = 'RB2005'
    bar_list, msg = download_bar_data(symbol=symbol, start = '', end='', frequency='1d')
    """

    """
    # 下载指数Bar数据【日线、分钟线】
    symbol = 'RB8888'
    bar_list, msg = download_bar_data(symbol=symbol, start = '2019-12-16', end='2019-12-20', frequency='1d')
    a = 2
    """

