# encoding: UTF-8

from __future__ import print_function
import sys
import json
from datetime import datetime
from time import time, sleep

from pymongo import MongoClient, ASCENDING
import pandas as pd

from vnpy.trader.object import BarData, TickData
from vnpy.app.cta_strategy.base import (MINUTE_DB_NAME,
                                        DAILY_DB_NAME,
                                        TICK_DB_NAME)
from vnpy.trader.constant import Exchange

import rqdatac as rq
from rqdatac import *
rq.init()

mc = MongoClient()                  # Mongo连接
dbMinute = mc[MINUTE_DB_NAME]       # 数据库
dbDaily = mc[DAILY_DB_NAME]
dbTick = mc[TICK_DB_NAME]

#====== vnpy_v1.9.2_LTS ======
#rq.init(USERNAME, PASSWORD, ('rqdatad-pro.ricequant.com', 16011))

FIELDS = ['open', 'high', 'low', 'close', 'volume']

DOMINANT_DB_NAME = 'Dominant_db'

#----------------------------------------------------------------------
def generateVtBar(row, symbol):
    """生成K线"""
    gateway_name = 'RQ'
    exchange = Exchange.RQ
    datetime = row.name
    endDatetime = None

    bar = BarData(gateway_name = gateway_name, symbol = symbol, exchange = exchange, datetime = datetime, endDatetime = endDatetime)
    bar.exchange = bar.exchange.value
    bar.open_price = row['open']
    bar.high_price = row['high']
    bar.low_price = row['low']
    bar.close_price = row['close']
    bar.volume = row['volume']
    
    return bar

#----------------------------------------------------------------------
def generateVtTick(row, symbol):
    """生成K线"""
    gateway_name = 'RQ'
    exchange = Exchange.RQ
    datetime = row.name

    tick = TickData(gateway_name = gateway_name, symbol = symbol, exchange = exchange, datetime = datetime)
    
    tick.last_price = row['last']
    tick.volume = row['volume']
    tick.open_interest = row['open_interest']
    tick.open_price = row['open']
    tick.high_price = row['high']
    tick.low_price = row['low']
    tick.pre_close = row['prev_close']
    tick.limit_up = row['limit_up']
    tick.limit_down = row['limit_down']
    
    tick.bid_price_1 = row['b1']
    tick.bid_price_2 = row['b2']
    tick.bid_price_3 = row['b3']
    tick.bid_price_4 = row['b4']
    tick.bid_price_5 = row['b5']
    
    tick.bid_volume_1 = row['b1_v']
    tick.bid_volume_2 = row['b2_v']
    tick.bid_volume_3 = row['b3_v']
    tick.bid_volume_4 = row['b4_v']
    tick.bid_volume_5 = row['b5_v']
    
    tick.ask_price_1 = row['a1']
    tick.ask_price_2 = row['a2']
    tick.ask_price_3 = row['a3']
    tick.ask_price_4 = row['a4']
    tick.ask_price_5 = row['a5']
    
    tick.ask_volume_1 = row['a1_v']
    tick.ask_volume_2 = row['a2_v']
    tick.ask_volume_3 = row['a3_v']
    tick.ask_volume_4 = row['a4_v']
    tick.ask_volume_5 = row['a5_v']
    
    return tick

#----------------------------------------------------------------------
def downloadMinuteBarBySymbol(symbol, min=1):
    """下载某一合约的分钟线数据"""
    start = time()

    """ modify by loe """
    dbMinute = mc[MINUTE_DB_NAME.replace('1', str(min))]
    cl = dbMinute[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引

    df = rq.get_price(symbol, frequency=str(min) + 'm', fields=FIELDS, end_date=datetime.now().strftime('%Y%m%d'))

    """ modify by loe """
    barList = ['', '']
    count = 0
    for ix, row in df.iterrows():
        bar = generateVtBar(row, symbol)
        barList[:-1] = barList[1:]
        barList[-1] = bar
        count += 1
        if count >= 2:
            the = barList[0]
            the.endDatetime = bar.datetime
            d = the.__dict__
            flt = {'datetime': the.datetime}
            cl.replace_one(flt, d, True)

    end = time()
    cost = (end - start) * 1000

    print(u'合约%s的分钟K线数据下载完成%s - %s，耗时%s毫秒' %(symbol, df.index[0], df.index[-1], cost))

#----------------------------------------------------------------------
def downloadDailyBarBySymbol(symbol):
    """下载某一合约日线数据"""
    start = time()

    cl = dbDaily[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    df = rq.get_price(symbol, frequency='1d', fields=FIELDS, start_date='20000104', end_date=datetime.now().strftime('%Y%m%d'))

    for ix, row in df.iterrows():
        bar = generateVtBar(row, symbol)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)

    end = time()
    cost = (end - start) * 1000

    print(u'合约%s的日K线数据下载完成%s - %s，耗时%s毫秒' %(symbol, df.index[0], df.index[-1], cost))

#----------------------------------------------------------------------
def downloadTickBySymbol(symbol, date):
    """下载某一合约日线数据"""
    start = time()

    cl = dbTick[symbol]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)         # 添加索引
    
    df = rq.get_price(symbol,
                      frequency='tick', 
                      start_date=date, 
                      end_date=date)
    
    for ix, row in df.iterrows():
        tick = generateVtTick(row, symbol)
        tick.exchange = tick.exchange.value
        d = tick.__dict__
        flt = {'datetime': tick.datetime}
        cl.replace_one(flt, d, True)

    end = time()
    cost = (end - start) * 1000

    print(u'合约%sTick数据下载完成%s - %s，耗时%s毫秒' %(symbol, df.index[0], df.index[-1], cost))

""" modify by loe """
# 获取主力合约列表并保存到数据库
def dominantSymbolToDatabase(underlyingSymbol, toDatabase, startDate=None, endDate=None):
        if toDatabase:
            # 获取数据库
            client = MongoClient('localhost', 27017)
            db = client[DOMINANT_DB_NAME]
            collection = db[underlyingSymbol]

        dominantList = get_dominant_future(underlyingSymbol, start_date=startDate, end_date=endDate, rule=0)
        if toDatabase:
            i = 0
            symbol = ''
            #print('*'*26 + underlyingSymbol + '*'*26)
            while i < len(dominantList):
                index = dominantList.index[i]
                rqSymbol = dominantList[index]
                if not symbol or symbol != rqSymbol:
                    symbol = rqSymbol
                    #print('%s\t%s\n' % (index, rqSymbol))

                    date = index.to_pydatetime()
                    data = {'date':date,
                            'symbol':rqSymbol}
                    collection.update_many({'date': index}, {'$set': data}, upsert=True)
                i += 1

        return dominantList

# 下载主力真实合约bar数据到数据库
def downloadDominantSymbol(underlyingSymbol, startDate = None):
    # 查询数据库
    client = MongoClient('localhost', 27017)
    db = client[DOMINANT_DB_NAME]
    collection = db[underlyingSymbol]

    if startDate:
        flt = {'date': {'$gte': startDate}}
        cursor = collection.find(flt).sort('date')
    else:
        cursor = collection.find().sort('date')
    for dic in cursor:
        date = dic['date']
        symbol = dic['symbol']
        print(date)
        print(symbol)
        downloadDailyBarBySymbol(symbol)
        print('\n')

# 获取今日主力合约
def showDominantSymbol(underlyingSymbol):
    dominantList = get_dominant_future(underlyingSymbol, rule=0)
    if len(dominantList):
        index = dominantList.index[len(dominantList)-1]
        rqSymbol = dominantList[index]
        return index, rqSymbol
    else:
        return None


# 获取一年内的主力合约
def showYearDominantSymbol(underlyingSymbol):
    nowYear = datetime.now().year
    startDate = datetime.now().replace(year=nowYear-1)
    dominantList = get_dominant_future(underlyingSymbol, start_date=startDate, end_date=datetime.now(), rule=0)

    result = []
    i = 0
    symbol = ''
    while i < len(dominantList):
        index = dominantList.index[i]
        theSymbol = dominantList[index]
        if symbol != theSymbol:
            symbol = theSymbol
            result.append([index.to_pydatetime(), theSymbol])
        i += 1
    return result