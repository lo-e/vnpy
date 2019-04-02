# encoding: UTF-8

from __future__ import print_function
import sys
import json
from datetime import datetime
from time import time, sleep

from pymongo import MongoClient, ASCENDING
import pandas as pd

from vnpy.trader.vtObject import VtBarData, VtTickData
from vnpy.trader.app.ctaStrategy.ctaBase import (MINUTE_DB_NAME, 
                                                 DAILY_DB_NAME, 
                                                 TICK_DB_NAME)

import rqdatac as rq
from rqdatac import *

# 加载配置
config = open('config.json')
setting = json.load(config)

mc = MongoClient()                  # Mongo连接
dbMinute = mc[MINUTE_DB_NAME]       # 数据库
dbDaily = mc[DAILY_DB_NAME]
dbTick = mc[TICK_DB_NAME]

USERNAME = setting['rqUsername']
PASSWORD = setting['rqPassword']
try:
    rq.init(USERNAME, PASSWORD)
except:
    rq.init()

FIELDS = ['open', 'high', 'low', 'close', 'volume']

DOMINANT_DB_NAME = 'Dominant_db'

#----------------------------------------------------------------------
def generateVtBar(row, symbol):
    """生成K线"""
    bar = VtBarData()
    
    bar.symbol = symbol
    bar.vtSymbol = symbol
    bar.open = row['open']
    bar.high = row['high']
    bar.low = row['low']
    bar.close = row['close']
    bar.volume = row['volume']
    bar.datetime = row.name
    bar.date = bar.datetime.strftime("%Y%m%d")
    bar.time = bar.datetime.strftime("%H:%M:%S")
    
    return bar

#----------------------------------------------------------------------
def generateVtTick(row, symbol):
    """生成K线"""
    tick = VtTickData()
    tick.symbol = symbol
    tick.vtSymbol = symbol
    
    tick.lastPrice = row['last']
    tick.volume = row['volume']
    tick.openInterest = row['open_interest']
    tick.datetime = row.name
    tick.openPrice = row['open']
    tick.highPrice = row['high']
    tick.lowPrice = row['low']
    tick.preClosePrice = row['prev_close']
    tick.upperLimit = row['limit_up']
    tick.lowerLimit = row['limit_down']
    
    tick.bidPrice1 = row['b1']
    tick.bidPrice2 = row['b2']
    tick.bidPrice3 = row['b3']
    tick.bidPrice4 = row['b4']
    tick.bidPrice5 = row['b5']
    
    tick.bidVolume1 = row['b1_v']
    tick.bidVolume2 = row['b2_v']
    tick.bidVolume3 = row['b3_v']
    tick.bidVolume4 = row['b4_v']
    tick.bidVolume5 = row['b5_v']    
    
    tick.askPrice1 = row['a1']
    tick.askPrice2 = row['a2']
    tick.askPrice3 = row['a3']
    tick.askPrice4 = row['a4']
    tick.askPrice5 = row['a5']
    
    tick.askVolume1 = row['a1_v']
    tick.askVolume2 = row['a2_v']
    tick.askVolume3 = row['a3_v']
    tick.askVolume4 = row['a4_v']
    tick.askVolume5 = row['a5_v']        
    
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
        d = tick.__dict__
        flt = {'datetime': tick.datetime}
        cl.replace_one(flt, d, True)            

    end = time()
    cost = (end - start) * 1000

    print(u'合约%sTick数据下载完成%s - %s，耗时%s毫秒' %(symbol, df.index[0], df.index[-1], cost))

""" modify by loe """
# 获取主力合约列表并保存到数据库
def dominantSymbolToDatabase(underlyingSymbol, startDate, endDate, toDatabase):
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

# 下载主力真实合约数据到数据库
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
    dominantList = get_dominant_future(underlyingSymbol, start_date=datetime.now(), end_date=datetime.now(), rule=0)
    if len(dominantList):
        return dominantList[-1]

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