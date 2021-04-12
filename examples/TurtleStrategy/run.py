# encoding: UTF-8

from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import copy

from turtleEngine import BacktestingEngine
from csv import DictReader
import csv
import os
from collections import OrderedDict
import re
from pymongo import MongoClient, ASCENDING
from vnpy.app.cta_strategy.base import DAILY_DB_NAME
import pandas as pd
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import DIR_SYMBOL

def one():
    engine = BacktestingEngine()
    engine.setPeriod(datetime(2019, 1, 15), datetime(2021, 12, 31))
    engine.tradingStart = datetime(2019, 4, 9)
    figSavedName = ''
    if figSavedName:
        figSavedName = f'figSaved{DIR_SYMBOL}{figSavedName}'

    filename = 'setting.csv'
    symbolList = []
    with open(filename, errors='ignore') as f:
        r = DictReader(f)
        for d in r:
            #"""
            symbol = re.sub("\d", "", d['symbol'])
            is_crypto = d['is_crypto'] == 'true'
            if not is_crypto:
                symbol = symbol + '99'
            d['symbol'] = symbol
            #"""
            symbolList.append(d)
    if not symbolList:
        return
    engine.initListPortfolio(symbolList, 200000)
    engine.loadData()
    engine.runBacktesting()
    engine.showResult(figSavedName)
    print(u"\n最大占用保证金：%s\t持仓单位：%s" % (engine.portfolio.maxBond[0], engine.portfolio.maxBond[1]))

    #"""
    resultList = []
    totalPnl = 0
    calculateDic = {}
    for symbol in engine.symbolList:
        tradeList = engine.getTradeData(symbol)
        for trade in tradeList:
            print('%s\t\t%s %s\t\t%s\t\t%s\t%s@%s' % (trade.dt, trade.symbol, trade.direction.value, trade.offset.value,
                                                      engine.sizeDict[trade.symbol], trade.volume, trade.price))

            tOpen = False
            pnl = 0
            offset = ''
            direction = 0

            symbolDic = calculateDic.get(trade.symbol, {})

            if trade.offset == Offset.OPEN:
                offset = '开仓'
                tOpen = True
            elif trade.offset == Offset.CLOSE:
                offset = '平仓'
                tOpen = False

            if trade.direction == Direction.LONG:
                direction = '多'
                if tOpen:
                    symbolDic['direction'] = 1
            elif trade.direction == Direction.SHORT:
                direction = '空'
                if tOpen:
                    symbolDic['direction'] = -1

            if trade.volume:
                if tOpen:
                    symbolDic['size'] = engine.sizeDict[trade.symbol]
                    vol = symbolDic.get('volume', 0)
                    pri = symbolDic.get('price', 0)
                    pri = vol*pri + trade.volume*trade.price

                    vol += trade.volume
                    symbolDic['volume'] = vol
                    pri = pri / vol
                    symbolDic['price'] = pri
                    calculateDic[trade.symbol] = symbolDic
                else:
                    if symbolDic['volume'] != trade.volume:
                        raise('平仓数量有误！')
                    pnl = symbolDic['direction'] * (trade.price - symbolDic['price']) * trade.volume * symbolDic['size']
                    totalPnl += pnl
                    calculateDic[trade.symbol] = {}

            dic = {'datetime':trade.dt,
                   'symbol':trade.symbol,
                   'direction':direction,
                   'offset':offset,
                   'size':engine.sizeDict[trade.symbol],
                   'volume':trade.volume,
                   'price':trade.price}
            if pnl:
                dic['pnl'] = str(pnl)
                dic['totalPnl'] = str(totalPnl)
            else:
                dic['pnl'] = ''
                dic['totalPnl'] = ''

            resultList.append(dic)
        print('\n\n')
    if len(resultList):
        fieldNames = ['datetime', 'symbol', 'direction', 'offset', 'size', 'volume', 'price', 'pnl', 'totalPnl']
        # 文件路径
        filePath = 'result.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

    folio = engine.portfolio
    signalDic = folio.signalDict
    for s, signalList in signalDic.items():
        print('*' * 6 + s + '*' * 6)
        for signal in signalList:
            print('currentSymbol\t%s' % signal.bar.vt_symbol)
            print('datetime\t%s' % signal.bar.datetime)
            print('ATR\t%s' % signal.atrVolatility)
            print('multiplier\t%s' % engine.portfolio.multiplierDict[signal.symbol])
            print('virtualUnit\t%s' % signal.unit)
            print('unit\t%s' % engine.portfolio.unitDict[signal.symbol])
            print('longStop\t%s' % signal.longStop)
            print('shortStop\t%s' % signal.shortStop)
            if signal.result:
                print('entry\t%s' % signal.result.entry)
            print('lastPnl\t%s' % signal.getLastPnl())
            print('newDominantOpen\t%s' % signal.newDominantOpen)
            print('\n')
    #"""

def two():
    filename = 'setting.csv'
    count = 0
    resultList = []
    with open(filename, errors='ignore') as f:
        r = DictReader(f)
        for d in r:
            engine = BacktestingEngine()
            engine.setPeriod(datetime(2010, 9, 15), datetime(2021, 1, 1))
            engine.tradingStart = datetime(2011, 1, 1)

            engine.initSinglePortfolio(d, 200000)

            engine.loadData()
            engine.runBacktesting()
            if not len(engine.resultList):
                continue

            timeseries, result = engine.calculateResult()
            print(u'Sharpe Ratio：\t%s' % result['sharpeRatio'])
            count += 1
            print(u'count：\t%s\n' % count)

            temp = d.copy()
            temp.pop('is_crypto')
            temp.pop('min_volume')
            temp['sharpeRatio'] = result['sharpeRatio']
            temp['totalReturn'] = result['totalReturn']
            temp['annualizedReturn'] = result['annualizedReturn']
            temp['maxDrawdown'] = result['maxDrawdown']
            temp['maxDdPercent'] = result['maxDdPercent']
            resultList.append(temp)

    if len(resultList):
        fieldNames = ['symbol', 'size', 'priceTick', 'variableCommission', 'fixedCommission', 'slippage', 'name', 'sharpeRatio', 'totalReturn', 'annualizedReturn', 'maxDrawdown', 'maxDdPercent']
        # 文件路径
        filePath = 'result.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

def three():
    resultDic = OrderedDict()
    dirPath = 'resultList'
    for root, subdirs, files in os.walk(dirPath):
        for theFile in files:
            filePath = f'{root}{DIR_SYMBOL}{theFile}'
            with open(filePath) as f:
                r = DictReader(f)
                for d in r:
                    symbol = d['symbol']
                    if not symbol in resultDic:
                        resultDic[symbol] = d
                    else:
                        hisResult = resultDic[symbol]
                        hisResult['result'] = str(float(hisResult['result']) + float(d['result']))

    resultList = resultDic.values()
    if len(resultList):
        fieldNames = ['symbol', 'size', 'priceTick', 'variableCommission', 'fixedCommission', 'slippage', 'name', 'result']
        # 文件路径
        filePath = f'resultList{DIR_SYMBOL}result_all.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

def four():
    filename = 'setting.csv'
    symbolList = []
    with open(filename, errors='ignore') as f:
        r = DictReader(f)
        for d in r:
            symbolList.append(d)
    if not symbolList:
        return

    combineList = combine(symbolList, 6)
    count = 0
    resultList = []
    for l in combineList:
        engine = BacktestingEngine()
        engine.setPeriod(datetime(2010, 9, 15), datetime(2021, 1, 1))
        engine.tradingStart = datetime(2011, 1, 1)
        engine.initListPortfolio(l, 200000)

        engine.loadData()
        engine.runBacktesting()
        if not len(engine.resultList):
            continue

        timeseries, result = engine.calculateResult()
        dic = {'symbolList':engine.symbolList,
               'sharpe':result['sharpeRatio'],
               'totalPnl':result['totalReturn'],
               'annualizedPnl':result['annualizedReturn']}
        resultList.append(dic)

        count += 1
        print(u'count：\t%s\n' % count)

    if len(resultList):
        fieldNames = ['symbolList', 'sharpe', 'totalPnl', 'annualizedPnl']
        # 文件路径
        filePath = 'result.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

    print('='*20)
    print('组合数：%s' % count)

# 年度成交量排名
def volumeSorted():
    startDt = datetime(2010, 1, 1)
    endDt = datetime(2010, 12, 31)
    underlyingList = ['RB', 'CU', 'NI', 'ZN', 'RU', 'AL', 'HC', 'J', 'I', 'PP', 'AP', 'TA', 'A', 'AG', 'AU', 'B', 'BB', 'BU', 'C', 'CF', 'CS', 'CY', 'EG', 'FB', 'FG', 'FU', 'JD', 'JM', 'JR', 'L', 'LR', 'M', 'MA', 'OI', 'P', 'PB', 'PM', 'RI', 'RM', 'RS', 'SC', 'SF', 'SM', 'SN', 'SP', 'SR', 'V', 'WH', 'WR', 'Y', 'ZC', 'IF', 'IC', 'IH']

    volumeDic = {}
    # 数据库
    mc = MongoClient()
    db = mc[DAILY_DB_NAME]
    for underlyingSymbol in underlyingList:
        totalVolume = 0
        symbol = underlyingSymbol + '99'
        cl = db[symbol]
        cl.ensure_index([('datetime', ASCENDING)], unique=True)
        flt = {'datetime': {'$gte': startDt,
                            '$lte': endDt}}

        cursor = cl.find(flt).sort('datetime')
        for d in cursor:
            totalVolume += d['volume']
        volumeDic[underlyingSymbol] = totalVolume
    resultDic = {'volume':volumeDic}
    df = pd.DataFrame(resultDic).sort_values('volume', ascending=False)
    print(df.head(10))

# 随机组合，l是数组，n是组合的元素数量
def combine(l, n):
    answers = []
    one = [0] * n
    def next_c(li = 0, ni = 0):
        if ni == n:
            answers.append(copy.copy(one))
            return
        for lj in range(li, len(l)):
            one[ni] = l[lj]
            next_c(lj + 1, ni + 1)
    next_c()
    return answers

if __name__ == '__main__':
    one()