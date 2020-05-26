# encoding: UTF-8

from datetime import datetime
import copy
from constant import Currency

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
    pnlList = []
    returnList = []
    maxBondList = []
    filename = 'setting.csv'
    with open(filename, errors='ignore') as f:
        r = DictReader(f)
        for d in r:
            print('='*60)
            engine = BacktestingEngine()
            #engine.setPeriod(datetime(2015, 9, 1), datetime(2018, 12, 31))
            #engine.tradingStart = datetime(2016, 1, 1)
            engine.setPeriod(datetime(2019, 9, 1), datetime(2020, 12, 31))
            engine.tradingStart = datetime(2020, 1, 1)
            figSavedName = ''
            if figSavedName:
                figSavedName = f'figSaved{DIR_SYMBOL}{figSavedName}'

            engine.initSinglePortfolio(d)
            engine.loadData()
            engine.runBacktesting()
            print('='*60)

            result = engine.showResult(figSavedName)
            pnl = result['totalNetPnl']
            theReturn = result['totalReturn']
            pnlList.append(f'{pnl}{engine.portfolioCurrency}')
            returnList.append(theReturn)
            maxBondStr = f'{engine.portfolio.maxBond[0]} {engine.portfolioCurrency}'
            maxBondRate = 100 * engine.portfolio.maxBond[0] / engine.portfolioValue
            maxBondList.append([maxBondStr, maxBondRate])

            #"""
            resultList = []
            totalPnl = 0
            calculateDic = {}
            for symbol in engine.symbolList:
                tradeList = engine.getTradeData(symbol)
                for trade in tradeList:
                    print('%s\t\t%s %s\t\t%s\t\t%s\t%s@%s' % (trade.dt, trade.symbol, trade.direction.value, trade.offset.value,
                                                              engine.sizeDict[trade.symbol], trade.volume, trade.price))
                    if trade.offset == Offset.CLOSE:
                        print('.'*2)

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
                for signal in signalList:
                    print('.' * 2)
                    print('datetime\t%s' % signal.bar.datetime)
                    print('ATR\t%s' % signal.atrVolatility)
                    print('virtualUnit\t%s' % signal.unit)
                    print('unit\t%s' % engine.portfolio.unitDict[signal.symbol])
                    print('longStop\t%s' % signal.longStop)
                    print('shortStop\t%s' % signal.shortStop)
                    if signal.result:
                        print('entry\t%s' % signal.result.entry)
                    print('lastPnl\t%s' % signal.getLastPnl())
            print('\n\n')

    portfolioPnl = '\n'.join(pnlList)
    portfolioReturn = ''
    for r in returnList:
        portfolioReturn += f'{r} %' + '\n'
    totalReturn = sum(returnList)
    portfolioMaxBondStr = ''
    totalMaxBond = 0
    for maxBond in maxBondList:
        portfolioMaxBondStr += maxBond[0] + '\t' + f'{maxBond[1]} %' + '\n'
        totalMaxBond += maxBond[1]
    print(f'组合盈亏：\n{portfolioPnl}\n')
    print(f'组合收益率：\n{portfolioReturn}')
    print(f'总收益率：\n{totalReturn}\n')
    print(f'组合占用最大保证金：\n{portfolioMaxBondStr}')
    print(f'总占用最大保证金：\n{totalMaxBond} %')
    #"""

def two():
    filename = 'setting.csv'
    count = 0
    resultList = []
    with open(filename) as f:
        r = DictReader(f)
        for d in r:
            engine = BacktestingEngine()
            engine.setPeriod(datetime(2012, 10, 15), datetime(2019, 12, 31))

            engine.initSinglePortfolio(d, 10000000)

            engine.loadData()
            engine.runBacktesting()
            if not len(engine.resultList):
                continue

            timeseries, result = engine.calculateResult()
            print(u'Sharpe Ratio：\t%s' % result['sharpeRatio'])
            count += 1
            print(u'count：\t%s\n' % count)

            temp = d.copy()
            temp['result'] = result['sharpeRatio']
            resultList.append(temp)

    if len(resultList):
        fieldNames = ['symbol', 'size', 'priceTick', 'variableCommission', 'fixedCommission', 'slippage', 'name', 'result']
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
    with open(filename) as f:
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
        engine.setPeriod(datetime(2009, 6, 15), datetime(2019, 12, 31))
        engine.tradingStart = datetime(2010, 1, 1)
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
        for lj in xrange(li, len(l)):
            one[ni] = l[lj]
            next_c(lj + 1, ni + 1)
    next_c()
    return answers

if __name__ == '__main__':
    one()