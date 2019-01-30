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

def one():
    engine = BacktestingEngine()
    engine.setPeriod(datetime(2017, 10, 15), datetime(2018, 12, 31))
    engine.initPortfolio('setting.csv', 10000000)

    engine.loadData()
    engine.runBacktesting()
    engine.showResult()

    #"""
    for symbol in engine.vtSymbolList:
        tradeList = engine.getTradeData(symbol)
        for trade in tradeList:
            print '%s\t\t%s %s\t\t%s\t\t%s@%s' % (trade.dt, trade.vtSymbol, trade.direction, trade.offset,
                                                  trade.volume, trade.price)
        print '\n\n'
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
            print u'Sharpe Ratio：\t%s' % result['sharpeRatio']
            count += 1
            print u'count：\t%s\n' % count

            temp = d.copy()
            temp['result'] = result['sharpeRatio']
            resultList.append(temp)

    if len(resultList):
        fieldNames = ['vtSymbol', 'size', 'priceTick', 'variableCommission', 'fixedCommission', 'slippage', 'name', 'result']
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
            filePath = '%s\\%s' % (root, theFile)
            with open(filePath) as f:
                r = DictReader(f)
                for d in r:
                    symbol = d['vtSymbol']
                    if not symbol in resultDic:
                        resultDic[symbol] = d
                    else:
                        hisResult = resultDic[symbol]
                        hisResult['result'] = str(float(hisResult['result']) + float(d['result']))

    resultList = resultDic.values()
    if len(resultList):
        fieldNames = ['vtSymbol', 'size', 'priceTick', 'variableCommission', 'fixedCommission', 'slippage', 'name', 'result']
        # 文件路径
        filePath = 'resultList\\result_all.csv'
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
        engine.setPeriod(datetime(2012, 10, 15), datetime(2018, 12, 31))
        engine.initListPortfolio(l, 10000000)

        engine.loadData()
        engine.runBacktesting()
        if not len(engine.resultList):
            continue

        timeseries, result = engine.calculateResult()
        dic = {'symbolList':engine.vtSymbolList,
               'sharpe':result['sharpeRatio'],
               'totalPnl':result['totalReturn'],
               'annualizedPnl':result['annualizedReturn']}
        resultList.append(dic)

        count += 1
        print u'count：\t%s\n' % count

    if len(resultList):
        fieldNames = ['symbolList', 'sharpe', 'totalPnl', 'annualizedPnl']
        # 文件路径
        filePath = 'result.csv'
        with open(filePath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            # 写入csv文件
            writer.writerows(resultList)

    print '='*20
    print '组合数：%s' % count

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