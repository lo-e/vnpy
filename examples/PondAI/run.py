# encoding: UTF-8

from datetime import datetime
import copy
from constant import Currency

from engine import BacktestingEngine
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
import numpy as np

def backtesting():
    pnlList = []
    returnList = []
    filename = 'setting.csv'
    with open(filename, errors='ignore') as f:
        r = DictReader(f)
        for d in r:
            print('='*60)
            engine = BacktestingEngine()
            engine.setPeriod(datetime(2021, 9, 1), datetime(2022, 12, 31))
            engine.tradingStart = datetime(2022, 1, 1)
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

            #"""
            resultList = []
            totalPnl = 0
            calculateDic = {}
            for symbol in engine.symbolList:
                tradeList = engine.getTradeData(symbol)

                symbol_size = engine.sizeDict[symbol]
                open_price_list = []
                open_volumn_list = []
                open_direction = None
                for trade in tradeList:
                    print('%s\t\t%s %s\t\t%s\t\t%s\t%s@%s' % (trade.dt, trade.symbol, trade.direction.value, trade.offset.value,
                                                              engine.sizeDict[trade.symbol], trade.volume, trade.price))

                    if trade.offset == Offset.OPEN:
                        if not open_direction:
                            open_direction = trade.direction
                        elif open_direction != trade.direction:
                            raise ('成交数据异常！！检查代码')

                        open_price_list.append(trade.price)
                        open_volumn_list.append(trade.volume)
                    else:
                        if trade.volume != np.array(open_volumn_list).sum():
                            raise ('成交数据异常！！检查代码')

                        pnl_list = []
                        mean_pnl = 0
                        all_fund = 0
                        if open_direction == Direction.LONG:
                            for i, open_price in enumerate(open_price_list):
                                open_volumn = open_volumn_list[i]
                                all_fund += open_price * open_volumn

                                pnl = (1.0/open_price - 1.0/trade.price) * open_volumn * symbol_size
                                pnl_list.append(pnl)

                            mean_open = all_fund / trade.volume
                            mean_pnl = (1.0/mean_open - 1.0/trade.price) * trade.volume * symbol_size
                        else:
                            for i, open_price in enumerate(open_price_list):
                                open_volumn = open_volumn_list[i]
                                all_fund += open_price * open_volumn

                                pnl = (1.0 / trade.price - 1.0 / open_price) * open_volumn * symbol_size
                                pnl_list.append(pnl)

                            mean_open = all_fund / trade.volume
                            mean_pnl = (1.0 / trade.price - 1.0 / mean_open) * trade.volume * symbol_size

                        pnl_str = ''
                        for p in pnl_list:
                            pnl_str += str(p) + ' '
                        print(f'收益：{pnl_str}')
                        print(f'每笔总：{np.array(pnl_list).sum()}')
                        print(f'平均总：{mean_pnl}')
                        open_price_list = []
                        open_volumn_list = []
                        open_direction = None

                    if trade.offset == Offset.CLOSE:
                        print('\n')

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
    print(f'组合盈亏：\n{portfolioPnl}\n')
    print(f'组合收益率：\n{portfolioReturn}')
    print(f'总收益率：\n{totalReturn}\n')
    #"""

if __name__ == '__main__':
    backtesting()