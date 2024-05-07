# encoding: UTF-8

from __future__ import print_function
from csv import DictReader
from datetime import datetime
from collections import OrderedDict, defaultdict
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
from vnpy.trader.object import BarData
from vnpy.trader.constant import Direction, Exchange
from pondStrategy import PondPortfolio
from vnpy.app.cta_strategy.base import DAILY_DB_NAME, MINUTE_DB_NAME, HOUR_DB_NAME, MinuteDataBaseName, HourDataBaseName
import pandas as pd
from time import sleep, time
import threading
from vnpy.trader.utility import ceil_to

PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}

class BacktestingEngine(object):
    """回测引擎"""
    def __init__(self):
        self.portfolio = None
        
        # 合约配置信息
        self.symbolList = []
        self.min_volume_dict = {}           # 最低交易数量【商品为整数、数字货币带小数】
        self.priceTickDict = {}             # 最小价格变动字典
        self.variableCommissionDict = {}    # 变动手续费字典
        self.slippageDict = {}              # 滑点成本字典
        
        self.portfolioValue = 0
        self.startDt = None
        self.endDt = None
        self.currentDt = None
        
        self.dataDict = OrderedDict()
        self.tradeDict = OrderedDict()
        
        self.result = None
        self.resultList = []

        self.symbol_signal_dict = {}
        self.load_data_threads = []
    
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt
    
    def initPortfolio(self, filename, portfolioValue=10000000):
        """初始化投资组合"""
        self.portfolioValue = portfolioValue
        
        with open(filename) as f:
            r = DictReader(f)
            for d in r:
                self.symbolList.append(d['symbol'])
                self.priceTickDict[d['symbol']] = float(d['priceTick'])
                self.min_volume_dict[d['symbol']] = float(d['min_volume'])
                PRICETICK_DICT[d['symbol']] = float(d['priceTick'])
                VARIABLE_COMMISSION_DICT[d['symbol']] = float(d['variableCommission'])
                SLIPPAGE_DICT[d['symbol']] = float(d['slippage'])
            
        self.portfolio = PondPortfolio(self)
        self.portfolio.init(portfolioValue, self.symbolList)
        
        self.output(u'投资组合的合约代码%s' %(self.symbolList))
        self.output(u'投资组合的初始价值%s' %(portfolioValue))

    def initSinglePortfolio(self, d, portfolioValue=10000000):
        """初始化投资组合"""
        self.portfolioValue = portfolioValue
        self.symbolList.append(d['symbol'])

        self.priceTickDict[d['symbol']] = float(d['priceTick'])
        self.min_volume_dict[d['symbol']] = float(d['min_volume'])
        PRICETICK_DICT[d['symbol']] = float(d['priceTick'])
        VARIABLE_COMMISSION_DICT[d['symbol']] = float(d['variableCommission'])
        SLIPPAGE_DICT[d['symbol']] = float(d['slippage'])

        self.portfolio = PondPortfolio(self)
        self.portfolio.init(portfolioValue, self.symbolList)

        self.output(u'投资组合的合约代码%s' % (self.symbolList))
        self.output(u'投资组合的初始价值%s' % (portfolioValue))

    def initListPortfolio(self, l, portfolioValue=10000000):
        """初始化投资组合"""
        self.portfolioValue = portfolioValue

        for d in l:
            self.symbolList.append(d['symbol'])
            self.priceTickDict[d['symbol']] = float(d['priceTick'])
            self.min_volume_dict[d['symbol']] = float(d['min_volume'])
            PRICETICK_DICT[d['symbol']] = float(d['priceTick'])
            VARIABLE_COMMISSION_DICT[d['symbol']] = float(d['variableCommission'])
            SLIPPAGE_DICT[d['symbol']] = float(d['slippage'])

        self.portfolio = PondPortfolio(self)
        self.portfolio.init(portfolioValue, self.symbolList)

        self.output(f"投资组合的合约代码（总数：{len(self.symbolList)}）\n{self.symbolList[:10]}")
        self.output(f"投资组合的初始价值：{portfolioValue}")
    
    def loadData(self):
        """加载数据"""
        mc = MongoClient()
        db = mc[HOUR_DB_NAME]
        dataDict = {}
        index = 0
        for symbol in self.symbolList:
            index += 1
            flt = {'datetime':{'$gte':self.startDt,
                               '$lte':self.endDt}} 
            
            collection = db[symbol]
            cursor = collection.find(flt).sort('datetime')
            
            for d in cursor:
                exchange = Exchange.RQ
                bar = BarData(gateway_name = '', symbol = '', exchange = exchange, datetime = None, endDatetime = None)
                bar.__dict__ = d
                
                barDict = dataDict.setdefault(bar.datetime, OrderedDict())
                barDict[bar.symbol] = bar
            
            self.output(f"{datetime.now()}({index}/{len(self.symbolList)})\t{symbol}数据加载完成，总数据量：{cursor.retrieved}")

        dateList = sorted(dataDict.keys())
        for theDatetime in dateList:
            self.dataDict[theDatetime] = dataDict[theDatetime]
        
        self.output(u'全部数据加载完成')
    
    def runBacktesting(self):
        """运行回测"""
        self.output(f"{datetime.now()}\t开始回放K线数据")
        
        backtesting_start = time()
        log_time_gap = 60
        log_time = 0
        for dt, barDict in self.dataDict.items():
            time_cost = int(time() - backtesting_start)
            if time_cost > log_time:
                log_time += log_time_gap
                print(f"K线数据回放：{dt}")
            self.currentDt = dt

            previousResult = self.result
            
            self.result = DailyResult(dt)
            self.result.updatePos(self.portfolio.posDict)
            self.resultList.append(self.result)
            
            if previousResult:
                self.result.updatePreviousClose(previousResult.closeDict)

            for bar in barDict.values():
                self.portfolio.onBar(bar)
                self.result.updateBar(bar)
        
        self.output(f"{datetime.now()}\tK线数据回放结束")
    
    def calculateResult(self, annualDays=240):
        """计算结果"""
        self.output(u'开始统计回测结果')
        
        for result in self.resultList:
            result.calculatePnl()
        
        resultList = self.resultList
        dateList = [result.date for result in resultList]

        startDate = dateList[0]
        endDate = dateList[-1]
        totalDays = len(dateList)
        
        profitDays = 0
        lossDays = 0
        endBalance = self.portfolioValue
        highlevel = self.portfolioValue
        totalNetPnl = 0
        totalCommission = 0
        totalSlippage = 0
        totalTradeCount = 0
        
        netPnlList = []
        balanceList = []
        highlevelList = []
        drawdownList = []
        ddPercentList = []
        drawdownOriginList = []
        ddOriginPercentList = []
        dateList = []
        returnList = []
        
        for result in resultList:
            if result.netPnl > 0:
                profitDays += 1
            elif result.netPnl < 0:
                lossDays += 1
            netPnlList.append(result.netPnl)
            
            prevBalance = endBalance
            endBalance += result.netPnl
            balanceList.append(endBalance)
            returnList.append(endBalance/prevBalance - 1)
            
            highlevel = max(highlevel, endBalance)
            highlevelList.append(highlevel)

            dateList.append(result.date)

            drawdown = endBalance - highlevel
            drawdownList.append(drawdown)
            ddPercentList.append(drawdown/highlevel*100)

            drawdownOrigin = endBalance - self.portfolioValue
            drawdownOriginList.append(drawdownOrigin)
            ddOriginPercentList.append(drawdownOrigin / self.portfolioValue * 100)

            totalCommission += result.commission
            totalSlippage += result.slippage
            totalTradeCount += result.tradeCount
            totalNetPnl += result.netPnl

        drawdownSeries = pd.Series(drawdownList, index=dateList)
        maxDrawdown = drawdownSeries.min()
        maxDrawdownDate = drawdownSeries.idxmin()

        ddPercentSeries = pd.Series(ddPercentList, index=dateList)
        maxDdPercent = ddPercentSeries.min()
        maxDdPercentDate = ddPercentSeries.idxmin()

        drawdownOriginSeries = pd.Series(drawdownOriginList, index=dateList)
        maxDrawdownOrigin = drawdownOriginSeries.min()
        maxDrawdownOriginDate = drawdownOriginSeries.idxmin()

        ddOriginPercentSeries = pd.Series(ddOriginPercentList, index=dateList)
        maxDdPercentOrigin = ddOriginPercentSeries.min()
        maxDdPercentOriginDate = ddOriginPercentSeries.idxmin()

        totalReturn = (endBalance / self.portfolioValue - 1) * 100
        dailyReturn = np.mean(returnList) * 100
        annualizedReturn = dailyReturn * annualDays
        returnStd = np.std(returnList) * 100
        
        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(annualDays)
        else:
            sharpeRatio = 0
        
        # 返回结果
        result = {
            'startDate': startDate,
            'endDate': endDate,
            'totalDays': totalDays,
            'profitDays': profitDays,
            'lossDays': lossDays,
            'endBalance': endBalance,
            'maxDrawdown': maxDrawdown,
            'maxDrawdownDate': maxDrawdownDate,
            'maxDdPercent': maxDdPercent,
            'maxDdPercentDate': maxDdPercentDate,
            'maxDrawdownOrigin': maxDrawdownOrigin,
            'maxDrawdownOriginDate': maxDrawdownOriginDate,
            'maxDdPercentOrigin': maxDdPercentOrigin,
            'maxDdPercentOriginDate': maxDdPercentOriginDate,
            'totalNetPnl': totalNetPnl,
            'dailyNetPnl': totalNetPnl/totalDays,
            'totalCommission': totalCommission,
            'dailyCommission': totalCommission/totalDays,
            'totalSlippage': totalSlippage,
            'dailySlippage': totalSlippage/totalDays,
            'totalTradeCount': totalTradeCount,
            'dailyTradeCount': totalTradeCount*1.0/totalDays,
            'totalReturn': totalReturn,
            'annualizedReturn': annualizedReturn,
            'dailyReturn': dailyReturn,
            'returnStd': returnStd,
            'sharpeRatio': sharpeRatio
            }
        
        timeseries = {
            'balance': balanceList,
            'return': returnList,
            'highLevel': highlevel,
            'drawdown': drawdownList,
            'ddPercent': ddPercentList,
            'date': dateList,
            'netPnl': netPnlList
        }
        
        return timeseries, result
    
    def showResult(self, figSavedPath=''):
        """显示回测结果"""
        timeseries, result = self.calculateResult()
        
        # 输出统计结果
        self.output('-' * 30)
        self.output(u'首个交易日：\t%s' % result['startDate'])
        self.output(u'最后交易日：\t%s' % result['endDate'])
        
        self.output(u'总交易日：\t%s' % result['totalDays'])
        self.output(u'盈利交易日\t%s' % result['profitDays'])
        self.output(u'亏损交易日：\t%s' % result['lossDays'])
        
        self.output(u'起始资金：\t%s' % self.portfolioValue)
        self.output(u'结束资金：\t%s' % formatNumber(result['endBalance']))
    
        self.output(u'总收益率：\t%s%%' % formatNumber(result['totalReturn']))
        self.output(u'年化收益：\t%s%%' % formatNumber(result['annualizedReturn']))
        self.output(u'总盈亏：\t%s' % formatNumber(result['totalNetPnl']))
        self.output(u'最大回撤: \t%s\t%s' % (formatNumber(result['maxDrawdown']), result['maxDrawdownDate']))
        self.output(u'百分比最大回撤: %s%%\t%s' % (formatNumber(result['maxDdPercent']), result['maxDdPercentDate']))
        # self.output(u'最大回撤【本金】: \t%s\t%s' % (formatNumber(result['maxDrawdownOrigin']), result['maxDrawdownOriginDate']))
        # self.output(u'百分比最大回撤【本金】: %s%%\t%s' % (formatNumber(result['maxDdPercentOrigin']), result['maxDdPercentOriginDate']))
        
        self.output(u'总手续费：\t%s' % formatNumber(result['totalCommission']))
        self.output(u'总滑点：\t%s' % formatNumber(result['totalSlippage']))
        self.output(u'总成交笔数：\t%s' % formatNumber(result['totalTradeCount']))
        
        self.output(u'日均盈亏：\t%s' % formatNumber(result['dailyNetPnl']))
        self.output(u'日均手续费：\t%s' % formatNumber(result['dailyCommission']))
        self.output(u'日均滑点：\t%s' % formatNumber(result['dailySlippage']))
        self.output(u'日均成交笔数：\t%s' % formatNumber(result['dailyTradeCount']))
        
        self.output(u'日均收益率：\t%s%%' % formatNumber(result['dailyReturn']))
        self.output(u'收益标准差：\t%s%%' % formatNumber(result['returnStd']))
        self.output(u'Sharpe Ratio：\t%s' % formatNumber(result['sharpeRatio']))
        
        # 绘图
        fig = plt.figure(figsize=(10, 16))
        
        pBalance = plt.subplot(4, 1, 1)
        pBalance.set_title('Balance')
        plt.plot(timeseries['date'], timeseries['balance'])
        
        pDrawdown = plt.subplot(4, 1, 2)
        pDrawdown.set_title('Drawdown')
        pDrawdown.fill_between(range(len(timeseries['drawdown'])), timeseries['drawdown'])
        
        pPnl = plt.subplot(4, 1, 3)
        pPnl.set_title('Daily Pnl') 
        plt.bar(range(len(timeseries['drawdown'])), timeseries['netPnl'])

        pKDE = plt.subplot(4, 1, 4)
        pKDE.set_title('Daily Pnl Distribution')
        plt.hist(timeseries['netPnl'], bins=50)

        pBalance.set_position([0.125, 0.75, 0.775, 0.15])  # Adjust the position of the Balance plot
        pDrawdown.set_position([0.125, 0.50, 0.775, 0.15])  # Adjust the position of the Drawdown plot
        pPnl.set_position([0.125, 0.25, 0.775, 0.15])      # Adjust the position of the Daily Pnl plot
        pKDE.set_position([0.125, 0.05, 0.775, 0.15]) 

        if figSavedPath:
            plt.savefig(figSavedPath)
        
        plt.show()        
    
    def sendOrder(self, symbol, direction, offset, price, volume):
        """记录交易数据（由portfolio调用）"""
        # 记录成交数据
        trade = TradeData(symbol, self.currentDt, direction, offset, price, volume)
        l = self.tradeDict.setdefault(self.currentDt, [])        
        l.append(trade)
        
        self.result.updateTrade(trade)

    def output(self, content):
        """输出信息"""
        print(content)
    
    def getTradeData(self, symbol=''):
        """获取交易数据"""
        tradeList = []
        
        for l in self.tradeDict.values():
            for trade in l:
                if not symbol:
                    tradeList.append(trade)
                elif trade.symbol == symbol:
                    tradeList.append(trade)
        
        return tradeList

class TradeData(object):
    def __init__(self, symbol, dt, direction, offset, price, volume):
        self.dt = dt
        self.symbol = symbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume

class DailyResult(object):
    """每日的成交记录"""
    def __init__(self, date):
        self.date = date
        
        self.closeDict = {}                     # 收盘价字典
        self.previousCloseDict = {}             # 昨收盘字典
        
        self.tradeDict = defaultdict(list)      # 成交字典
        self.posDict = {}                       # 持仓字典（开盘时）
        
        self.tradingPnl = 0                     # 交易盈亏
        self.holdingPnl = 0                     # 持仓盈亏
        self.totalPnl = 0                       # 总盈亏
        self.commission = 0                     # 佣金
        self.slippage = 0                       # 滑点
        self.netPnl = 0                         # 净盈亏
        self.tradeCount = 0                     # 成交笔数
    
    def updateTrade(self, trade):
        """更新交易"""
        l = self.tradeDict[trade.symbol]
        l.append(trade)
        self.tradeCount += 1
        
    def updatePos(self, d):
        """更新昨持仓"""
        self.posDict.update(d)
    
    def updateBar(self, bar):
        """更新K线"""
        self.closeDict[bar.symbol] = bar.close_price
    
    def updatePreviousClose(self, d):
        """更新昨收盘"""
        self.previousCloseDict.update(d)

    def calculateTradingPnl(self):
        """计算当日交易盈亏"""
        for symbol, l in self.tradeDict.items():
            close = self.closeDict[symbol]

            slippage = SLIPPAGE_DICT[symbol] * PRICETICK_DICT[symbol]
            variableCommission = VARIABLE_COMMISSION_DICT[symbol]
            
            for trade in l:
                if trade.direction == Direction.LONG:
                    side = 1
                else:
                    side = -1

                commissionCost = trade.volume * trade.price * variableCommission
                slippageCost = trade.volume * slippage

                if close:
                    pnl = (close - trade.price) * trade.volume * side
                    self.commission += commissionCost
                    self.slippage += slippageCost
                    self.tradingPnl += pnl
                else:
                    print('*' * 20)
                    print('%s\t%s volume：%s\t计算当日交易盈亏数据缺失' % (self.date, symbol, trade.volume))
                    print('*' * 20 + '\n')
    
    def calculateHoldingPnl(self):
        """计算当日持仓盈亏"""
        for symbol, pos in self.posDict.items():
            previousClose = self.previousCloseDict.get(symbol, 0)
            close = self.closeDict.get(symbol, 0)
            #close = self.closeDict[symbol]

            if close:
                if previousClose:
                    pnl = (close - previousClose) * pos
                    self.holdingPnl += pnl
            elif pos:
                print('*'*20)
                print('%s\t%s pos：%s\t计算当日持仓盈亏数据缺失' % (self.date, symbol, pos))
                print('*'*20 + '\n')

    def calculatePnl(self):
        """计算总盈亏"""
        self.calculateHoldingPnl()
        self.calculateTradingPnl()

        self.totalPnl = self.holdingPnl + self.tradingPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage

def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符
