# encoding: UTF-8

from __future__ import print_function

from csv import DictReader
from datetime import datetime, timedelta
from collections import OrderedDict, defaultdict

import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient

from vnpy.trader.object import BarData
from vnpy.trader.constant import Direction, Exchange

from martingStrategy_trending_forward import MartingForwardPortfolio
from martingStrategy_trending_inverse import MartingInversePortfolio

from vnpy.app.cta_strategy.base import DAILY_DB_NAME, MINUTE_DB_NAME, HOUR_DB_NAME, MinuteDataBaseName, HourDataBaseName
import pandas as pd

PRICETICK_DICT = {}
VARIABLE_COMMISSION_DICT = {}
SLIPPAGE_DICT = {}

########################################################################
class BacktestingEngine(object):
    """组合类CTA策略回测引擎"""
    def __init__(self):
        """Constructor"""
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

        self.target_symbol_list = []
        self.tradingStart = None
    
    def setPeriod(self, startDt, endDt):
        """设置回测周期"""
        self.startDt = startDt
        self.endDt = endDt
    
    def initListPortfolio(self, l, marting_type:str, exchange:str, portfolioValue=10000000, history_file:str=""):
        """初始化投资组合"""
        self.portfolioValue = portfolioValue

        for d in l:
            self.symbolList.append(d['symbol'])
            self.priceTickDict[d['symbol']] = float(d['priceTick'])
            self.min_volume_dict[d['symbol']] = float(d['min_volume'])
            PRICETICK_DICT[d['symbol']] = float(d['priceTick'])
            VARIABLE_COMMISSION_DICT[d['symbol']] = float(d['variableCommission'])
            SLIPPAGE_DICT[d['symbol']] = float(d['slippage'])

        if marting_type == "FORWARD":
            self.portfolio = MartingForwardPortfolio(self)

            if exchange == "BINANCE":
                # 交集：25
                # self.target_symbol_list = ['ANKRUSDT.BINANCE', 'AXSUSDT.BINANCE', 'BELUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DASHUSDT.BINANCE', 'DOGEUSDT.BINANCE', 'EGLDUSDT.BINANCE', 'ENJUSDT.BINANCE', 'ETCUSDT.BINANCE', 'ETHUSDT.BINANCE', 'FTMUSDT.BINANCE', 'GALAUSDT.BINANCE', 'MKRUSDT.BINANCE', 'OGNUSDT.BINANCE', 'OMGUSDT.BINANCE', 'RLCUSDT.BINANCE', 'SFPUSDT.BINANCE', 'SKLUSDT.BINANCE', 'STORJUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'SXPUSDT.BINANCE', 'WAVESUSDT.BINANCE', 'YFIUSDT.BINANCE', 'ZILUSDT.BINANCE', 'ZRXUSDT.BINANCE']
                # 并集：82
                self.target_symbol_list = ['1000SHIBUSDT.BINANCE', '1000XECUSDT.BINANCE', 'AAVEUSDT.BINANCE', 'ADAUSDT.BINANCE', 'ALGOUSDT.BINANCE', 'ALPHAUSDT.BINANCE', 'ANKRUSDT.BINANCE', 'ARPAUSDT.BINANCE', 'ARUSDT.BINANCE', 'ATAUSDT.BINANCE', 'ATOMUSDT.BINANCE', 'AUDIOUSDT.BINANCE', 'AVAXUSDT.BINANCE', 'AXSUSDT.BINANCE', 'BAKEUSDT.BINANCE', 'BATUSDT.BINANCE', 'BCHUSDT.BINANCE', 'BELUSDT.BINANCE', 'BLZUSDT.BINANCE', 'BNBUSDT.BINANCE', 'C98USDT.BINANCE', 'CELRUSDT.BINANCE', 'CHRUSDT.BINANCE', 'COTIUSDT.BINANCE', 'CRVUSDT.BINANCE', 'CTKUSDT.BINANCE', 'CTSIUSDT.BINANCE', 'DASHUSDT.BINANCE', 'DENTUSDT.BINANCE', 'DGBUSDT.BINANCE', 'DOGEUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'EGLDUSDT.BINANCE', 'ENJUSDT.BINANCE', 'EOSUSDT.BINANCE', 'ETCUSDT.BINANCE', 'ETHUSDT.BINANCE', 'FILUSDT.BINANCE', 'FLMUSDT.BINANCE', 'FTMUSDT.BINANCE', 'GALAUSDT.BINANCE', 'GTCUSDT.BINANCE', 'IOSTUSDT.BINANCE', 'IOTAUSDT.BINANCE', 'KAVAUSDT.BINANCE', 'KNCUSDT.BINANCE', 'LINAUSDT.BINANCE', 'LITUSDT.BINANCE', 'LRCUSDT.BINANCE', 'MANAUSDT.BINANCE', 'MASKUSDT.BINANCE', 'MATICUSDT.BINANCE', 'MKRUSDT.BINANCE', 'NEARUSDT.BINANCE', 'NEOUSDT.BINANCE', 'OGNUSDT.BINANCE', 'OMGUSDT.BINANCE', 'ONEUSDT.BINANCE', 'PEOPLEUSDT.BINANCE', 'RENUSDT.BINANCE', 'RLCUSDT.BINANCE', 'RSRUSDT.BINANCE', 'RUNEUSDT.BINANCE', 'SFPUSDT.BINANCE', 'SKLUSDT.BINANCE', 'SOLUSDT.BINANCE', 'STORJUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'SXPUSDT.BINANCE', 'TRXUSDT.BINANCE', 'UNFIUSDT.BINANCE', 'UNIUSDT.BINANCE', 'WAVESUSDT.BINANCE', 'XEMUSDT.BINANCE', 'XLMUSDT.BINANCE', 'XRPUSDT.BINANCE', 'XTZUSDT.BINANCE', 'YFIUSDT.BINANCE', 'ZECUSDT.BINANCE', 'ZENUSDT.BINANCE', 'ZILUSDT.BINANCE', 'ZRXUSDT.BINANCE']

        else:
            self.portfolio = MartingInversePortfolio(self)

            if exchange == "BINANCE":
                # （TRENDING_INCREASE_RATE 0.04）最大连续趋势追踪6：8
                self.target_symbol_list = ['ALGOUSDT.BINANCE', 'ATOMUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE', 'TRXUSDT.BINANCE']
                self.target_symbol_list = ['ALGOUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE']
                # self.target_symbol_list = ['ALGOUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'ENSUSDT.BINANCE', 'EOSUSDT.BINANCE']
                # self.target_symbol_list = ['ALGOUSDT.BINANCE', 'CHRUSDT.BINANCE', 'DYDXUSDT.BINANCE', 'EOSUSDT.BINANCE', 'SUSHIUSDT.BINANCE']
                self.target_symbol_list = ["LINKUSDT.BINANCE", "BNBUSDT.BINANCE", "EOSUSDT.BINANCE", "AXSUSDT.BINANCE"]
            
            elif exchange == "OKX":
                self.target_symbol_list = ['ALGO-USDT-SWAP.OKX', 'DYDX-USDT-SWAP.OKX', 'ENS-USDT-SWAP.OKX', 'EOS-USDT-SWAP.OKX', 'SUSHI-USDT-SWAP.OKX']
            
        # 筛选合约
        if self.target_symbol_list:
            temp = []
            for symbol in self.target_symbol_list:
                if symbol in self.symbolList:
                    temp.append(symbol)
            self.symbolList = temp
        self.portfolio.init(portfolioValue, self.symbolList, history_file=history_file)
        self.portfolio.tradingStart = self.tradingStart

        self.output(f"投资组合的合约代码：{len(self.symbolList)}\n{self.symbolList}")
        self.output(f"投资组合的初始价值：{portfolioValue}")
    
    def loadData(self):
        """加载数据"""
        mc = MongoClient()
        db = mc[MinuteDataBaseName(5)]

        """ modify by loe """
        dataDict = {}
        for symbol in self.symbolList:
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
            
            self.output(u'%s数据加载完成，总数据量：%s' %(symbol, cursor.count()))

        dateList = sorted(dataDict.keys())
        for theDatetime in dateList:
            self.dataDict[theDatetime] = dataDict[theDatetime]
        
        self.output(u'全部数据加载完成')
    
    def runBacktesting(self, daily_mode:bool=False):
        """运行回测"""
        self.output(u'开始回放K线数据')
        
        for i in range(len(self.dataDict)):
            dt = list(self.dataDict.keys())[i]
            barDict = self.dataDict[dt]
            self.currentDt = dt
            
            # 确认是否更新Result
            result_update = True
            if daily_mode and self.result:
                last_dt = self.result.date
                if last_dt.hour < 8:
                    next_dt = last_dt.replace(hour=8, minute=0, second=0, microsecond=0)

                else:
                    next_dt = (last_dt + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
                
                if dt < next_dt:
                    result_update = False
            
            # 最后的数据必须更新Result
            if i == len(self.dataDict) - 1:
                result_update = True

            if result_update:
                previousResult = self.result
                self.result = DailyResult(dt)
                self.result.updatePos(self.portfolio.posDict)
                self.resultList.append(self.result)
                if previousResult:
                    self.result.updatePreviousClose(previousResult.closeDict)
            
            for bar in barDict.values():
                self.portfolio.onBar(bar)
                self.result.updateBar(bar)
        
        self.output(u'K线数据回放结束')
    
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
        """ modify by loe dailyTradeCount计算修改"""
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
            'drawdownSeries': drawdownSeries,
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
        self.output(u'最大回撤【本金】: \t%s\t%s' % (formatNumber(result['maxDrawdownOrigin']), result['maxDrawdownOriginDate']))
        self.output(u'百分比最大回撤【本金】: %s%%\t%s' % (formatNumber(result['maxDdPercentOrigin']), result['maxDdPercentOriginDate']))
        
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

        if figSavedPath:
            plt.savefig(figSavedPath)
        
        plt.show()        
    
    def sendOrder(self, symbol, direction, offset, price, volume):
        """记录交易数据（由portfolio调用）"""
        # 记录成交数据
        """ modify by loe """
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

    
########################################################################
class TradeData(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, symbol, dt, direction, offset, price, volume):
        """Constructor"""
        """ modify by loe """
        self.dt = dt
        self.symbol = symbol
        self.direction = direction
        self.offset = offset
        self.price = price
        self.volume = volume


########################################################################
class DailyResult(object):
    """每日的成交记录"""

    #----------------------------------------------------------------------
    def __init__(self, date):
        """Constructor"""
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
    
    #----------------------------------------------------------------------
    def updateTrade(self, trade):
        """更新交易"""
        l = self.tradeDict[trade.symbol]
        l.append(trade)
        self.tradeCount += 1
        
    #----------------------------------------------------------------------
    def updatePos(self, d):
        """更新昨持仓"""
        self.posDict.update(d)
    
    #----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.closeDict[bar.symbol] = bar.close_price
    
    #----------------------------------------------------------------------
    def updatePreviousClose(self, d):
        """更新昨收盘"""
        self.previousCloseDict.update(d)

    #----------------------------------------------------------------------
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


#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符
