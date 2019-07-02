# encoding: UTF-8

from collections import defaultdict

from vnpy.trader.constant import Direction
from vnpy.trader.utility import ArrayManager

import re
from pymongo import MongoClient, ASCENDING
from vnpy.trader.object import BarData
from vnpy.app.cta_strategy.base import DAILY_DB_NAME
from App.Turtle.base import TRANSFORM_SYMBOL_LIST

########################################################################
class TurtleResult(object):
    """一次完整的开平交易"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.unit = 0
        self.entry = 0                  # 开仓均价
        self.exit = 0                   # 平仓均价
        self.pnl = 0                    # 盈亏
    
    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓或者加仓"""
        cost = self.unit * self.entry    # 计算之前的开仓成本
        cost += change * price           # 加上新仓位的成本
        self.unit += change              # 加上新仓位的数量
        self.entry = cost / self.unit    # 计算新的平均开仓成本

    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.exit = price
        self.pnl = self.unit * (self.exit - self.entry)
    

########################################################################
class TurtleInitialManager(object):
    """海龟策略初始化工具"""

    #----------------------------------------------------------------------
    def __init__(self, vtSymbol,
                 entryWindow, exitWindow, atrWindow):
        
        self.vtSymbol = vtSymbol        # 合约代码
        self.vtSymbol = self.vtSymbol.split('.')[0]
        self.entryWindow = entryWindow  # 入场通道周期数
        self.exitWindow = exitWindow    # 出场通道周期数
        self.atrWindow = atrWindow      # 计算ATR周期数

        self.am = ArrayManager(self.entryWindow+1)      # K线容器
        self.atrAm = ArrayManager(self.atrWindow+1)     # K线容器
        
        self.atrVolatility = 0          # ATR波动率
        self.entryUp = 0                # 入场通道
        self.entryDown = 0
        self.exitUp = 0                 # 出场通道
        self.exitDown = 0
        
        self.longEntry1 = 0             # 多头入场位
        self.longEntry2 = 0
        self.longEntry3 = 0
        self.longEntry4 = 0
        self.longStop = 0               # 多头止损位
        
        self.shortEntry1 = 0            # 空头入场位
        self.shortEntry2 = 0
        self.shortEntry3 = 0
        self.shortEntry4 = 0
        self.shortStop = 0              # 空头止损位
        
        self.unit = 0                   # 信号持仓
        self.result = None              # 当前的交易
        self.resultList = []            # 交易列表
        self.bar = None                 # 最新K线

        self.client = None              # 数据库
        self.barList = []               # 实盘交易合约bar数据

    def backtesting(self):
        # 获取数据库bar数据
        self.client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=600)
        self.client.server_info()

        db = self.client[DAILY_DB_NAME]

        collectionName = self.vtSymbol.upper()
        startSymbol = re.sub("\d", "", collectionName)
        if startSymbol in TRANSFORM_SYMBOL_LIST:
            endSymbol = re.sub("\D", "", collectionName)
            collectionName = startSymbol + '1' + endSymbol

        collection = db[collectionName]
        cursor = collection.find().sort('date')
        for dic in cursor:
            b = BarData(gateway_name='', symbol='', exchange=None, datetime=None, endDatetime=None)
            b.__dict__ = dic
            self.barList.append(b)

        # 回测数据
        for bar in self.barList:
            self.onBar(bar)


    #----------------------------------------------------------------------
    def onBar(self, bar):
        self.bar = bar
        self.am.update_bar(bar)
        self.atrAm.update_bar(bar)
        if not self.am.inited or not self.atrAm.inited:
            return

        self.generateSignal(bar)
        self.calculateIndicator()

    #----------------------------------------------------------------------
    def generateSignal(self, bar):
        """
        判断交易信号
        要注意在任何一个数据点：buy/sell/short/cover只允许执行一类动作
        """
        # 如果指标尚未初始化，则忽略
        if not self.longEntry1:
            return
        
        # 优先检查平仓
        if self.unit > 0:
            longExit = max(self.longStop, self.exitDown)
            
            if bar.low_price <= longExit:
                self.sell(longExit)
                return

        elif self.unit < 0:
            shortExit = min(self.shortStop, self.exitUp)

            if bar.high_price >= shortExit:
                self.cover(shortExit)
                return

        # 没有仓位或者持有多头仓位的时候，可以做多（加仓）
        if self.unit >= 0:
            trade = False
            
            if bar.high_price >= self.longEntry1 and self.unit < 1:
                self.buy(self.longEntry1, 1)
                trade = True
            
            if bar.high_price >= self.longEntry2 and self.unit < 2:
                self.buy(self.longEntry2, 1)
                trade = True
            
            if bar.high_price >= self.longEntry3 and self.unit < 3:
                self.buy(self.longEntry3, 1)
                trade = True
            
            if bar.high_price >= self.longEntry4 and self.unit < 4:
                self.buy(self.longEntry4, 1)
                trade = True
            
            if trade:
                return

        # 没有仓位或者持有空头仓位的时候，可以做空（加仓）
        if self.unit <= 0:
            if bar.low_price <= self.shortEntry1 and self.unit > -1:
                self.short(self.shortEntry1, 1)
            
            if bar.low_price <= self.shortEntry2 and self.unit > -2:
                self.short(self.shortEntry2, 1)
            
            if bar.low_price <= self.shortEntry3 and self.unit > -3:
                self.short(self.shortEntry3, 1)
            
            if bar.low_price <= self.shortEntry4 and self.unit > -4:
                self.short(self.shortEntry4, 1)
            
    #----------------------------------------------------------------------
    def calculateIndicator(self):
        """计算技术指标"""
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)
        
        # 有持仓后，ATR波动率和入场位等都不再变化
        if not self.unit:
            self.atrVolatility = self.atrAm.atr(self.atrWindow)
            
            self.longEntry1 = self.entryUp
            self.longEntry2 = self.entryUp + self.atrVolatility * 0.5
            self.longEntry3 = self.entryUp + self.atrVolatility * 1
            self.longEntry4 = self.entryUp + self.atrVolatility * 1.5
            self.longStop = 0
            
            self.shortEntry1 = self.entryDown
            self.shortEntry2 = self.entryDown - self.atrVolatility * 0.5
            self.shortEntry3 = self.entryDown - self.atrVolatility * 1
            self.shortEntry4 = self.entryDown - self.atrVolatility * 1.5
            self.shortStop = 0
    
    #----------------------------------------------------------------------
    def buy(self, price, volume):
        """买入开仓"""
        price = self.calculateTradePrice(Direction.LONG, price)
        self.open(price, volume)
        
        # 以最后一次加仓价格，加上两倍N计算止损
        self.longStop = price - self.atrVolatility * 2

    #----------------------------------------------------------------------
    def sell(self, price):
        """卖出平仓"""
        price = self.calculateTradePrice(Direction.SHORT, price)
        self.close(price)
    
    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖出开仓"""
        price = self.calculateTradePrice(Direction.SHORT, price)
        self.open(price, -volume)
        
        # 以最后一次加仓价格，加上两倍N计算止损
        self.shortStop = price + self.atrVolatility * 2
    
    #----------------------------------------------------------------------
    def cover(self, price):
        """买入平仓"""
        price = self.calculateTradePrice(Direction.LONG, price)
        self.close(price)

    #----------------------------------------------------------------------
    def open(self, price, change):
        """开仓"""
        self.unit += change 
        
        if not self.result:
            self.result = TurtleResult()
        self.result.open(price, change)
    
    #----------------------------------------------------------------------
    def close(self, price):
        """平仓"""
        self.unit = 0
        
        self.result.close(price)
        self.resultList.append(self.result)
        self.result = None

    #----------------------------------------------------------------------
    def getLastPnl(self):
        """获取上一笔交易的盈亏"""
        if not self.resultList:
            return 0
        
        result = self.resultList[-1]
        return result.pnl
    
    #----------------------------------------------------------------------
    def calculateTradePrice(self, direction, price):
        """计算成交价格"""
        # 买入时，停止单成交的最优价格不能低于当前K线开盘价
        if direction == Direction.LONG:
            tradePrice = max(self.bar.open_price, price)
        # 卖出时，停止单成交的最优价格不能高于当前K线开盘价
        else:
            tradePrice = min(self.bar.open_price, price)
        
        return tradePrice
    