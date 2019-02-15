# encoding: UTF-8

"""
单标的海龟交易策略，实现了完整海龟策略中的信号部分。
"""

from __future__ import division

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import (DIRECTION_LONG, DIRECTION_SHORT,
                                    OFFSET_OPEN, OFFSET_CLOSE)
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate, 
                                                     BarGenerator, 
                                                     ArrayManager)
from vnpy.trader.app.ctaStrategy.ctaBase import *


########################################################################
class TurtleStrategy(CtaTemplate):
    """海龟交易策略"""
    className = 'TurtleStrategy'
    author = u'loe'

    # 策略参数
    entryWindow = 20                    # 入场通道窗口
    exitWindow = 10                     # 出场通道窗口
    atrWindow = 15                      # 计算ATR波动率的窗口

    # 策略变量
    entryUp = 0                         # 入场通道上轨
    entryDown = 0                       # 入场通道下轨
    exitUp = 0                          # 出场通道上轨
    exitDown = 0                        # 出场通道下轨
    atrVolatility = 0                   # ATR波动率
    
    longEntry1 = 0                      # 多头入场价格
    longEntry2 = 0
    longEntry3 = 0
    longEntry4 = 0
    shortEntry1 = 0                     # 空头入场价格
    shortEntry2 = 0
    shortEntry3 = 0
    shortEntry4 = 0
    longStop = 0                        # 多头止损价格
    shortStop = 0                       # 空头止损价格

    multiplier = 0                      # unit大小
    virtualUnit = 0                     # 信号仓位
    unit = 0                            # 实际持有仓位
    
    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'vtSymbol',
                 'perSize',
                 'entryWindow',
                 'exitWindow',
                 'atrWindow']


    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'entryUp',
               'entryDown',
               'exitUp',
               'exitDown',
               'longEntry1',
               'longEntry2',
               'longEntry3',
               'longEntry4',
               'shortEntry1',
               'shortEntry2',
               'shortEntry3',
               'shortEntry4',
               'longStop',
               'shortStop',
               'virtualUnit',
               'unit']
    
    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, turtlePortfolio, setting):
        """Constructor"""
        super(TurtleStrategy, self).__init__(ctaEngine, setting)

        self.portfolio = turtlePortfolio
        self.am = ArrayManager(60)
        self.atrAm = ArrayManager(self.atrWindow+1)

        self.portfolio.newSignal(self, DIRECTION_LONG, OFFSET_OPEN)
        
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' %self.name)
        self.barDbName = DAILY_DB_NAME
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(300)
        for bar in initData:
            self.onBar(bar)

        self.putEvent()

    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        if self.pos == 0:
            unitChange = 0
            # 多头开仓
            if tick.lastPrice >= self.longEntry1 and self.virtualUnit < 1:
                unitChange += 1
                self.virtualUnit += 1

            if tick.lastPrice >= self.longEntry2 and self.virtualUnit < 2:
                unitChange += 1
                self.virtualUnit += 1

            if tick.lastPrice >= self.longEntry2 and self.virtualUnit < 3:
                unitChange += 1
                self.virtualUnit += 1




        pass

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        self.cancelAll()
    
        # 保存K线数据
        self.am.updateBar(bar)
        self.atrAm.updateBar(bar)
        if not self.am.inited or not self.atrAm.inited:
            return
        
        # 计算指标数值
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)
        
        # 判断是否要进行交易
        if self.pos == 0:
            # 计算atr
            self.atrVolatility = self.am.atr(self.atrWindow)

            self.longEntry1 = self.entryUp
            self.longEntry2 = self.longEntry1 + 0.5*self.atrVolatility
            self.longEntry3 = self.longEntry2 + 0.5*self.atrVolatility
            self.longEntry4 = self.longEntry3 + 0.5*self.atrVolatility

            self.shortEntry1 = self.entryDown
            self.shortEntry2 = self.shortEntry1 - 0.5*self.atrVolatility
            self.shortEntry3 = self.shortEntry1 - 0.5*self.atrVolatility
            self.shortEntry4 = self.shortEntry1 - 0.5*self.atrVolatility
            self.longStop = 0
            self.shortStop = 0

            size = self.sizeDict[signal.vtSymbol]
            riskValue = self.portfolioValue * 0.01
            """ modify by loe """
            multiplier = 0
            if signal.atrVolatility * size:
                multiplier = riskValue / (signal.atrVolatility * size)
                multiplier = int(round(multiplier, 0))
            self.multiplierDict[signal.vtSymbol] = multiplier
        
        # 同步数据到数据库
        self.saveSyncData()        
    
        # 发出状态更新事件
        self.putEvent()        

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """成交推送"""
        if trade.direction == DIRECTION_LONG:
            self.longEntry = trade.price
            self.longStop = self.longEntry - self.atrVolatility * 2
        else:
            self.shortEntry = trade.price
            self.shortStop = self.shortEntry + self.atrVolatility * 2
        
        # 发出状态更新事件
        self.putEvent()

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass
        
