# encoding: UTF-8

"""
单标的海龟交易策略，实现了完整海龟策略中的信号部分。
"""

from __future__ import division

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import (DIRECTION_LONG, DIRECTION_SHORT,
                                    OFFSET_OPEN, OFFSET_CLOSE)
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
from vnpy.trader.vtUtility import BarGenerator, ArrayManager
from vnpy.trader.app.ctaStrategy.ctaBase import *
import datetime
import re

#====== 交易时间 ======
#商品期货
MORNING_START_CF = datetime.time(9, 0)
MORNING_REST_CF = datetime.time(10, 15)
MORNING_RESTART_CF = datetime.time(10, 30)
MORNING_END_CF = datetime.time(11, 30)
AFTERNOON_START_CF = datetime.time(13, 30)
AFTERNOON_END_CF = datetime.time(15, 0)

# 商品期货夜盘时间
NIGHT_START_CF = datetime.time(21, 0)
NIGHT_END_CF_N = datetime.time(23, 0) # 到夜间收盘
NIGHT_END_CF_NM = datetime.time(1, 0) # 到凌晨收盘
NIGHT_END_CF_M = datetime.time(2, 30) # 到凌晨收盘

#股指期货
MORNING_PRE_START_SF = datetime.time(6, 0)
MORNING_START_SF = datetime.time(9, 30)
MORNING_END_SF = datetime.time(11, 30)
AFTERNOON_START_SF = datetime.time(13, 0)
AFTERNOON_END_SF = datetime.time(15, 0)


def isFinanceSymbol(symbol):
    financeSymbols = ['IF', 'IC', 'IH']
    startSymbol = re.sub("\d", "", symbol)
    if startSymbol in financeSymbols:
        return True
    else:
        return False


########################################################################
class TurtleStrategy(CtaTemplate):
    """海龟交易策略"""
    className = 'TurtleStrategy'
    author = u'loe'

    # 策略参数
    lastSymbol = ''                 # 换月主力合约
    entryWindow = 20                    # 入场通道窗口
    exitWindow = 10                     # 出场通道窗口
    atrWindow = 15                      # 计算ATR波动率的窗口

    # 策略变量
    lastSymbolClearNeed = False         # 需要清空前主力合约仓位
    lastClearPos = 0
    posInitialNeed = False              # 需要初始化新主力合约仓位

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
                 'lastSymbol',
                 'perSize',
                 'tickPrice',
                 'entryWindow',
                 'exitWindow',
                 'atrWindow']


    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'lastSymbolClearNeed',
               'entryUp',
               'entryDown',
               'exitUp',
               'exitDown',
               'atrVolatility',
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
               'multiplier',
               'virtualUnit',
               'unit']
    
    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos',
                'atrVolatility',
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
                'multiplier',
                'virtualUnit',
                'unit']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, turtlePortfolio, setting):
        """Constructor"""
        super(TurtleStrategy, self).__init__(ctaEngine, setting)

        self.portfolio = turtlePortfolio
        self.am = ArrayManager(self.entryWindow+1)
        self.atrAm = ArrayManager(self.atrWindow+1)
        
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' %self.name)
        if self.lastSymbol:
            # 主力换月处理
            # 需要清空前主力合约仓位
            if self.pos:
                self.lastSymbolClearNeed = True
                self.clearPos = self.pos

            # 需要建立新主力仓位，初始化新主力状态
            self.newDominantInitial()

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
        if not self.trading:
            return

        # 过滤无效tick
        t = tick.datetime.time()
        isFinance = isFinanceSymbol(tick.vtSymbol)
        if not isFinance:
            if NIGHT_END_CF_M <= t < MORNING_START_CF or MORNING_REST_CF <= t < MORNING_RESTART_CF or MORNING_END_CF <= t < AFTERNOON_START_CF or AFTERNOON_END_CF <= t < NIGHT_START_CF or NIGHT_END_CF_M <= t < MORNING_START_CF:
                self.writeCtaLog(u'====== 过滤无效tick：%s\t%s ======' % (tick.vtSymbol, tick.datetime))
                return
        else:
            if MORNING_PRE_START_SF <= t < MORNING_START_SF or MORNING_END_SF <= t < AFTERNOON_START_SF or AFTERNOON_END_SF <= t:
                self.writeCtaLog(u'====== 过滤无效tick：%s\t%s ======' % (tick.vtSymbol, tick.datetime))
                return

        # 主力换月时清空前主力仓位
        if tick.vtSymbol == self.lastSymbol:
            if self.lastSymbolClearNeed:
                if self.lastClearPos > 0:
                    self.portfolio.newSignal(self.lastSymbol, DIRECTION_SHORT, OFFSET_CLOSE)
                    self.sendSymbolOrder(self.lastSymbol, CTAORDER_SELL, tick.lastPrice - self.tickPrice * 20,
                                         abs(self.lastClearPos))

                elif self.lastClearPos < 0:
                    self.portfolio.newSignal(self.lastSymbol, DIRECTION_LONG, OFFSET_CLOSE)
                    self.sendSymbolOrder(self.lastSymbol, CTAORDER_COVER, tick.lastPrice + self.tickPrice * 20,
                                         abs(self.lastClearPos))

                self.lastSymbolClearNeed = False
            return

        # 新主力仓位初始化
        if self.posInitialNeed:
            if self.unit > 0:
                self.buy(tick.lastPrice + self.tickPrice * 20, self.multiplier * abs(self.unit))

            elif self.unit < 0:
                self.short(tick.lastPrice - self.tickPrice * 20, self.multiplier * abs(self.unit))

            self.putEvent()
            self.posInitialNeed = False
            return

        # 撮合信号与交易
        if not self.am.inited or not self.atrAm.inited:
            return
        unitChange = 0
        action = False

        if self.virtualUnit >= 0:
            # 多头开仓加仓
            if tick.lastPrice >= self.longEntry1 and self.virtualUnit < 1:
                self.virtualUnit += 1
                action = True

                # 检查是否保证金超限
                if self.checkBondOver(tick.lastPrice):
                    return

                # 组合仓位管理
                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_LONG, OFFSET_OPEN):
                    unitChange += 1

            if tick.lastPrice >= self.longEntry2 and self.virtualUnit < 2:
                self.virtualUnit += 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_LONG, OFFSET_OPEN):
                    unitChange += 1

            if tick.lastPrice >= self.longEntry3 and self.virtualUnit < 3:
                self.virtualUnit += 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_LONG, OFFSET_OPEN):
                    unitChange += 1

            if tick.lastPrice >= self.longEntry4 and self.virtualUnit < 4:
                self.virtualUnit += 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_LONG, OFFSET_OPEN):
                    unitChange += 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.buy(tick.lastPrice+self.tickPrice*20, self.multiplier*abs(unitChange))

                self.putEvent()
                return

            # 止损平仓
            if self.virtualUnit > 0:
                longExit = max(self.longStop, self.exitDown)
                if tick.lastPrice <= longExit:
                    self.virtualUnit = 0
                    self.portfolio.newSignal(self.vtSymbol, DIRECTION_SHORT, OFFSET_CLOSE)
                    self.unit = 0
                    if self.pos > 0:
                        self.sell(tick.lastPrice-self.tickPrice*20, abs(self.pos))
                    # 平仓后更新最新指标
                    self.updateIndicator()

                self.putEvent()
                return

        if self.virtualUnit <= 0:
            # 空头开仓加仓
            if tick.lastPrice <= self.shortEntry1 and self.virtualUnit > -1:
                self.virtualUnit -= 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_SHORT, OFFSET_OPEN):
                    unitChange -= 1

            if tick.lastPrice <= self.shortEntry2 and self.virtualUnit > -2:
                self.virtualUnit -= 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_SHORT, OFFSET_OPEN):
                    unitChange -= 1

            if tick.lastPrice <= self.shortEntry3 and self.virtualUnit > -3:
                self.virtualUnit -= 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_SHORT, OFFSET_OPEN):
                    unitChange -= 1

            if tick.lastPrice <= self.shortEntry4 and self.virtualUnit > -4:
                self.virtualUnit -= 1
                action = True

                if self.checkBondOver(tick.lastPrice):
                    return

                if self.portfolio.newSignal(self.vtSymbol, DIRECTION_SHORT, OFFSET_OPEN):
                    unitChange -= 1

            if action:
                if unitChange:
                    self.unit += unitChange
                    self.short(tick.lastPrice - self.tickPrice * 20, self.multiplier * abs(unitChange))

                self.putEvent()
                return

            # 止损平仓
            if self.virtualUnit < 0:
                shortExit = min(self.shortStop, self.exitUp)
                if tick.lastPrice >= shortExit:
                    self.virtualUnit = 0
                    self.portfolio.newSignal(self.vtSymbol, DIRECTION_LONG, OFFSET_CLOSE)
                    self.unit = 0
                    if self.pos < 0:
                        self.cover(tick.lastPrice + self.tickPrice * 20, abs(self.pos))
                    # 平仓后更新最新指标
                    self.updateIndicator()

                self.putEvent()
                return

        self.putEvent()

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""

        # 保存K线数据
        self.am.updateBar(bar)
        self.atrAm.updateBar(bar)
        if not self.am.inited or not self.atrAm.inited:
            return
        
        # 计算指标数值
        self.entryUp, self.entryDown = self.am.donchian(self.entryWindow)
        self.exitUp, self.exitDown = self.am.donchian(self.exitWindow)

        # 判断是否要更新交易信号
        if self.virtualUnit == 0:
            self.updateIndicator()
        
        # 同步数据到数据库
        self.saveSyncData()        
    
        # 发出状态更新事件
        self.putEvent()        

    #----------------------------------------------------------------------

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """成交推送"""
        if trade.offset == OFFSET_OPEN:
            # 计算止损价格
            self.longStop = trade.price - 2*self.atrVolatility
            self.shortStop = trade.price + 2*self.atrVolatility
        
        # 发出状态更新事件
        self.putEvent()

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

    # 计算交易单位N
    def updateMultiplier(self):
        riskValue = self.portfolio.portfolioValue * 0.01
        if riskValue == 0:
            return

        multiplier = 0
        if self.atrVolatility * self.perSize:
            multiplier = riskValue / (self.atrVolatility * self.perSize)
            multiplier = int(round(multiplier, 0))
        self.multiplier = multiplier

    # 计算入场信号指标
    def updateIndicator(self):
        # 计算atr
        self.atrVolatility = self.atrAm.atr(self.atrWindow)

        self.updateMultiplier()

        self.longEntry1 = self.entryUp
        self.longEntry2 = self.longEntry1 + 0.5 * self.atrVolatility
        self.longEntry3 = self.longEntry2 + 0.5 * self.atrVolatility
        self.longEntry4 = self.longEntry3 + 0.5 * self.atrVolatility

        self.shortEntry1 = self.entryDown
        self.shortEntry2 = self.shortEntry1 - 0.5 * self.atrVolatility
        self.shortEntry3 = self.shortEntry2 - 0.5 * self.atrVolatility
        self.shortEntry4 = self.shortEntry3 - 0.5 * self.atrVolatility

        self.longStop = 0
        self.shortStop = 0

    # 检查预计交易保证金是否超限
    def checkBondOver(self, price):
        # 一个unit预计占用保证金不得超过初始资金的20%
        if price * self.multiplier * self.perSize * 0.1 > self.portfolio.portfolioValue * 0.2:
            self.portfolio.addOverBond(self.vtSymbol, price, self.perSize, self.multiplier, self.atrVolatility)
            return True
        else:
            return False

    # 主力换月，初始化交易状态
    def newDominantInitial(self):
        pass
