# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate, BarGenerator, ArrayManager)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME
import numpy as np

class TripleBarStrategy(CtaTemplate):
    """
    自定义bar，连续三个收盘涨或者收盘跌，反向开仓
    移动止损
    """
    className = 'TripleBarStrategy'
    author = u'loe'

    # bar类型
    barMin = 3
    # 交易单位
    tradeSize = 1

    #移动百分比止损
    stopPercent = 1.0 / 100

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol'
                 ]

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               ]

    # 同步列表
    syncList = ['pos'
                ]

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(TripleBarStrategy, self).__init__(ctaEngine, setting)

        # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        # k线生成器
        self.barGenerator = BarGenerator(self.onBar, self.barMin, self.onMultiBar)
        # k线管理器
        self.arrayManager = ArrayManager(size=3)
        # 记录最新tick，用于判断主力合约是否换月
        self.lastSymbol = ''
        # 当前持仓后的最高最低价，用于移动止损
        self.highPrice = 0
        self.lowPrice = 0
        self.direction = EMPTY_STRING

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' % self.name)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 记录当前合约月份
        if not self.lastSymbol or self.lastSymbol != tick.vtSymbol:
            if self.lastSymbol:
                # 移仓换月，清仓，这里涉及主力换月后的价格，暂时用新主力价格交易
                if self.pos > 0:
                    self.sell(tick.lastPrice - 10, abs(self.pos))
                elif self.pos < 0:
                    self.cover(tick.lastPrice + 10, abs(self.pos))

                # 重新初始化
                self.barGenerator = BarGenerator(self.onBar, self.barMin, self.onMultiBar)
                self.arrayManager = ArrayManager(size=3)

            self.lastSymbol = tick.vtSymbol

        else:
            # 止损百分比止损操作
            if self.pos > 0:
                if tick.lastPrice <= self.highPrice*(1 - self.stopPercent):
                    self.sell(tick.lastPrice - 10, abs(self.pos))
            if self.pos < 0:
                if tick.lastPrice >= self.lowPrice*(1 + self.stopPercent):
                    self.cover(tick.lastPrice + 10, abs(self.pos))

        self.highPrice = max(self.highPrice, tick.lastPrice)
        self.lowPrice = min(self.lowPrice, tick.lastPrice)

        self.barGenerator.updateTick(tick)
        pass

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        self.barGenerator.updateBar(bar)
        pass

    def onMultiBar(self, bar):
        # k线来了
        # 平仓当前仓位
        if self.pos > 0:
            self.sell(bar.close - 10, abs(self.pos))
        elif self.pos < 0:
            self.cover(bar.close + 10, abs(self.pos))

        self.arrayManager.updateBar(bar)
        if not self.arrayManager.inited:
            return

        openList = self.arrayManager.openArray
        closeList = self.arrayManager.closeArray
        theDirection = EMPTY_STRING
        if closeList[0] > openList[0] and closeList[1] > openList[1] and closeList[2] > openList[2]:
            # 卖出信号
            theDirection = 'ask'
        elif closeList[0] < openList[0] and closeList[1] < openList[1] and closeList[2] < openList[2]:
            # 买入信号
            theDirection = 'bid'

        if theDirection and self.direction != theDirection:
            if theDirection == 'bid':
                self.buy(bar.close + 10, self.tradeSize)
            elif theDirection == 'ask':
                self.short(bar.close - 10, self.tradeSize)

        self.direction = theDirection


    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        if trade.offset == u'开仓':
            self.highPrice = trade.price
            self.lowPrice = trade.price

        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

#===================================回测==================================
def GetEngin(settingDict, symbol,
                   startDate, endDate, slippage,
                   rate, priceTick):
    """运行单标的回测"""
    # 创建回测引擎实例
    engine = BacktestingEngine()

    # 设置引擎的回测模式为Tick
    engine.setBacktestingMode(engine.TICK_MODE)

    # 设置使用的数据库
    engine.setDatabase(TICK_DB_NAME, symbol)

    # 设置回测的起始日期
    engine.setStartDate(startDate, initDays=0)

    # 设置回测的截至日期
    engine.setEndDate(endDate)

    # 滑点设置
    engine.setSlippage(slippage)

    # 合约交易手续费
    engine.setRate(rate)

    # 合约最小价格变动
    engine.setPriceTick(priceTick)

    # 引擎中创建策略对象
    engine.initStrategy(TripleBarStrategy, settingDict)

    # 起始资金
    engine.setCapital(engine.strategy.capital)

    # 合约每手数量
    engine.setSize(engine.strategy.perSize)

    return  engine

if __name__ == '__main__':
    # 回测
    tickPrice = 1
    setting = {'capital': 6000,
               'lever': 5,
               'perSize': 10}
    engine = GetEngin(setting, 'rb00.TB',
                      '20170102', '20180206', 0,
                      1.07 / 10000, tickPrice)

    #"""
    # 回测
    engine.strategy.name = 'tripleBar_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #"""

