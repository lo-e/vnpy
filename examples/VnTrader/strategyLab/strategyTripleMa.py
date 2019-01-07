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
import talib

class TripleMaStrategy(CtaTemplate):
    """
    螺纹钢、5分钟级别、
    三均线策略
    10，20，120均线
    120均线做多空过滤

    MA120之上
        MA10 上穿 MA20，金叉，做多
        MA10 下穿 MA20，死叉，平多
    MA120之下
        MA10 下穿 MA20，死叉，做空
        MA10 上穿 MA20，金叉，平空

    移动百分比止损
    """
    className = 'TripleMaStrategy'
    author = u'loe'

    # 每笔交易数量
    tradeSize = 1
    """ 以下默认值可以在BacktestingEngine 或者 CtaEngine中设置更改 """
    # 默认5分钟k线
    barMin = 5
    # 默认MA120均线做信号过滤
    maFilter = 120
    # 默认MA10均线快速均线
    maFast = 10
    # 默认MA20慢速均线
    maSlow = 20

    #移动百分比止损
    stopPercent = 1.0 / 100

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'barMin',
                 'maFilter',
                 'maFast',
                 'maSlow'
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
        super(TripleMaStrategy, self).__init__(ctaEngine, setting)

        # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        # k线生成器
        self.barGenerator = BarGenerator(self.onBar, self.barMin, self.onMultiBar)
        # k线管理器，用于生成均线值
        self.arrayManager = ArrayManager(size=self.maFilter)
        # 记录最新tick，用于判断主力合约是否换月
        self.lastSymbol = ''
        # 当前持仓后的最高最低价，用于移动止损
        self.highPrice = 0
        self.lowPrice = 0
        self.isStopping = False

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
                self.arrayManager = ArrayManager(size=self.maFilter)

            self.lastSymbol = tick.vtSymbol

        else:
            # 止损百分比止损操作
            if self.pos > 0:
                if tick.lastPrice <= self.highPrice*(1 - self.stopPercent):
                    self.sell(tick.lastPrice - 10, abs(self.pos))
                    self.isStopping = True
            if self.pos < 0:
                if tick.lastPrice >= self.lowPrice*(1 + self.stopPercent):
                    self.cover(tick.lastPrice + 10, abs(self.pos))
                    self.isStopping = True

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
        self.arrayManager.updateBar(bar)
        if not self.arrayManager.inited:
            return

        # 快速均线
        maFastArray = self.arrayManager.sma(self.maFast, True)
        maFastValue = maFastArray[-1]
        # 慢速均线
        maSlowArray = self.arrayManager.sma(self.maSlow, True)
        maSlowValue = maSlowArray[-1]
        # 信号过滤均线
        maFilterValue = self.arrayManager.sma(self.maFilter)

        # 快速均线的MA5
        maFast_ma5 = talib.MA(np.array(maFastArray, dtype=float), 5)[-1]

        ''' 信号逻辑 '''
        # 无论开仓平仓信号产生，下单前都要通过isStopping确认是否正在止损
        if not self.pos:
            # 空仓状态
            if maFastValue > maFilterValue and maSlowValue > maFilterValue and (maFastValue > maSlowValue) and (maFastArray[-2] < maSlowArray[-2]) and (maFast_ma5 < maFastValue):
                # 快速均线和慢速均线都在过滤信号均线之上，并且快速均线上穿慢速均线，开仓做多
                if not self.isStopping:
                    self.buy(bar.close + 10, self.tradeSize)
            elif maFastValue < maFilterValue and maSlowValue < maFilterValue and (maFastValue < maSlowValue) and (maFastArray[-2] > maSlowArray[-2]) and (maFast_ma5 > maFastValue):
                # 快速均线和慢速均线都在过滤信号均线之下，并且快速均线下穿慢速均线，开仓做空
                if not self.isStopping:
                    self.short(bar.close-10, self.tradeSize)
        elif self.pos > 0:
            # 持有多仓
            if not self.isStopping and maFastValue < maSlowValue:
                # 并且快速均线下穿慢速均线，平多
                self.sell(bar.close - 10, abs(self.pos))
        elif self.pos < 0:
            # 持有空仓
            if not self.isStopping and maFastValue > maSlowValue:
                # 并且快速均线上穿慢速均线，平空
                self.cover(bar.close + 10, abs(self.pos))

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
        elif self.pos == 0:
            self.isStopping = False

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
    engine.initStrategy(TripleMaStrategy, settingDict)

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
                      '20180106', '20180206', 0,
                      0 / 10000, tickPrice)

    #"""
    # 回测
    engine.strategy.name = 'tripleMa_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    # engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #"""

