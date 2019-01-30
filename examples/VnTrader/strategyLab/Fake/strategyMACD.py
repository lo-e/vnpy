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

class MACDStrategy(CtaTemplate):
    """
    信号：5分钟MACD
    过滤：无
    出场：移动止损
    """
    className = 'MACDStrategy'
    author = u'loe'

    # 每笔交易数量
    tradeSize = 1
    # 默认5分钟k线
    barMin = 5
    # RSI窗口数
    rsiWindow = 12
    # RSI入场信号
    rsiLevel = 30
    # 止盈百分比
    stopEarnPecent = 1.0 / 100
    movingEarnPecent = 0.1 / 100
    # 止损点
    stopLossPercent = 1.0 / 100

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'capital',
                 'lever',
                 'perSize',
                 'barMin',
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
        super(MACDStrategy, self).__init__(ctaEngine, setting)

        # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        # k线生成器
        self.barGenerator = BarGenerator(self.onBar, self.barMin, self.onMultiBar)
        # k线管理器，用于生成均线值
        self.arrayManager = ArrayManager(size=90)
        # 记录最新tick，用于判断主力合约是否换月
        self.lastSymbol = ''
        # 开仓价格
        self.enterPrice = EMPTY_FLOAT
        # 开仓后最高价
        self.high = EMPTY_FLOAT
        # 开仓后最低价
        self.low = EMPTY_FLOAT
        # 触发止盈
        self.earningStoping = False
        # macd信号
        self.hist = EMPTY_FLOAT

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
        if self.lastSymbol and self.lastSymbol != tick.vtSymbol:
            # 移仓换月，清仓，这里涉及主力换月后的价格，暂时用新主力价格交易
            if self.pos > 0:
                self.sell(tick.lastPrice - 10, abs(self.pos))
            elif self.pos < 0:
                self.cover(tick.lastPrice + 10, abs(self.pos))

            # 重新初始化
            self.barGenerator = BarGenerator(self.onBar, self.barMin, self.onMultiBar)
            self.arrayManager = ArrayManager(size=90)
            self.lastSymbol = tick.vtSymbol
            return

        if tick.datetime.hour >= 22 and tick.datetime.minute >= 59:
            # 不隔夜，平仓
            if self.pos > 0:
                self.sell(tick.lastPrice - 10, abs(self.pos))
                print tick.datetime, u'不隔夜', tick.lastPrice - self.enterPrice
            elif self.pos < 0:
                self.cover(tick.lastPrice + 10, abs(self.pos))
                print tick.datetime, u'不隔夜', self.enterPrice - tick.lastPrice
            return

        """ 止盈止损 """
        self.high = max(self.high, tick.lastPrice)
        self.low = min(self.low, tick.lastPrice)

        if self.pos > 0:
            if not self.earningStoping and tick.lastPrice >= self.enterPrice * (1 + self.stopEarnPecent):
                # 开启止盈
                self.earningStoping = True

            if self.earningStoping and tick.lastPrice <= self.high * (1 - self.movingEarnPecent):
                # 止盈
                self.sell(tick.lastPrice - 10, abs(self.pos))
                print tick.datetime, u'止盈', tick.lastPrice - self.enterPrice
            elif tick.lastPrice <= self.enterPrice * (1 - self.stopLossPercent):
                # 止损
                self.sell(tick.lastPrice - 10, abs(self.pos))
                print tick.datetime, u'止损', tick.lastPrice - self.enterPrice
        if self.pos < 0:
            if not self.earningStoping and tick.lastPrice <= self.enterPrice * (1 - self.stopEarnPecent):
                # 开启止盈
                self.earningStoping = True

            if self.earningStoping and tick.lastPrice >= self.low * (1 + self.movingEarnPecent):
                # 止盈
                self.cover(tick.lastPrice + 10, abs(self.pos))
                print tick.datetime, u'止盈', self.enterPrice - tick.lastPrice
            elif tick.lastPrice >= self.enterPrice * (1 + self.stopLossPercent):
                # 止损
                self.cover(tick.lastPrice + 10, abs(self.pos))
                print tick.datetime, u'止损', self.enterPrice - tick.lastPrice

        self.barGenerator.updateTick(tick)
        self.lastSymbol = tick.vtSymbol
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

        rsiValue = self.arrayManager.rsi(self.rsiWindow)
        rsiDirection = EMPTY_UNICODE
        if rsiValue > 50 + self.rsiLevel:
            # 空头信号
            rsiDirection = u'空'
        elif rsiValue < 50 - self.rsiLevel:
            # 多头信号
            rsiDirection = u'多'

        macd, signal, hist = self.arrayManager.macd(12, 26, 9)

        if not self.pos:
            if rsiDirection == u'空':
                # 空头信号，开仓空头
                self.short(bar.close - 10, self.tradeSize)
            elif rsiDirection == u'多':
                # 多头信号，开仓多头
                self.buy(bar.close + 10, self.tradeSize)

        # if not self.pos:
        #     if self.hist > 0 and hist < 0 and macd < 0 and signal < 0 and rsiDirection == u'空':
        #         # 空头信号，开仓空头
        #         self.short(bar.close - 10, self.tradeSize)
        #     elif self.hist < 0 and hist > 0 and macd > 0 and signal > 0 and rsiDirection == u'多':
        #         # 多头信号，开仓多头
        #         self.buy(bar.close + 10, self.tradeSize)

        self.hist = hist

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        if trade.offset == u'开仓':
            self.enterPrice = trade.price
            self.high = trade.price
            self.low = trade.price
        else:
            self.high = EMPTY_FLOAT
            self.low = EMPTY_FLOAT
            self.earningStoping = False

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
    engine.initStrategy(MACDStrategy, settingDict)

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
                      '20160506', '20160602', 0,
                      0 / 10000, tickPrice)

    #"""
    # 回测
    engine.strategy.name = 'MACD_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #"""

