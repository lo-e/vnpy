# coding: utf8

#from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
import numpy as np
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME

class TestStrategy(CtaTemplate):
    """"""
    className = 'TestStrategy'
    author = u'loe'

    stopPercent = 0.2 / 100

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'EntryPriceList',
               'high',
               'low',
               'averagePrice']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(TestStrategy, self).__init__(ctaEngine, setting)
        self.EntryPriceList = []
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.averagePrice = EMPTY_FLOAT
        self.openDirection = 'bid'

    # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' % self.name)
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' % self.name)

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        if not self.trading:
            return

        # 发出状态更新事件
        self.cancelAll()

        if tick.datetime.hour >= 9 and tick.datetime.second >= 1:
            # 开盘
            if not self.pos:
                # 开仓
                if self.openDirection == 'bid':
                    self.buy(tick.lastPrice + 10, 1)
                elif self.openDirection == 'ask':
                    self.short(tick.lastPrice - 10, 1)
            elif self.pos > 0:
                # 多头持仓
                if tick.lastPrice <= self.high * ( 1 - self.stopPercent):
                    if tick.lastPrice > self.averagePrice:
                        # 盈利了离场
                        self.sell(tick.lastPrice - 10, abs(self.pos))
                        self.openDirection = 'ask'
                    elif abs(self.pos) >= 1:
                        # 亏损了离场
                        self.sell(tick.lastPrice - 10, abs(self.pos))
                        self.openDirection = 'ask'
                    else:
                        # 双倍加仓
                        self.buy(tick.lastPrice + 10, abs(self.pos) * 2)
            else:
                # 空头持仓
                if tick.lastPrice >= self.low * ( 1 + self.stopPercent):
                    if tick.lastPrice < self.averagePrice:
                        # 盈利了离场
                        self.cover(tick.lastPrice + 10, abs(self.pos))
                        self.openDirection = 'bid'
                    elif abs(self.pos) >= 1:
                        # 亏损了离场
                        self.cover(tick.lastPrice + 10, abs(self.pos))
                        self.openDirection = 'bid'
                    else:
                        # 双倍加仓
                        self.short(tick.lastPrice - 10, abs(self.pos) * 2)

        self.high = max(self.high, tick.lastPrice)
        self.low = min(self.low, tick.lastPrice)

        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 发出状态更新事件
        if trade.offset == u'开仓':
            tradeInfo = [trade.price,trade.volume]
            self.EntryPriceList.append(tradeInfo)
            totalPrice = EMPTY_FLOAT
            totalVolume = EMPTY_FLOAT
            for tradeInfo in self.EntryPriceList:
                totalPrice += tradeInfo[0] * tradeInfo[1]
                totalVolume += tradeInfo[1]
            self.high = totalPrice / totalVolume
            self.low = totalPrice / totalVolume
            self.averagePrice = self.high
        else:
            self.EntryPriceList = []
            self.high = EMPTY_FLOAT
            self.low = EMPTY_FLOAT
            self.averagePrice = EMPTY_FLOAT

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
    engine.initStrategy(TestStrategy, settingDict)

    # 起始资金
    engine.setCapital(engine.strategy.capital)

    # 合约每手数量
    engine.setSize(engine.strategy.perSize)

    return  engine

if __name__ == '__main__':
    tickPrice = 1
    setting = {'capital':6000,
               'lever':5,
               'perSize':10}

    engine = GetEngin(setting, 'rb00.TB',
                      '20170806', '20180206', 0,
                      1.07 / 10000, tickPrice)


    #'''
    # 回测
    engine.strategy.name = 'Test_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

