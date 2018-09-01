# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME

class VolumeStrategy(CtaTemplate):
    """根据每笔tick的一档订单量决定方向下单"""
    className = 'VolumeStrategy'
    author = u'loe'

    # 多空订单量相差倍数
    space = 200
    # 交易方向挂单的最低量
    minVolume = 20
    # 止损百分比
    stopPercent = 3.0 / 100
    # 持仓价和持仓后的最高最低价
    enterPrice = 0
    highPrice = 0
    lowPrice = 0
    # 累计盈亏
    totalEarning = 0
    # 合约代码
    lastSymbol = ''

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'capital',
                 'lever',
                 'perSize'
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
        super(VolumeStrategy, self).__init__(ctaEngine, setting)
        # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)
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
        self.cancelAll()



        # if  (time(15, 20) > tick.datetime.time() > time(14, 59, 56)):
        #     if self.pos > 0:
        #         self.sell(tick.lastPrice - 100, abs(self.pos))
        #     elif self.pos < 0:
        #         self.cover(tick.lastPrice + 100, abs(self.pos))
        #     return

        # if tick.datetime.time() > time(22, 59, 56):
        #     if self.pos > 0:
        #         self.sell(tick.lastPrice - 100, abs(self.pos))
        #     elif self.pos < 0:
        #         self.cover(tick.lastPrice + 100, abs(self.pos))
        #     return

        symbol = tick.vtSymbol
        if not self.lastSymbol:
            self.lastSymbol = symbol

        if self.lastSymbol != symbol:
            if self.pos > 0:
                self.sell(tick.lastPrice - 100, abs(self.pos))
            elif self.pos < 0:
                self.cover(tick.lastPrice + 100, abs(self.pos))

            print u"###### 移仓换月 %s %s %s######" % (tick.datetime, self.lastSymbol, symbol)
            self.lastSymbol = symbol
            return

        # 止损
        self.highPrice = max(self.highPrice, tick.lastPrice)
        self.lowPrice = min(self.lowPrice, tick.lastPrice)
        if self.pos > 0 and tick.lastPrice <= self.highPrice * (1 - self.stopPercent):
            self.sell(tick.lastPrice - 100, abs(self.pos))
            datetime = tick.datetime.replace(microsecond = 0)
            earning = (tick.lastPrice - self.enterPrice) * self.perSize
            self.totalEarning += earning
            print u"====== %s %s 止损 多 %s  %s 盈亏 %s====== %s" % (datetime, symbol, self.enterPrice, tick.lastPrice, earning, self.totalEarning)

        if self.pos < 0 and tick.lastPrice >= self.lowPrice * (1 + self.stopPercent):
            self.cover(tick.lastPrice + 100, abs(self.pos))
            datetime = tick.datetime.replace(microsecond=0)
            earning = (tick.lastPrice - self.enterPrice) * -1 * self.perSize
            self.totalEarning += earning
            print u"====== %s %s 止损 空 %s  %s 盈亏 %s====== %s" % (datetime, symbol, self.enterPrice, tick.lastPrice, earning, self.totalEarning)

        bidVolume = tick.bidVolume1
        askVolume = tick.askVolume1
        if bidVolume > askVolume * self.space and askVolume >= self.minVolume:
            if self.pos < 0:
                self.cover(tick.lastPrice + 100, abs(self.pos))
                datetime = tick.datetime.replace(microsecond=0)
                earning = (tick.lastPrice - self.enterPrice) * -1 * self.perSize
                self.totalEarning += earning
                print u"====== %s %s 空 %s  %s 盈亏 %s====== %s" % (datetime, symbol, self.enterPrice, tick.lastPrice, earning, self.totalEarning)
                self.buy(tick.lastPrice + 100, 1)
            elif self.pos == 0:
                self.buy(tick.lastPrice + 100, 1)
        elif askVolume > bidVolume * self.space and bidVolume >= self.minVolume:
            if self.pos > 0:
                self.sell(tick.lastPrice - 100, abs(self.pos))
                datetime = tick.datetime.replace(microsecond=0)
                earning = (tick.lastPrice - self.enterPrice) * self.perSize
                self.totalEarning += earning
                print u"====== %s %s 多 %s  %s 盈亏 %s====== %s" % (datetime, symbol, self.enterPrice, tick.lastPrice, earning, self.totalEarning)
                self.short(tick.lastPrice - 100, 1)
            elif self.pos == 0:
                self.short(tick.lastPrice - 100, 1)

        # 发出状态更新事件
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
        if trade.offset == u'开仓':
            self.enterPrice = trade.price
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
    engine.initStrategy(VolumeStrategy, settingDict)

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
                      '20170101', '20180206', 0,
                      0 / 10000, tickPrice)


    #'''
    # 回测
    engine.strategy.name = 'volume_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

