# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate, BarGenerator)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME
import numpy as np

class PercentStrategy(CtaTemplate):
    """numpy.percentile()函数计算价格差的制定概率范围，推测下一bar的价格"""
    className = 'PercentStrategy'
    author = u'loe'

    # 开仓信号百分比
    enterPercent = 90
    # bar容器大小
    barListSize = 20
    # 交易大小
    tradeSize = 1
    # 预测持仓盈利点数
    earnValue = 0

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
        super(PercentStrategy, self).__init__(ctaEngine, setting)
        # bar生成器
        self.bg = BarGenerator(self.onBar)
        # bar的容器
        self.highBarList = np.zeros(self.barListSize)
        self.lowBarList = np.zeros(self.barListSize)
        # bar容器装载的数量
        self.listCount = 0
        self.listInit = False
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

        self.bg.updateTick(tick)
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        self.highBarList[0:self.barListSize - 1] = self.highBarList[1:self.barListSize]
        self.lowBarList[0:self.barListSize - 1] = self.lowBarList[1:self.barListSize]
        self.highBarList[-1] = bar.high - bar.open
        self.lowBarList[-1] = bar.open - bar.low

        self.listCount += 1
        if not self.listInit and self.listCount >= self.barListSize:
            self.listInit = True
        if not self.listInit:
            return

        # 计算多头概率值
        highValue = np.percentile(self.highBarList, self.enterPercent)
        upCount = 0
        lowCount = 0
        for i in self.highBarList:
            if i >= highValue:
                upCount += 1
            elif i < highValue:
                lowCount += 1
        bidEnter = False
        if upCount > lowCount:
            # 多头信号出现
            bidEnter = True

        # 计算空头概率值
        lowValue = np.percentile(self.lowBarList, self.enterPercent)
        upCount = 0
        lowCount = 0
        for i in self.lowBarList:
            if i >= lowValue:
                upCount += 1
            elif i < lowValue:
                lowCount += 1
        askEnter = False
        if upCount > lowCount:
            # 多头信号出现
            askEnter = True


        if not self.pos:
            if bidEnter and askEnter:
                if highValue >= lowValue:
                    # 多头买入
                    self.cancelAll()
                    self.buy(bar.close + 10, self.tradeSize)
                    self.earnValue = highValue
                else:
                    # 空头买入
                    self.cancelAll()
                    self.short(bar.close - 10, self.tradeSize)
                    self.earnValue = lowValue
            elif bidEnter:
                # 多头买入
                self.cancelAll()
                self.buy(bar.close + 10, self.tradeSize)
                self.earnValue = highValue

            elif askEnter:
                # 空头买入
                self.cancelAll()
                self.short(bar.close - 10, self.tradeSize)
                self.earnValue = lowValue

        elif self.pos < 0:
            if askEnter:
                # 取消之前的订单，下新的限价单
                self.cancelAll()
                self.earnValue = lowValue
                self.cover(bar.close - self.earnValue, abs(self.pos))
            elif bidEnter:
                # 取消之前的订单， 先清仓再多头开仓
                self.cancelAll()
                self.earnValue = highValue
                self.cover(bar.close + 10, abs(self.pos))
                self.buy(bar.close + 10, self.tradeSize)
        elif self.pos > 0:
            if bidEnter:
                # 取消之前的订单，下新的限价单
                self.cancelAll()
                self.earnValue = highValue
                self.sell(bar.close + self.earnValue, abs(self.pos))
            elif askEnter:
                # 取消之前的订单， 先清仓再多头开仓
                self.cancelAll()
                self.earnValue = lowValue
                self.sell(bar.close - 10, abs(self.pos))
                self.short(bar.close - 10, self.tradeSize)

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""

        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        if trade.offset == u'开仓':
            # 下停止单止盈
            if trade.direction == u'多':
                self.sell(trade.price + self.earnValue, trade.volume)
            elif trade.direction == u'空':
                self.cover(trade.price - self.stopSize, trade.volume)

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
    engine.initStrategy(PercentStrategy, settingDict)

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
                      '20180126', '20180206', 0,
                      0 / 10000, tickPrice)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio】
    setting = OptimizationSetting()                                                     # 新建一个优化任务设置对象
    setting.setOptimizeTarget('totalNetPnl')
    # 设置优化排序的目标是策略夏普比率
    # setting.addParameter('upDegree', 2, 20, 1)               # 优化参数openLen，起始0，结束1，步进1
    start = datetime.now()
    engine.runParallelOptimization(PercentStrategy, setting)
    print datetime.now() - start
    '''

    #"""
    # 回测
    engine.strategy.name = 'percent_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #"""

