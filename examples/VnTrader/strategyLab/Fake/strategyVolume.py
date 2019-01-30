# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME
import numpy as np

class VolumeStrategy(CtaTemplate):
    """根据每笔tick的一档订单量决定方向下单"""
    className = 'VolumeStrategy'
    author = u'loe'

    # tick容器数量
    listSize = 17
    # 判断信号的买卖量相差倍数
    upDegree = 5
    # 开仓后考虑止盈点数，若判断信号仍然有效，继续持有
    overSize = 3
    # 开仓后停止单反向点数
    stopSize = 2
    # 交易数量
    tradeSize = 1

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'capital',
                 'lever',
                 'perSize',
                 'upDegree',
                 'listSize'
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

        self.listCount = 0
        self.listInit = False
        self.bidPrice = 0
        self.askPrice = 0
        self.bidVolumeList = np.zeros(self.listSize)
        self.askVolumeList = np.zeros(self.listSize)
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

        # 数据加载
        self.bidVolumeList[0:self.listSize - 1] = self.bidVolumeList[1:self.listSize]
        self.askVolumeList[0:self.listSize - 1] = self.askVolumeList[1:self.listSize]
        self.bidVolumeList[-1] = tick.bidVolume1
        self.askVolumeList[-1] = tick.askVolume1

        self.listCount += 1
        if not self.listInit and self.listCount >= self.listSize:
            self.listInit = True

        # 信号分析
        if self.listInit:
            bidSum = self.bidVolumeList.sum()
            askSum = self.askVolumeList.sum()

            if self.pos == 0:
                # 空仓中，多头列表之和大于空头列表之和，建仓多头；否则反之。
                if bidSum > askSum * self.upDegree:
                    self.cancelAll()
                    self.buy(tick.lastPrice + 10, self.tradeSize)
                elif bidSum < askSum * self.upDegree:
                    self.cancelAll()
                    self.short(tick.lastPrice - 10, self.tradeSize)
            elif self.pos > 0:
                # 多头持仓中，当前价格正向超出持仓价设置的固定点数，判断信号趋势，如果仍然符合，继续持仓，并且撤销之前的停止单，以最新价格开新的停止单，若不符合，清仓。
                if tick.lastPrice >= self.bidPrice + self.overSize:
                    if bidSum <= askSum * self.upDegree:
                        self.cancelAll()
                        self.sell(tick.lastPrice - 10, abs(self.pos))
                    else:
                        self.cancelAll()
                        self.sell(tick.lastPrice - self.stopSize, abs(self.pos), True)
            elif self.pos < 0:
                # 空头持仓中，同上。
                if tick.lastPrice <= self.askPrice - self.overSize:
                    if askSum <= bidSum * self.upDegree:
                        self.cancelAll()
                        self.cover(tick.lastPrice + 10, abs(self.pos))
                    else:
                        self.cancelAll()
                        self.cover(tick.lastPrice + self.stopSize, abs(self.pos), True)


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
        if abs(trade.volume) > 100:
            print ''
        if trade.offset == u'开仓':
            if trade.direction == u'多':
                # 反向停止单
                self.sell(trade.price - self.stopSize, trade.volume, True)
                # 多头建仓价格
                self.bidPrice = trade.price
            elif trade.direction == u'空':
                # 反向停止单
                self.cover(trade.price + self.stopSize, trade.volume, True)
                # 空头建仓价格
                self.askPrice = trade.price

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
                      '20180126', '20180206', 0,
                      0 / 10000, tickPrice)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio】
    setting = OptimizationSetting()                                                     # 新建一个优化任务设置对象
    setting.setOptimizeTarget('totalNetPnl')
    # 设置优化排序的目标是策略夏普比率
    setting.addParameter('upDegree', 2, 20, 1)               # 优化参数openLen，起始0，结束1，步进1
    setting.addParameter('listSize', 10, 100, 1)                                  # 增加优化参数
    start = datetime.now()
    engine.runParallelOptimization(VolumeStrategy, setting)
    print datetime.now() - start
    '''

    #"""
    # 回测
    engine.strategy.name = 'volume_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #"""

