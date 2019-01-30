# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time,  timedelta
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate, CtaSignal, BarGenerator, ArrayManager, TargetPosTemplate)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME, DAILY_DB_NAME
import numpy as np
import pymongo


class SeriesBarSignal(CtaSignal):
    """
    开盘前确认前一天龙虎榜持仓排名预测当前交易日方向；
    第一阶段：下单一个单位，止盈stopEarning1，止损stopLose1，价格触及止损，进入第二阶段
    第二阶段：下单一个单位，止盈stopEarning2，均价止损stopLose2，价格触及止损，进入第三阶段
    第三阶段：下单一个单位，止盈stopEarning3，均价止损stopLose3，价格触及止损，结束战斗
    """

    # 当前日期
    date = None
    # 当前交易方向
    direction = 0
    # 建仓价格列表
    priceList = []
    # 建仓均价
    averagePrice = 0
    # 当前合约
    lastSymbol = ''

    # 三个阶段建仓的止盈止损
    stopEarning1 = 0.6 / 100
    stopLose1 = 0.3 / 100

    stopEarning2 = 0.2 / 100
    stopLose2 = 0.3 / 100

    stopEarning3 = 0.1 / 100
    stopLose3 = 0.3 / 100

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(SeriesBarSignal, self).__init__()

        client = pymongo.MongoClient('localhost', 27017)
        self.db = client[DAILY_DB_NAME]

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """Tick更新"""
        self.lastTick = tick

        # 夜盘收盘，清仓
        theTime = tick.datetime.time()
        if theTime > time(22, 59, 56):
            self.setSignalPos(0)
            return

        # 移仓换月
        if not self.lastSymbol or (self.lastSymbol != tick.vtSymbol):
            self.lastSymbol = tick.vtSymbol
            self.setSignalPos(0)
            return

        if not self.date or self.date != tick.datetime.date():
            # 新的一天
            self.date = tick.datetime.date()
            self.direction = 0
            self.priceList = []
            self.averagePrice = 0

            collection = self.db[tick.vtSymbol + '.TB']
            collection.create_index('datetime')
            dateStr = tick.datetime.strftime('%Y%m%d')
            theDate = datetime.strptime(dateStr, '%Y%m%d') - timedelta(1)
            max = 10
            index = 0
            while index < max:
                cursor = collection.find({'datetime':theDate}).sort('datetime')
                has = False
                for dic in cursor:
                    lastBuyChange = dic['buyInterestChange']
                    lastSellChange = dic['sellInterestChange']

                    if lastBuyChange > lastSellChange:
                        self.direction = 1
                    elif lastBuyChange < lastSellChange:
                        self.direction = -1
                    has = True
                if has:
                    break
                theDate = theDate - timedelta(1)
                index += 1

            if self.direction:
                self.through(1)
            else:
                self.setSignalPos(0)

        # 没有建仓不考虑止盈止损
        if not len(self.priceList):
            return

        if self.direction > 0:
            # 多头交易日
            stopEarning = 0
            stopLose = 0
            if len(self.priceList) == 1:
                # 第一阶段
                stopEarning = self.stopEarning1
                stopLose = self.stopLose1

            if len(self.priceList) == 2:
                # 第二阶段
                stopEarning = self.stopEarning2
                stopLose = self.stopLose2

            if len(self.priceList) == 3:
                # 第三阶段
                stopEarning = self.stopEarning3
                stopLose = self.stopLose3

            if tick.lastPrice > self.averagePrice * (1 + stopEarning):
                # 止盈
                self.setSignalPos(0)

            if tick.lastPrice < self.averagePrice * (1 - stopLose):
                # 止损
                if len(self.priceList) == 1:
                    # 第一阶段
                    self.through(2)
                elif len(self.priceList) == 2:
                    # 第二阶段
                    self.through(3)
                else:
                    self.setSignalPos(0)

        elif self.direction < 0:
            # 空头交易日
            stopEarning = 0
            stopLose = 0
            if len(self.priceList) == 1:
                # 第一阶段
                stopEarning = self.stopEarning1
                stopLose = self.stopLose1

            if len(self.priceList) == 2:
                # 第二阶段
                stopEarning = self.stopEarning2
                stopLose = self.stopLose2

            if len(self.priceList) == 3:
                # 第三阶段
                stopEarning = self.stopEarning3
                stopLose = self.stopLose3

            if tick.lastPrice < self.averagePrice * (1 - stopEarning):
                # 止盈
                self.setSignalPos(0)

            if tick.lastPrice > self.averagePrice * (1 + stopLose):
                # 止损
                if len(self.priceList) == 1:
                    # 第一阶段
                    self.through(2)
                elif len(self.priceList) == 2:
                    # 第二阶段
                    self.through(3)
                else:
                    self.setSignalPos(0)
        else:
            self.setSignalPos(0)


    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """K线更新"""
        pass

    def onTrade(self, trade):
        if trade.direction == u'空' and self.direction > 0:
            # 清仓结束
            if trade.price > self.averagePrice:
                self.success()
            else:
                self.failure()

            # 清空建仓价格列表和建仓均价
            self.priceList = []
            self.averagePrice = 0

        if trade.direction == u'多' and self.direction < 0:
            # 清仓结束
            if trade.price < self.averagePrice:
                self.success()
            else:
                self.failure()

            # 清空建仓价格列表和建仓均价
            self.priceList = []
            self.averagePrice = 0


        if ((self.direction > 0) and (trade.direction == u'多')) or (
                (self.direction < 0) and (trade.direction == u'空')):
            # 建仓
            self.priceList.append([trade.price, trade.volume])
            # 计算建仓均价
            totalPrice = 0
            totalVolume = 0
            for tradeInfo in self.priceList:
                totalPrice += tradeInfo[0] * tradeInfo[1]
                totalVolume += tradeInfo[1]

            if totalVolume:
                self.averagePrice = totalPrice / totalVolume

    def through(self, step):
        if not step or step > 3:
            return
        if not self.direction:
            return

        if step == 1:
            # 第一阶段建仓
            self.setSignalPos(self.direction)
        elif step == 2:
            # 第二阶段建仓
            self.setSignalPos(self.direction * 3)
        elif step == 3:
            #第三阶段建仓
            self.setSignalPos(self.direction * 6)

    def success(self):
        self.setSignalPos(0)

    def failure(self):
        self.setSignalPos(0)


class SignalStrategy(TargetPosTemplate):
    """根据每笔tick的一档订单量决定方向下单"""
    className = 'SignalStrategy'
    author = u'loe'

    # 信号
    mainSignal = SeriesBarSignal()
    # 交易价格扩大值
    tickAdd = 10

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
               'targetPos'
               ]

    # 同步列表
    syncList = ['pos'
                ]

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(SignalStrategy, self).__init__(ctaEngine, setting)

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
        self.lastTick = tick

        # 通知信号最新tick
        self.mainSignal.onTick(tick)

        # 设置目标仓位
        self.setTargetPos(self.mainSignal.getSignalPos())
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
        self.mainSignal.onTrade(trade)
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
    engine.initStrategy(SignalStrategy, settingDict)

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
    engine.strategy.name = 'signal_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

