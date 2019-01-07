# coding: utf8

from vnpy.trader.app.ctaStrategy.stgEarningManager import stgEarningManager
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
import collections
import threading
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME

class SimpleStrategy(CtaTemplate):
    """根据夜盘收盘前的价格趋势开仓入场，移动百分比离场"""
    className = 'SimpleStrategy'
    author = u'loe'
    earningManager = None

    # 策略参数
    startSecond = 23
    startTime = None  # 趋势判断开始时间
    endTime = None  # 趋势判断截至时间
    stopTime = time(22, 59, 59, 500000)  # 建仓截止时间
    openPercent = 0.14  # 开仓趋势过滤
    outPercent = 0.16 # 移动止盈止损百分比
    tickPrice = 1 # 合约价格最小波动

    # 策略变量
    tradeSize = 1  # 交易数量
    startPrice = EMPTY_FLOAT # 趋势判断开始价格
    endPrice = EMPTY_FLOAT # 趋势判断截至价格
    highPrice = EMPTY_FLOAT # 持仓后的最高价，为了多头止盈止损的计算
    lowPrice = EMPTY_FLOAT # 持仓后的最低价， 为了空头止盈止损的计算
    todayDate = EMPTY_STRING  # 当前日期
    todayEntry = False  # 当天是否已经交易
    ordering = False  # 有未成交的委托

    entryOrderPrice = EMPTY_FLOAT # 开仓委托价
    offsetOrderPrice = EMPTY_FLOAT  # 平仓委托价
    entryPrice = EMPTY_FLOAT # 开仓价

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'capital',
                 'lever',
                 'perSize',
                 'startSecond',
                 'startTime',
                 'endTime',
                 'openPercent',
                 'outPercent',
                 'tickPrice']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'tradeSize',
               'todayDate',
               'todayEntry',
               'startPrice',
               'endPrice',
               'highPrice',
               'lowPrice',
               'entryPrice',
               'entryOrderPrice',
               'offsetOrderPrice',
               'ordering']

    # 同步列表
    syncList = ['pos',
                'todayDate',
                'todayEntry',
                'startPrice',
                'endPrice',
                'highPrice',
                'lowPrice',
                'entryPrice',
                'entryOrderPrice',
                'ordering']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(SimpleStrategy, self).__init__(ctaEngine, setting)
        self.startTime = time(22, 58, self.startSecond)  # 趋势判断开始时间
        self.endTime = time(22, 59, 56)  # 趋势判断截至时间
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
        if tick.datetime.time() >= self.stopTime and not self.pos:
            # 超过建仓截止时间，取消所有委托
            self.cancelAll()
            return

        #self.tradeSize = 1
        self.tradeSize = self.getMaxTradeVolumeWithLever(tick.lastPrice)

        if (not self.todayDate) or (datetime.strptime(self.todayDate, '%Y-%m-%d').date() != tick.datetime.date()):
            # 撤销未成交的单
            self.cancelAll()
            self.ordering = False
            # 早盘第一个tick收到后信号初始化
            self.todayDate = tick.datetime.strftime('%Y-%m-%d')
            self.todayEntry = False
            self.startPrice = EMPTY_FLOAT
            self.endPrice = EMPTY_FLOAT

        if self.pos == 0:
            self.highPrice = EMPTY_FLOAT
            self.lowPrice = EMPTY_FLOAT
            self.entryPrice = EMPTY_FLOAT
            self.entryOrderPrice = EMPTY_FLOAT
            self.offsetOrderPrice = EMPTY_FLOAT

            # 当前仓位为空才会做新的开仓信号判断
            if (self.startPrice == 0) and (tick.datetime.time() >= self.startTime) and (tick.datetime.time() < self.endTime):
                self.startPrice = tick.lastPrice

            if (self.endPrice == 0) and (tick.datetime.time() >= self.endTime) and (tick.datetime.time() < self.stopTime):
                self.endPrice = tick.lastPrice

            if self.startPrice and self.endPrice and (not self.ordering) and (not self.todayEntry):
                sub = self.endPrice - self.startPrice
                if (sub >= self.startPrice * self.openPercent / 100) and self.trading:
                    # 开仓多头
                    self.buy(tick.lastPrice + self.tickPrice*10, self.tradeSize)  # 限价单
                    self.entryOrderPrice = tick.lastPrice
                    self.ordering = True
                    self.writeCtaLog(u'开仓多头 %s' % tick.time)

                    # print u'开仓 多 %s bid:%s ask:%s' % (self.tradeSize, tick.bidVolume1, tick.askVolume1)
                elif (sub <= -self.startPrice * self.openPercent / 100) and self.trading:
                    # 开仓空头
                    self.short(tick.lastPrice - self.tickPrice*10, self.tradeSize)  # 限价单
                    self.entryOrderPrice = tick.lastPrice
                    self.ordering = True
                    self.writeCtaLog(u'开仓空头 %s' % tick.time)

                    # print u'开仓 空 %s bid:%s ask:%s' % (self.tradeSize, tick.bidVolume1, tick.askVolume1)

        elif self.pos > 0:
            # 持有多头仓位
            if (tick.lastPrice <= self.highPrice * (1 - self.outPercent / 100)) and ( not self.ordering) and self.trading:
                # 止盈止损
                self.sell(tick.lastPrice - self.tickPrice*20, abs(self.pos))  # 限价单
                self.offsetOrderPrice = tick.lastPrice
                self.ordering = True

                # print u'平仓 空 %s bid:%s ask:%s' % (self.tradeSize, tick.bidVolume1, tick.askVolume1)
        elif self.pos < 0:
            # 持有空头仓位
            if (tick.lastPrice >= self.lowPrice * (1 + self.outPercent / 100)) and ( not self.ordering) and self.trading:
                # 止盈止损
                self.cover(tick.lastPrice + self.tickPrice*20, abs(self.pos))  # 限价单
                self.offsetOrderPrice = tick.lastPrice
                self.ordering = True

                # print u'平仓 多 %s bid:%s ask:%s' % (self.tradeSize, tick.bidVolume1, tick.askVolume1)

        if self.pos:
            self.highPrice = max(self.highPrice, tick.lastPrice)
            self.lowPrice = min(self.lowPrice, tick.lastPrice)

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
        self.ordering = False
        if trade.offset == u'开仓':
            self.todayEntry = True
            self.highPrice = trade.price
            self.lowPrice = trade.price
            self.entryPrice = trade.price

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
    engine.initStrategy(SimpleStrategy, settingDict)

    # 起始资金
    engine.setCapital(engine.strategy.capital)

    # 合约每手数量
    engine.setSize(engine.strategy.perSize)

    return  engine

if __name__ == '__main__':
    tickPrice = 1
    setting = {'capital':1000000,
               'lever':10,
               'perSize':10}

    #setting = {'openLen': 4*tickPrice, 'tickPrice': tickPrice}
    #setting = {'outPercent':0.14}
    #setting = {'openPercent': 0.14}
    setting['startSecond'] = 23
    engine = GetEngin(setting, 'rb00.TB',
                      '20160506', '20180906', 0,
                      1.07 / 10000, tickPrice)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio】
    setting = OptimizationSetting()                                                     # 新建一个优化任务设置对象
    setting.setOptimizeTarget('totalNetPnl')                                            # 设置优化排序的目标是策略夏普比率
    #setting.addParameter('openLen', 1*tickPrice, 6*tickPrice, tickPrice)               # 优化参数openLen，起始0，结束1，步进1
    #setting.addParameter('outPercent', 0.15, 0.17, 0.01)                               # 增加优化参数
    #setting.addParameter('openPercent', 0.15, 0.16, 0.01)
    setting.addParameter('startSecond', 21, 26, 1)
    start = datetime.now()
    engine.runParallelOptimization(SimpleStrategy, setting)
    print datetime.now() - start
    '''


    #'''
    # 回测
    engine.strategy.name = 'simple_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

