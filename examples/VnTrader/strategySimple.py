# coding: utf8

from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)

class SimpleStrategy(CtaTemplate):
    """根据夜盘收盘前的价格趋势开仓入场，移动百分比离场"""
    className = 'SimpleStrategy'
    author = u'loe'

    # 策略参数
    tradeSize = 1 # 交易数量
    startTime = time(22, 58, 26) # 趋势判断开始时间
    endTime =  time(22, 59, 56) # 趋势判断截至时间
    todayDate = EMPTY_STRING # 当前日期
    todayEntry = False # 当天是否已经交易
    outPercent = 0.1 # 移动止盈止损百分比

    '''
    tradeSize = 1  # 交易数量
    startTime = time(22, 39, 0)  # 趋势判断开始时间
    endTime = time(22, 40, 0)  # 趋势判断截至时间
    todayDate = EMPTY_STRING  # 当前日期
    todayEntry = False  # 当天是否已经产生信号
    outPercent = 0.1  # 移动止盈止损百分比
    '''


    # 策略变量
    startPrice = EMPTY_FLOAT # 趋势判断开始价格
    endPrice = EMPTY_FLOAT # 趋势判断截至价格
    highPrice = EMPTY_FLOAT # 持仓后的最高价，为了多头止盈止损的计算
    lowPrice = EMPTY_FLOAT # 持仓后的最低价， 为了空头止盈止损的计算

    entryPrice = EMPTY_FLOAT # 开仓价
    outPrice = EMPTY_FLOAT # 平仓价
    offsetEarning = EMPTY_FLOAT # 开平仓盈亏

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'tradeSize',
                 'startTime',
                 'endTime',
                 'outPercent']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'todayDate',
               'todayEntry',
               'startPrice',
               'endPrice',
               'highPrice',
               'lowPrice']

    # 同步列表
    syncList = ['pos',
                'todayDate',
                'todayEntry',
                'startPrice',
                'endPrice',
                'highPrice',
                'lowPrice',
                'entryPrice',
                'outPrice',
                'offsetEarning']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(SimpleStrategy, self).__init__(ctaEngine, setting)

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
        # 撤销未成交的单
        self.cancelAll()

        if (not self.todayDate) or (datetime.strptime(self.todayDate, '%Y-%m-%d').date() != tick.datetime.date()):
            # 早盘第一个tick收到后信号初始化
            self.todayDate = tick.datetime.strftime('%Y-%m-%d')
            self.todayEntry = False
            self.startPrice = EMPTY_FLOAT
            self.endPrice = EMPTY_FLOAT

        if self.pos == 0:
            self.highPrice = EMPTY_FLOAT
            self.lowPrice = EMPTY_FLOAT
            # 当前仓位为空才会做新的开仓信号判断
            if (self.startPrice == 0) and (tick.datetime.time() >= self.startTime) and (tick.datetime.time() < self.endTime):
                self.startPrice = tick.lastPrice

            if (self.endPrice == 0) and (tick.datetime.time() >= self.endTime):
                self.endPrice = tick.lastPrice

            if self.startPrice and self.endPrice and (not self.todayEntry):
                sub = self.endPrice - self.startPrice
                if sub > 0:
                    # 开仓多头
                    self.buy(tick.lastPrice + 10, self.tradeSize)  # 限价单
                elif sub < 0:
                    # 开仓空头
                    self.short(tick.lastPrice - 10, self.tradeSize)  # 限价单
        elif self.pos > 0:
            # 持有多头仓位
            if tick.lastPrice <= self.highPrice * (1 - self.outPercent / 100):
                # 止盈止损
                self.sell(tick.lastPrice - 10, abs(self.pos))  # 限价单
        elif self.pos < 0:
            # 持有空头仓位
            if tick.lastPrice >= self.lowPrice * (1 + self.outPercent / 100):
                # 止盈止损
                self.cover(tick.lastPrice + 10, abs(self.pos))  # 限价单

        if self.pos:
            self.highPrice = max(self.highPrice, tick.lastPrice)
            self.lowPrice = min(self.lowPrice, tick.lastPrice)

        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 发出状态更新事件
        self.putEvent()
        if trade.offset == u'开仓':
            self.todayEntry = True
            self.highPrice = trade.price
            self.lowPrice = trade.price

            self.entryPrice = trade.price
            self.outPrice = EMPTY_FLOAT
        elif trade.offset == u'平仓':
            self.outPrice = trade.price
            sub = self.outPrice - self.entryPrice

            if trade.direction == u'多':
                self.offsetEarning -= sub
            else:
                self.offsetEarning += sub

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

    def saveDailyEarningToCSV(self):
        fileName = 'clone\\abc.csv'