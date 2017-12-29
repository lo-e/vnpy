# coding: utf8

from datetime import datetime
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate,
                                                     BarManager,
                                                     ArrayManager)

class SimpleStrategy(CtaTemplate):
    """双指数均线策略Demo"""
    className = 'SimpleStrategy'
    author = 'loe'

    # 策略参数
    earnCount = 6 # 止盈点数
    lossCount = 6 # 止损点数
    posCount = 1 # 仓位控制
    initDirection = u'多' # 初始头寸方向
    initDays = 1

    # 策略变量
    lastDirection = EMPTY_UNICODE # 上次头寸方向
    currentDirection = EMPTY_UNICODE # 当前头寸方向
    posPrice = EMPTY_FLOAT # 当前头寸价格

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'earnCount',
                 'lossCount']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'lastDirection',
               'currentDirection',
               'posPrice']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos',
                'posPrice']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(SimpleStrategy, self).__init__(ctaEngine, setting)

    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s演示策略初始化' % self.name)

        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadBar(self.initDays)
        for bar in initData:
            self.onBar(bar)

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'simple演示策略启动')
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'simple演示策略停止')
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        if not self.trading:
            return

        # 取消所有未成交的委托
        self.cancelAll()

        if self.pos > 0:
            # 持有多头
            self.currentDirection = u'多'

            if tick.lastPrice >= (self.posPrice + self.earnCount):
                # 止盈
                self.sell(tick.lastPrice-6.0, abs(self.pos))
                self.writeCtaLog(u'止盈委托%.2f' % tick.lastPrice)
            if tick.lastPrice <= (self.posPrice - self.lossCount):
                # 止损
                self.sell(tick.lastPrice-6.0, abs(self.pos))
                self.writeCtaLog(u'止损委托%.2f' % tick.lastPrice)
        elif self.pos < 0:
            #持有空头
            self.currentDirection = u'空'

            if tick.lastPrice <= (self.posPrice - self.earnCount):
                # 止盈
                self.cover(tick.lastPrice+6.0, abs(self.pos))
                self.writeCtaLog(u'止盈委托%.2f' % tick.lastPrice)
            if tick.lastPrice >= (self.posPrice + self.lossCount):
                # 止损
                self.cover(tick.lastPrice+6.0, abs(self.pos))
                self.writeCtaLog(u'止损委托%.2f' % tick.lastPrice)
        else:
            #未持仓
            if len(self.currentDirection):
                self.lastDirection = self.currentDirection
                self.currentDirection = EMPTY_UNICODE

            toDirection = self.initDirection
            if self.lastDirection == u'多':
                toDirection = u'空'
            elif self.lastDirection == u'空':
                toDirection = u'多'

            if toDirection == u'多':
                self.buy(tick.lastPrice + 6.0, self.posCount)
                self.writeCtaLog(u'多头开仓委托%.2f' % tick.lastPrice)
            elif toDirection == u'空':
                self.short(tick.lastPrice - 6.0, self.posCount)
                self.writeCtaLog(u'多头空仓委托%.2f' % tick.lastPrice)
            else:
                # 默认做多
                self.buy(tick.lastPrice + 6.0, self.posCount)
                self.writeCtaLog(u'【默认】多头开仓委托%.2f' % tick.lastPrice)

        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        # 发出状态更新事件
        self.writeCtaLog(u'barDatetime:%s close:%.2f' % (datetime.strftime(bar.datetime, '%Y-%m-%d %H-%M-%S.%f'), bar.close))
        self.putEvent()

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onTrade
        if trade.offset == u'开仓':
            self.posPrice = trade.price
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass