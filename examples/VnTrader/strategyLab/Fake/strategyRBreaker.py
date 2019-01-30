# coding: utf8

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting

class RBreakerStrategy(CtaTemplate):
    """趋势突破策略"""
    className = 'RBreakerStrategy'
    author = u'loe'

    # 策略参数
    fixedSize = 1                       # 交易数量
    stopPercent = 0.66                  # 移动止损百分比
    stopTradeTime = time(14, 59)        # 强制平仓时间

    setup = 0.35
    enter = 1.07
    bbreak = 0.25

    # 策略变量
    bar = None              # 最新bar

    Bbreak = EMPTY_FLOAT    # 突破买入价
    Sbreak = EMPTY_FLOAT    # 突破卖出价
    Bsetup = EMPTY_FLOAT    # 观察买入价
    Ssetup = EMPTY_FLOAT    # 观察卖出价
    Benter = EMPTY_FLOAT    # 反转买入价
    Senter = EMPTY_FLOAT    # 反转卖出价

    readyForEnterSell = False   # 等待反转卖出
    readyForEnterBuy = False    # 等待反转买入

    hasEntry = False            # 当前周期已经发单建仓
    entryHigh = EMPTY_FLOAT     # 移动最高价
    entryLow = EMPTY_FLOAT      # 移动最低价

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'fixedSize',
                 'stopPercent',
                 'setup',
                 'enter',
                 'bbreak']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'Bbreak',
               'Sbreak',
               'Bsetup',
               'Ssetup',
               'Benter',
               'Senter',
               'readyForEnterSell',
               'readyForEnterBuy',
               'hasEntry',
               'entryHigh',
               'entryLow']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(RBreakerStrategy, self).__init__(ctaEngine, setting)

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
        if not self.bar and tick.datetime.time() < time(21, 0):
            return

        if not self.bar or (self.bar.datetime.date() != tick.datetime.date() and tick.datetime.time() >= time(20, 59)):
            if self.bar:
                if self.bar.vtSymbol != tick.vtSymbol:
                    self.Bbreak = EMPTY_FLOAT  # 突破买入价
                    self.Sbreak = EMPTY_FLOAT  # 突破卖出价
                    self.Bsetup = EMPTY_FLOAT  # 观察买入价
                    self.Ssetup = EMPTY_FLOAT  # 观察卖出价
                    self.Benter = EMPTY_FLOAT  # 反转买入价
                    self.Senter = EMPTY_FLOAT  # 反转卖出价
                else:
                    # 新的交易日计算指标变量
                    '''
                    self.Ssetup = self.bar.high + 0.35 * (self.bar.close - self.bar.low)
                    self.Bsetup = self.bar.low - 0.35 * (self.bar.high - self.bar.close)
                    self.Senter = 1.07 / 2 * (self.bar.high + self.bar.low) - 0.07 * self.bar.low
                    self.Benter = 1.07 / 2 * (self.bar.high + self.bar.low) - 0.07 * self.bar.high
                    self.Bbreak = self.Ssetup + 0.25 * (self.Ssetup - self.Bsetup)
                    self.Sbreak = self.Bsetup - 0.25 * (self.Ssetup - self.Bsetup)
                    '''

                    self.Ssetup = self.bar.high + self.setup * (self.bar.close - self.bar.low)
                    self.Bsetup = self.bar.low - self.setup * (self.bar.high - self.bar.close)
                    self.Senter = self.enter / 2 * (self.bar.high + self.bar.low) - (self.enter - 1) * self.bar.low
                    self.Benter = self.enter / 2 * (self.bar.high + self.bar.low) - (self.enter - 1) * self.bar.high
                    self.Bbreak = self.Ssetup + self.bbreak * (self.Ssetup - self.Bsetup)
                    self.Sbreak = self.Bsetup - self.bbreak * (self.Ssetup - self.Bsetup)

            self.bar = VtBarData()
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.datetime = tick.datetime.replace(hour = 0, minute=0, second=0, microsecond=0)
            self.bar.open = tick.lastPrice
            self.bar.close = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice

            # 新的一天，开始新的建仓
            self.hasEntry = False
            self.readyForEnterSell = False
            self.readyForEnterBuy = False

        self.bar.close = tick.lastPrice
        self.bar.high = max(self.bar.high, tick.lastPrice)
        self.bar.low = min(self.bar.low, tick.lastPrice)

        # 交易===========================================

        # 强制平仓
        if tick.datetime.time() >= self.stopTradeTime and tick.datetime.time() < time(15, 59):
            if self.pos > 0:
                self.sell(tick.lastPrice - 20, abs(self.pos))
            elif self.pos < 0:
                self.cover(tick.lastPrice + 20, abs(self.pos))

            return

        # 当前无仓位，判断趋势建仓
        if self.pos == 0 and not self.hasEntry:
            self.entryHigh = EMPTY_FLOAT
            self.entryLow = EMPTY_FLOAT

            # 指标变量缺失停止建仓
            if (not self.Bbreak) or (not self.Sbreak) or (not self.Bsetup) or (not self.Ssetup) or (not self.Benter) or (not self.Senter):
                return

            if tick.lastPrice >= self.Bbreak:
                # 突破突破买入价
                self.buy(tick.lastPrice + 20, self.fixedSize)
                self.hasEntry =  True
                return
            elif tick.lastPrice <= self.Sbreak:
                # 突破突破卖出价
                self.short(tick.lastPrice - 20, self.fixedSize)
                self.hasEntry = True
                return
            elif tick.lastPrice >= self.Ssetup:
                self.readyForEnterSell = True
            elif tick.lastPrice <= self.Bsetup:
                self.readyForEnterBuy = True

            if self.readyForEnterSell and tick.lastPrice <= self.Senter:
                # 反转卖出
                '''
                self.short(tick.lastPrice - 20, self.fixedSize)
                self.hasEntry = True
                '''
                return

            if self.readyForEnterBuy and tick.lastPrice >= self.Benter:
                # 反转买入
                '''
                self.buy(tick.lastPrice + 20, self.fixedSize)
                self.hasEntry = True
                '''
                return

        # 持有多头仓位
        elif self.pos > 0:
            if self.entryHigh and tick.lastPrice <= (self.entryHigh * (1 - self.stopPercent/100)):
                #self.sell(tick.lastPrice - 20, abs(self.pos))
                pass

            self.entryHigh = max(self.entryHigh, tick.lastPrice)

        # 持有空头仓位
        elif self.pos < 0:
            if self.entryLow and tick.lastPrice >= (self.entryLow * (1 + self.stopPercent/100)):
                #self.cover(tick.lastPrice + 20, abs(self.pos))
                pass

            if self.entryLow:
                self.entryLow =  min(self.entryLow, tick.lastPrice)
            else:
                self.entryLow = tick.lastPrice

        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        pass

    def onNextBar(self, bar):
        """收到下一条Bar推送（用于回测，判断下下个bar主力合约换月，平仓当前所有头寸）"""
        pass

    def onXminBar(self, bar):
        """收到X分钟K线"""
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
        if trade.offset == u'开仓':
            # 开仓价初始化移动最高最低价
            self.entryHigh = trade.price
            self.entryLow = trade.price

        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

#===================================回测==================================
def GetEngin(settingDict, symbol,
                   startDate, endDate, slippage,
                   rate, size, priceTick, capital):
    """运行单标的回测"""
    # 创建回测引擎实例
    engine = BacktestingEngine()

    # 设置引擎的回测模式为Tick
    engine.setBacktestingMode(engine.TICK_MODE)

    #设置回测数据来源数据库
    engine.setDatabase(TICK_DB_NAME, symbol)

    # 设置回测的起始日期
    engine.setStartDate(startDate, initDays=0)

    # 设置回测的截至日期
    engine.setEndDate(endDate)

    # 滑点设置
    engine.setSlippage(slippage)

    # 合约交易手续费
    engine.setRate(rate)

    # 合约每手数量
    engine.setSize(size)

    # 合约最小价格变动
    engine.setPriceTick(priceTick)

    # 起始资金
    engine.setCapital(capital)

    # 引擎中创建策略对象
    engine.initStrategy(RBreakerStrategy, settingDict)

    return  engine

if __name__ == '__main__':
    stopPercent = 0.66

    setup = 0.35
    enter = 1.07
    bbreak = 0.25

    setting = {'stopPercent':stopPercent,
               'setup':setup,
               'enter':enter,
               'bbreak':bbreak}
    engine = GetEngin(setting, 'au00.TB',
                      '20170102', '20180206', 0,
                      0.1 / 10000, 1000, 0.05, 30000)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio 最大回撤maxDrawdown】
    setting = OptimizationSetting()                                                         # 新建一个优化任务设置对象
    setting.setOptimizeTarget('sharpeRatio')                                                # 设置优化排序的目标是策略夏普比率
    #setting.addParameter('stopPercent', 0.26, 1.0, 0.1)                                     # 优化参数
    setting.addParameter('setup', 0.15, 0.65, 0.1)
    setting.addParameter('enter', 0.87, 1.57, 0.1)
    #setting.addParameter('bbreak', 0.15, 0.55, 0.1)
    start = datetime.now()
    engine.runParallelOptimization(RBreakerStrategy, setting)
    print datetime.now() - start
    '''

    #'''
    # 回测
    engine.strategy.name = 'rbreaker_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

