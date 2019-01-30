# coding: utf8

from vnpy.trader.vtObject import VtBarData
from datetime import datetime, time
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate,
                                                     BarGenerator,
                                                     ArrayManager)
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting

class GardenStrategy(CtaTemplate):
    """空中花园"""
    className = 'GardenStrategy'
    author = u'loe'

    # 策略参数
    fixedSize = 1
    limitPercent = 1.0

    # 策略变量
    currentBar = None
    needCleanPositon = False

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'limitPercent']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(GardenStrategy, self).__init__(ctaEngine, setting)

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
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        if not self.currentBar or self.newDateBar(bar):
            self.needCleanPositon = False
            if self.currentBar and self.currentBar.vtSymbol == bar.vtSymbol and (not self.pos):
                if bar.open >= self.currentBar.close * (1 + self.limitPercent / 100.0):
                    # 高开，发第一根bar的最高价停止单
                    self.buy(bar.high, self.fixedSize, True)
                elif bar.open <= self.currentBar.close * (1 - self.limitPercent / 100.0):
                    # 低开，发第一根bar的最低价停止单
                    self.short(bar.low, self.fixedSize, True)

            self.currentBar = VtBarData()
            self.currentBar.vtSymbol = bar.vtSymbol
            self.currentBar.datetime = bar.datetime.replace(hour = 0, minute=0, second=0, microsecond=0)
            self.currentBar.low = bar.low
        else:
            self.currentBar.close = bar.close

        if self.needCleanPositon:
            # 平仓
            self.cancelAll()
            if self.pos > 0:
                self.sell(bar.close - 100, abs(self.pos))
            elif self.pos < 0:
                self.cover(bar.close + 100, abs(self.pos))

    def onNextBar(self, bar):
        """收到下一条Bar推送（提前两个bar提示即将新的一天的，这里进行平仓操作，注意不要写未来函数）"""
        if self.newDateBar(bar):
            # 一天的最后一个bar进行平仓
            self.needCleanPositon = True

    def newDateBar(self, bar):
        if self.currentBar and self.currentBar.datetime.date() != bar.datetime.date() and bar.datetime.time() > time(20):
            # 新的一天
            return True
        else:
            return False

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

#===================================回测==================================
def GetEngin(settingDict, symbol, tickCross, dataBase,
                   startDate, endDate, slippage,
                   rate, size, priceTick, capital):
    """运行单标的回测"""
    # 创建回测引擎实例
    engine = BacktestingEngine()

    # 设置引擎的回测模式为Bar
    engine.setBacktestingMode(engine.BAR_MODE)

    # 是否ticks数据结算bar模式的订单
    engine.tickCross = tickCross

    #设置回测数据来源数据库
    engine.setDatabase(dataBase, symbol)

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
    engine.initStrategy(GardenStrategy, settingDict)

    return  engine

if __name__ == '__main__':
    setting = {'limitPercent':0.2}
    barDbName = 'VnTrader_1Min_Db'
    engine = GetEngin(setting, 'au00.TB', True, barDbName,
                      '20170102', '20180206', 0,
                      0.0 / 10000, 1000, 0.05, 30000)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio 最大回撤maxDrawdown】
    setting = OptimizationSetting()                                                         # 新建一个优化任务设置对象
    setting.setOptimizeTarget('sharpeRatio')                                                # 设置优化排序的目标是策略夏普比率
    setting.addParameter('bollDev', 2.1, 2.2, 0.1)                                         # 优化参数openLen，起始0，结束1，步进1
    setting.addParameter('slMultiplier', 0.7, 0.7, 0.2)                                    # 增加优化参数
    setting.addParameter('window', 23, 23, 1)
    setting.addParameter('amSize', 30, 30, 15)
    start = datetime.now()
    engine.runParallelOptimization(GardenStrategy, setting)
    print datetime.now() - start
    '''

    #'''
    # 回测
    engine.strategy.name = 'garden_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    engine.ShowTradeDetail()
    engine.showDailyResult(df, result)
    #'''

