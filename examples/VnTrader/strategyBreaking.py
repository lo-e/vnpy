# coding: utf8

from datetime import datetime
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate,
                                                     BarGenerator,
                                                     ArrayManager)
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting

class BreakingStrategy(CtaTemplate):
    """趋势突破策略"""
    className = 'BreakingStrategy'
    author = u'loe'

    # 策略参数
    kLine = 28  # 几分钟k线
    amSize = 29 # 容量
    window = 17 # 回望窗口数

    bollDev = 2.4  # 布林通道的偏差
    slMultiplier = 1.1  # 计算止损距离的乘数
    fixedSize = 1  # 每次交易的数量

    # 策略变量
    currentBar = None
    currentSymbol = EMPTY_STRING
    needCleanPositon = False # 主力换月时清仓

    bollUp = 0  # 布林通道上轨
    bollDown = 0  # 布林通道下轨
    cciValue = 0  # CCI指标数值
    atrValue = 0  # ATR指标数值

    intraTradeHigh = 0  # 持仓期内的最高点
    intraTradeLow = 0  # 持仓期内的最低点
    longStop = 0  # 多头止损
    shortStop = 0  # 空头止损

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'kLine',
                 'amSize',
                 'window',
                 'bollWindow',
                 'cciWindow',
                 'atrWindow',
                 'bollDev',
                 'slMultiplier',
                 'fixedSize']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'bollUp',
               'bollDown',
               'cciValue',
               'atrValue',
               'intraTradeHigh',
               'intraTradeLow',
               'longStop',
               'shortStop']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['pos',
                'intraTradeHigh',
                'intraTradeLow']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(BreakingStrategy, self).__init__(ctaEngine, setting)

        self.bollWindow = self.window  # 布林通道窗口数
        self.cciWindow = self.window  # CCI窗口数
        self.atrWindow = self.window  # ATR窗口数

        self.bm = BarGenerator(self.onBar, self.kLine, self.onXminBar)  # 创建K线合成器对象
        self.am = ArrayManager(size = self.amSize)

    # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)
        # 载入历史数据，并采用回放计算的方式初始化策略数值
        initData = self.loadTick(0)
        for bar in initData:
            self.onBar(bar)

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
        self.bm.updateTick(tick)
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        self.currentBar = bar
        self.onXminBar(bar)

    def onNextBar(self, bar):
        """收到下一条Bar推送（用于回测，判断下下个bar主力合约换月，平仓当前所有头寸）"""
        if self.currentSymbol and self.currentSymbol != bar.vtSymbol:
            self.needCleanPositon = True

    def onXminBar(self, bar):
        """收到X分钟K线"""
        # 全撤之前发出的委托
        self.cancelAll()

        # 主力换月，重新初始化
        if not self.currentSymbol or self.currentSymbol != bar.vtSymbol:
            self.am = ArrayManager(size = self.amSize)
            self.currentSymbol = bar.vtSymbol
            self.needCleanPositon = False

        if self.needCleanPositon:
            # 主力合约换月，平仓当前头寸
            if self.pos > 0:
                self.cancelAll()
                self.sell(self.currentBar.close - 100, abs(self.pos))
            elif self.pos < 0:
                self.cancelAll()
                self.cover(self.currentBar.close + 100, abs(self.pos))
            return

        # 保存K线数据
        am = self.am
        am.updateBar(bar)

        if not am.inited:
            return

        # 计算指标数值
        self.bollUp, self.bollDown = am.boll(self.bollWindow, self.bollDev)
        self.cciValue = am.cci(self.cciWindow)
        self.atrValue = am.atr(self.atrWindow)

        # 判断是否要进行交易
        # 当前无仓位，发送开仓委托
        if self.pos == 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = bar.low

            if self.cciValue > 0:
                self.buy(self.bollUp, self.fixedSize, True)

            elif self.cciValue < 0:
                self.short(self.bollDown, self.fixedSize, True)

        # 持有多头仓位
        elif self.pos > 0:
            self.intraTradeHigh = max(self.intraTradeHigh, bar.high)
            self.intraTradeLow = bar.low
            self.longStop = self.intraTradeHigh - self.atrValue * self.slMultiplier

            self.sell(self.longStop, abs(self.pos), True)

        # 持有空头仓位
        elif self.pos < 0:
            self.intraTradeHigh = bar.high
            self.intraTradeLow = min(self.intraTradeLow, bar.low)
            self.shortStop = self.intraTradeLow + self.atrValue * self.slMultiplier

            self.cover(self.shortStop, abs(self.pos), True)

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
    engine.setStartDate(startDate, initDays=10)

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
    engine.initStrategy(BreakingStrategy, settingDict)

    return  engine

if __name__ == '__main__':
    kLine = 28
    amSize = 30
    window = 23

    setting = {'kLine': kLine,
               'amSize': amSize,
               'window': window,
               'bollDev': 2.1,
               'slMultiplier': 0.7}

    barDbName = 'VnTrader_%ldMin_Db' % kLine
    engine = GetEngin(setting, 'au00.TB', False, barDbName,
                      '20170102', '20180206', 0,
                      0.35 / 10000, 1000, 0.05, 30000)

    '''
    # 参数优化【总收益率totalReturn 总盈亏totalNetPnl 夏普比率sharpeRatio 最大回撤maxDrawdown】
    setting = OptimizationSetting()                                                         # 新建一个优化任务设置对象
    setting.setOptimizeTarget('sharpeRatio')                                                # 设置优化排序的目标是策略夏普比率
    setting.addParameter('bollDev', 2.1, 2.2, 0.1)                                         # 优化参数openLen，起始0，结束1，步进1
    setting.addParameter('slMultiplier', 0.7, 0.7, 0.2)                                    # 增加优化参数
    setting.addParameter('window', 23, 23, 1)
    setting.addParameter('amSize', 30, 30, 15)
    start = datetime.now()
    engine.runParallelOptimization(BreakingStrategy, setting)
    print datetime.now() - start
    '''

    #'''
    # 回测
    engine.strategy.name = 'breaking_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    #engine.ShowTradeDetail()
    #engine.showDailyResult(df, result)
    engine.showBacktestingResult()
    #'''

