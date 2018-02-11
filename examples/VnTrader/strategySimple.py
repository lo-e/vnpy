# coding: utf8

from strategyEarning.stgEarningManager import stgEarningManager
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
    #'''
    test = False
    tradeSize = 1 # 交易数量
    startTime = time(22, 58, 26) # 趋势判断开始时间
    endTime =  time(22, 59, 56) # 趋势判断截至时间
    outPercent = 0.1 # 移动止盈止损百分比
    openLen = 1  # 开仓趋势过滤
    #'''

    '''
    test = True
    tradeSize = 1  # 交易数量
    startTime = time(0, 0, 0)  # 趋势判断开始时间
    endTime = time(0, 0, 0)  # 趋势判断截至时间
    outPercent = 0.05  # 移动止盈止损百分比
    openLen = 1  # 开仓趋势过滤
    '''


    # 策略变量
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
                 'tradeSize',
                 'startTime',
                 'endTime',
                 'outPercent',
                 'openLen']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
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
        if self.test:
            self.autoAgainForTest()

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

            if (self.endPrice == 0) and (tick.datetime.time() >= self.endTime):
                self.endPrice = tick.lastPrice

            if self.startPrice and self.endPrice and (not self.ordering) and (not self.todayEntry):
                sub = self.endPrice - self.startPrice
                if sub >= self.openLen:
                    # 开仓多头
                    self.buy(tick.lastPrice + 10, self.tradeSize)  # 限价单
                    self.entryOrderPrice = tick.lastPrice
                    self.ordering = True
                    self.writeCtaLog(u'开仓多头 %s' % tick.time)
                elif sub <= -self.openLen:
                    # 开仓空头
                    self.short(tick.lastPrice - 10, self.tradeSize)  # 限价单
                    self.entryOrderPrice = tick.lastPrice
                    self.ordering = True
                    self.writeCtaLog(u'开仓空头 %s' % tick.time)

            if (self.startPrice != 0) and (self.endPrice != 0) and (self.startPrice == self.endPrice) and self.test:
                self.autoAgainForTest()

        elif self.pos > 0:
            # 持有多头仓位
            if (tick.lastPrice <= self.highPrice * (1 - self.outPercent / 100)) and ( not self.ordering):
                # 止盈止损
                self.sell(tick.lastPrice - 20, abs(self.pos))  # 限价单
                self.offsetOrderPrice = tick.lastPrice
                self.ordering = True
        elif self.pos < 0:
            # 持有空头仓位
            if (tick.lastPrice >= self.lowPrice * (1 + self.outPercent / 100)) and ( not self.ordering):
                # 止盈止损
                self.cover(tick.lastPrice + 20, abs(self.pos))  # 限价单
                self.offsetOrderPrice = tick.lastPrice
                self.ordering = True

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
        elif (trade.offset == u'平仓') or (trade.offset == u'平今') or (trade.offset == u'平昨'):
            outPrice = trade.price
            sub = outPrice - self.entryPrice

            if trade.direction == u'多':
                if sub:
                    offsetEarning = -sub * trade.volume
                else:
                    offsetEarning = EMPTY_FLOAT
                entryDirect = '空'
            else:
                if sub:
                    offsetEarning = sub * trade.volume
                else:
                    offsetEarning = EMPTY_FLOAT
                entryDirect = '多'

            #另开线程进行盈亏记录操作
            newThread = threading.Thread(target=self.saveEarning, args=(offsetEarning, (self.todayDate + ' ' + trade.tradeTime), self.entryOrderPrice, self.entryPrice, entryDirect, self.offsetOrderPrice, outPrice, trade.volume))
            #开启线程守护（当主线程挂起时，无论子线程有没有执行完都会随主线程挂起）
            #newThread.setDaemon(True)
            newThread.start()

            if self.test:
                self.autoAgainForTest()
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass

    def saveEarning(self, offsetEarning = EMPTY_FLOAT, offsetTime = EMPTY_STRING, entryOrderPrice = EMPTY_FLOAT, entryPrice = EMPTY_FLOAT, entryDirect = EMPTY_STRING, offsetOrderPrice = EMPTY_FLOAT, offsetPrice = EMPTY_FLOAT, offsetVolume = EMPTY_INT):
        # 每日盈亏记录
        if not self.vtSymbol:
            return

        if self.test:
            fileName = self.name + '_' + self.vtSymbol + '_test'
        else:
            fileName = self.name + '_' + self.vtSymbol
        if not self.earningManager:
            self.earningManager = stgEarningManager()
        hisData = self.earningManager.loadDailyEarning(fileName)
        toltalEarning = EMPTY_FLOAT
        if len(hisData):
            toltalEarning = float(hisData[-1]['累计盈亏'])

        toltalEarning += offsetEarning
        content = collections.OrderedDict()
        content['时间'] = offsetTime
        content['开仓委托价'] = entryOrderPrice
        content['开仓价'] = entryPrice
        content['头寸'] = entryDirect
        content['平仓委托价'] = offsetOrderPrice
        content['平仓价'] = offsetPrice
        content['成交量'] = offsetVolume
        content['盈亏'] = offsetEarning
        content['累计盈亏'] = toltalEarning
        content['备注'] = ''
        self.earningManager.updateDailyEarning(fileName, content)

    def autoAgainForTest(self):
        nowTime = datetime.now().time()
        startMinute = nowTime.minute
        startHour = nowTime.hour
        startMinute += 1
        if startMinute >= 60:
            startMinute = 0
            startHour += 1

        endMinute = startMinute + 1
        endHour =  startHour
        if endMinute >= 60:
            endMinute = 0
            endHour += 1

        self.startTime = nowTime.replace(hour=startHour, minute=startMinute)
        self.endTime = nowTime.replace(hour=endHour, minute=endMinute)
        self.todayDate = EMPTY_STRING


#===================================回测==================================
def GetEngin(settingDict, symbol,
                   startDate, endDate, slippage,
                   rate, size, priceTick, capital):
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

    # 合约每手数量
    engine.setSize(size)

    # 合约最小价格变动
    engine.setPriceTick(priceTick)

    # 起始资金
    engine.setCapital(capital)

    # 引擎中创建策略对象
    engine.initStrategy(SimpleStrategy, settingDict)

    return  engine

if __name__ == '__main__':
    engine = GetEngin({'openLen': 0}, 'rb00.TB',
                      '20160506', '20180206', 0,
                      1.3 / 10000, 10, 1, 6000)

    '''
    # 参数优化
    setting = OptimizationSetting()                             # 新建一个优化任务设置对象
    setting.setOptimizeTarget('sharpeRatio')                    # 设置优化排序的目标是策略夏普比率
    setting.addParameter('openLen', 0, 6, 1)                    # 优化参数openLen，起始0，结束1，步进1
    #setting.addParameter('outPercent', 0.1, 0.1, 0.1)          # 增加优化参数
    start = datetime.now()
    engine.runParallelOptimization(SimpleStrategy, setting)
    print datetime.now() - start
    '''


    #'''
    # 回测
    engine.strategy.name = 'Simple'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    df = engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics(df)
    engine.showDailyResult(df, result)
    #'''

