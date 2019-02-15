# coding: utf8

from datetime import datetime, time, timedelta
from vnpy.trader.vtConstant import EMPTY_STRING, EMPTY_FLOAT, EMPTY_UNICODE, EMPTY_INT
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate)
import numpy as np
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.app.ctaStrategy.ctaBase import TICK_DB_NAME, MINUTE_DB_NAME
from vnpy.trader.vtUtility import ArrayManager, BarGenerator
from collections import defaultdict
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import *

import rqdatac as rq
from rqdatac import *
userName = "license"
password = "OZfNj71Cb6i-eXwOZzuSdHK-U23E1-irpH4EBPwf_RqIgY0-vka35iYmjBot1i2W9eJ4xiZWtrNpxsvy_iY3Il2_YkNjNzW9TVUuFWNp0QNKe0qgO1CSSYDe5uD9ajcN5J7rNtxdagH4Rmfiuv_gHc5bdxxXCnjRH2vZ8JSb2wE=TQFXjsEHPXK2rGTBE1nlgn-BlbI2IvHZmfFsM8tw3jZYATwcUvnXCCGATnAKLIBRnSc1JYVhfm1CUyRdam0vrWeUgebLaPT1rJiXkdQT2WUTwNIB3AY-TVKJciNZzGw0glt02EZXYvYPVlgiqefYVNkLEJ3EApj4i5wsd5C9J48="

class TestStrategy(CtaTemplate):
    """"""
    className = 'TestStrategy'
    author = u'loe'

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'capital',
                 'lever',
                 'perSize']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']

    # 同步列表
    syncList = ['pos']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(TestStrategy, self).__init__(ctaEngine, setting)

        """
        # 实盘需要提前导入初始化数据
        if self.getEngineType() == ENGINETYPE_TRADING:
            self.barGennerator = BarGenerator(self.onFakeBar, self.barMin, self.onBar)
            # RQData下载历史数据初始化
            self.loadHistoryData()
        """

    # ----------------------------------------------------------------------
    def loadHistoryData(self):
        rq.init(userName, password)
        minData = rq.get_price('RB1905',
                               frequency=str(self.barMin) + 'm',
                               start_date=(datetime.now() - timedelta(1)).strftime('%Y%m%d'),
                               end_date=(datetime.now() + timedelta(1)))

        for ix, row in minData.iterrows():
            bar = VtBarData()
            bar.symbol = self.vtSymbol
            bar.vtSymbol = self.vtSymbol
            bar.open = row['open']
            bar.high = row['high']
            bar.low = row['low']
            bar.close = row['close']
            bar.volume = row['volume']
            bar.datetime = row.name
            bar.date = bar.datetime.strftime("%Y%m%d")
            bar.time = bar.datetime.strftime("%H:%M:%S")
            if bar.datetime.minute % self.barMin:
                # 未满的分钟行情，自动衔接
                self.barGennerator.xminBar = bar;
            else:
                self.historyData.append(bar)
        if len(self.historyData):
            self.writeCtaLog(u'历史数据加载完成')
        else:
            self.writeCtaLog(u'历史数据加载失败')


    # ----------------------------------------------------------------------

    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' % self.name)

        """
        # 实盘需要提前导入初始化数据
        if self.getEngineType() == ENGINETYPE_TRADING:
            for bar in self.historyData:
                self.onBar(bar)
        """
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
        self.cancelAll()
        if not self.trading:
            return

        self.sell(tick.lastPrice-20, 1)
        # 发出状态更新事件
        self.putEvent()

    # ----------------------------------------------------------------------
    def onFakeBar(self, bar):
        self.minBar = bar.datetime
        self.barGennerator.updateBar(bar)
        pass

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
        self.cancelAll()

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
    engine.initStrategy(TestStrategy, settingDict)

    # 起始资金
    engine.setCapital(engine.strategy.capital)

    # 合约每手数量
    engine.setSize(engine.strategy.perSize)

    return  engine

if __name__ == '__main__':
    tickPrice = 5
    setting = {'capital':6000,
               'lever':5,
               'perSize':5,
               'tickPrice':tickPrice}

    engine = GetEngin(setting, 'ZN00',
                      '20180101', '20190601', 0,
                      0 / 10000, tickPrice)


    #'''
    # 回测
    engine.strategy.name = 'Test_backtesting'
    engine.strategy.vtSymbol = engine.symbol
    engine.runBacktesting()
    # 保存交易记录到iCloud
    engine.ShowTradeDetail()
    # 图表展示交易指标
    engine.calculateDailyResult()
    df, result = engine.calculateDailyStatistics()
    engine.showDailyResult(df, result)

    # engine.showBacktestingResult()
    #'''

