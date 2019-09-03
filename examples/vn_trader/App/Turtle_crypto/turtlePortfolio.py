# encoding: UTF-8

from collections import defaultdict

from vnpy.trader.constant import (Direction, Offset)
""" modify by loe """
import re
from datetime import  datetime

""" modify by loe """
MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_DIRECTION_POS = 12      # 单方向最大持仓


########################################################################
class TurtlePortfolio(object):
    """海龟组合"""
    name = ''

    # 参数
    portfolioValue = 0  # 组合市值

    # 变量
    unitDict = defaultdict(int)  # 每个品种的持仓情况
    totalLong = 0  # 总的多头持仓
    totalShort = 0  # 总的空头持仓
    overBondList = []                        # 保证金超限统计
    pnlDic = {}                           # 交易盈亏记录


    paramList = ['name',
                 'portfolioValue']

    varList = ['totalLong',
                'totalShort']

    syncList = ['unitDict',
                'totalLong',
                'totalShort',
                'overBondList',
                'pnlDic']

    #----------------------------------------------------------------------
    def __init__(self, engine, setting):
        """Constructor"""
        self.engine = engine
        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]
    
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """"""
        for signal in self.signalDict[bar.vtSymbol]:
            signal.onBar(bar)
    
    #----------------------------------------------------------------------
    def newSignal(self, vtSymbol, direction, offset):
        """对交易信号进行过滤，符合条件的才发单执行"""

        # 开仓
        if offset == Offset.OPEN:
            # 买入
            if direction == Direction.LONG:
                # 组合持仓不能超过上限
                if self.totalLong >= MAX_DIRECTION_POS:
                    return False
                
                # 单品种持仓不能超过上限
                if self.unitDict.get(vtSymbol, 0) >= MAX_PRODUCT_POS:
                    return False

            # 卖出
            else:
                if self.totalShort <= -MAX_DIRECTION_POS:
                    return False
                
                if self.unitDict.get(vtSymbol, 0) <= -MAX_PRODUCT_POS:
                    return False
        
        self.updateUnit(vtSymbol, direction, offset)
        return True

    
    #----------------------------------------------------------------------
    def updateUnit(self, vtSymbol, direction, offset):
        """"""

        # 计算合约持仓
        if offset == Offset.OPEN:
            if direction == Direction.LONG:
                self.unitDict[vtSymbol] = self.unitDict.get(vtSymbol, 0) + 1

            else:
                self.unitDict[vtSymbol] = self.unitDict.get(vtSymbol, 0) - 1
        else:
            if vtSymbol in self.unitDict.keys():
                self.unitDict.pop(vtSymbol)
        
        # 计算总持仓、类别持仓
        self.totalLong = 0
        self.totalShort = 0
        
        for symbol, unit in self.unitDict.items():
            # 总持仓
            if unit > 0:
                self.totalLong += unit
            elif unit < 0:
                self.totalShort += unit

        # 同步到数据库
        self.engine.savePortfolioSyncData()

    # 保证金超限
    def addOverBond(self, symbol, price, perSize, multiplier, atrVolatility):
        dic = {'symbol':symbol,
               'datetime': self.engine.today,
               'price':price,
               'perSize':perSize,
               'multiplier':multiplier,
               'atrVolatility':atrVolatility}
        self.overBondList.append(dic)

        # 同步到数据库
        self.engine.savePortfolioSyncData()

    # 记录盈亏
    def addPnl(self, symbol, pnl, multiplierList, size, exitUp, exitDown, longStop, shortStop):
        startSymbol = re.sub("\d", "", symbol)
        dic = {'symbol':symbol,
               'datetime':self.engine.today,
               'pnl':pnl,
               'multiplierList':multiplierList,
               'size':size,
               'exitUp':exitUp,
               'exitDown':exitDown,
               'longStop':longStop,
               'shortStop':shortStop}

        pnlList = self.pnlDic.get(startSymbol, [])
        pnlList.append(dic)
        self.pnlDic[startSymbol] = pnlList
        # 同步到数据库
        self.engine.savePortfolioSyncData()