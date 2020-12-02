# encoding: UTF-8

from collections import defaultdict

from vnpy.trader.constant import (Direction, Offset)
""" modify by loe """
import re
from datetime import  datetime
from copy import copy

""" modify by loe """
MAX_PRODUCT_POS = 4         # 单品种最大持仓
MAX_CATEGORY_POS = 6        # 高度关联最大持仓
MAX_DIRECTION_POS = 12      # 单方向最大持仓

CATEGORY_DICT = {'finance':['IF','IC','IH'],
                'nonferrous_metal':['AL'],
                 'ferrous_metal':['RB','I','HC','SM'],
                 'coal':['JM','J','ZC'],
                 'chemical_industry':['TA', 'RU']}


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
    categoryLongUnitDict = defaultdict(int)  # 高度关联品种多头持仓情况
    categoryShortUnitDict = defaultdict(int) # 高度关联品种空头持仓情况
    overBondList = []                        # 保证金超限统计
    pnlDic = {}                              # 交易盈亏记录
    today = None


    paramList = ['name',
                 'portfolioValue',
                 'is_crypto']

    varList = ['today',
                'totalLong',
                'totalShort']

    syncList = ['unitDict',
                'totalLong',
                'totalShort',
                'today',
                'categoryLongUnitDict',
                'categoryShortUnitDict',
                'overBondList',
                'pnlDic']

    #----------------------------------------------------------------------
    def __init__(self, engine, setting):
        """Constructor"""
        self.engine = engine
        self.on_update_today()

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

                """ modify by loe """
                # 高度关联品种单方向持仓不能超过上限
                startSymbol = re.sub("\d", "", vtSymbol).upper()
                startSymbol = startSymbol.split('.')[0]
                for key, value in CATEGORY_DICT.items():
                    if startSymbol in value:
                        if self.categoryLongUnitDict.get(key, 0) >= MAX_CATEGORY_POS:
                            return False
                        break

            # 卖出
            else:
                if self.totalShort <= -MAX_DIRECTION_POS:
                    return False
                
                if self.unitDict.get(vtSymbol, 0) <= -MAX_PRODUCT_POS:
                    return False

                """ modify by loe """
                startSymbol = re.sub("\d", "", vtSymbol).upper()
                startSymbol = startSymbol.split('.')[0]
                for key, value in CATEGORY_DICT.items():
                    if startSymbol in value:
                        if self.categoryShortUnitDict.get(key, 0) <= -MAX_CATEGORY_POS:
                            return False
                        break
        
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
        self.categoryLongUnitDict = defaultdict(int)
        self.categoryShortUnitDict = defaultdict(int)
        
        for symbol, unit in self.unitDict.items():
            # 总持仓
            if unit > 0:
                self.totalLong += unit
            elif unit < 0:
                self.totalShort += unit

            """ modify by loe """
            # 类别持仓
            startSymbol = re.sub("\d", "", symbol).upper()
            startSymbol = startSymbol.split('.')[0]
            for key, value in CATEGORY_DICT.items():
                if startSymbol in value:
                    if unit > 0:
                        self.categoryLongUnitDict[key] += unit
                    elif unit < 0:
                        self.categoryShortUnitDict[key] += unit
                    break

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
    def addPnl(self, symbol, pnl, multiplier, size, exitUp, exitDown, longStop, shortStop):
        startSymbol = re.sub("\d", "", symbol)
        dic = {'symbol':symbol,
               'datetime':self.today,
               'pnl':pnl,
               'multiplier':multiplier,
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

    def on_update_today(self):
        self.today = copy(self.engine.today)
        # 同步到数据库
        self.engine.savePortfolioSyncData()