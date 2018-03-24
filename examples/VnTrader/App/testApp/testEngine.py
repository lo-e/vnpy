# encoding: UTF-8

'''
本文件中实现了TestApp引擎
'''

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtObject import VtSubscribeReq

EVENT_TESTAPP_LOG = 'EVENT_TESTAPP_LOG'

########################################################################
class TestEngine(object):
    """数据记录引擎"""

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine

        self.symbol = 'al1805'

        # 注册事件监听
        self.registerEvent()

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.procecssTickEvent)

    #----------------------------------------------------------------------
    def procecssTickEvent(self, event):
        """处理行情事件"""
        tick = event.dict_['data']
        vtSymbol = tick.vtSymbol

        msg = str(tick.datetime) + vtSymbol + '  ' + str(tick.lastPrice)
        self.writeDrLog(msg)

    def subscribe(self):
        subscriveReq = VtSubscribeReq()
        subscriveReq.symbol = self.symbol
        self.mainEngine.subscribe(subscriveReq, 'CTP')
        
    #----------------------------------------------------------------------
    def writeDrLog(self, content):
        """快速发出日志事件"""
        event = Event(EVENT_TESTAPP_LOG)
        event.dict_['data'] = content
        self.eventEngine.put(event)