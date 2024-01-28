# encoding: UTF-8

from datetime import datetime
from copy import copy
from time import time, sleep
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue

class CopytradePortfolio(object):
    """ 跟单交易组合 """
    paramList = []
    varList = []
    syncList = []

    def __init__(self, engine, setting):
        self.engine = engine

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

    def on_timer(self):
        pass
    