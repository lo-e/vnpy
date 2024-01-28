# encoding: UTF-8

from datetime import datetime
from copy import copy
from time import time, sleep
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue

class CopytradePortfolio(object):
    """ 跟单交易组合 """
    paramList = ["portfolio_value",
                 "portfolio_stop_loss"]
    varList = []
    syncList = []

    def __init__(self, engine, setting):
        self.engine = engine

        self.symbols = setting.get("symbols", [])
        self.copy_setting = setting.get("copy_setting", {})

        # 投资组合设置
        self.portfolio_value = 0
        stop_loss_value = 0
        for __, copy_data in self.copy_setting.items():
            start = copy_data.get("start", False)
            if start:
                trade_value = copy_data.get("trade_assets", 0)
                stop_loss = copy_data.get("stop_loss", -1)
                self.portfolio_value += trade_value
                stop_loss_value += trade_value * stop_loss
        self.portfolio_stop_loss = stop_loss_value / self.portfolio_value if self.portfolio_value else -1

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

    def on_timer(self):
        pass
    