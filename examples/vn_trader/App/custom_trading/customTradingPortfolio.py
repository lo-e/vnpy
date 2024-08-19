# encoding: UTF-8

from datetime import datetime
from copy import copy
from time import time, sleep
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue, Empty
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
from vnpy.trader.object import SubscribeRequest
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading

class CustomTradingPortfolio(object):
    """ 自主交易组合管理 """
    parameters = ["name"]

    variables = [
        "inited",
        "starting"
    ]

    syncs = [
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.inited = False
        self.starting = False
        self.strategy_symbols = set()
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()        # 数据下载引擎
        self.downloading_at = -1                                    # 记录开始下载的分钟时间点

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        # 下载数据
        time_minute = datetime.now().minute
        self.downloading_at = time_minute
        thread = Thread(target=self.download_data)
        thread.start()

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_timer(self):
        # 每隔五分钟下载
        time_minute = datetime.now().minute
        if not time_minute % 5 and self.downloading_at != time_minute and not len(self.download_engine.threads):
            self.downloading_at = time_minute
            thread = Thread(target=self.download_data)
            thread.start()

        # 投资组合事件推送
        self.cta_engine.put_portfolio_event()

    def download_data(self):
        # 按交易所分类合约
        contract_exchange_dict = {}
        for symbol in self.strategy_symbols:
            exchange = symbol.split(".")[-1]
            exchange_symbols = contract_exchange_dict.get(exchange, set())
            exchange_symbols.add(symbol.split(".")[0])
            contract_exchange_dict[exchange] = exchange_symbols

        # 先清空历史下载数据 
        self.download_engine.delete_history_data(target_dir=self.name)

        # 开始下载
        for exchange, exchange_symbols in contract_exchange_dict.items():
            if exchange == "BINANCE":
                self.download_engine.download_from_binance(
                    contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False
                )

            elif exchange == "OKX":
                self.download_engine.download_from_okx(
                    contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False
                )
            
            elif exchange == "BYBIT":
                self.download_engine.download_from_bybit(
                    contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False
                )

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)