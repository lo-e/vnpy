# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
from time import time, sleep
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue, Empty
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
from vnpy.trader.object import SubscribeRequest
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from vnpy.trader.constant import Direction, Offset

class HitNewPortfolio(object):
    parameters = ["name",
                  "portfolioValue"]

    syncs = [
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.inited = False
        self.starting = False
        self.strategy_symbols = set()
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.data_update_hour_time: datetime = None

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        pass

    def on_timer(self):
        download_need = False
        current_hour_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        
        if not self.data_update_hour_time and datetime.now().minute <= 55:
            download_need = True

        if self.data_update_hour_time and self.data_update_hour_time != current_hour_time:
            download_need = True

        if download_need:
            self.data_update_hour_time = current_hour_time
            thread = Thread(target=self.download_data)
            thread.start()

            # 状态更新
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