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
from vnpy.trader.constant import Direction, Offset, Exchange
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from App.Turtle_crypto.dataservice.utility import get_csv_path
import pandas as pd
import os

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
        self.exchange_instruments_data = {}
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.data_update_hour_time: datetime = None

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        self.load_instruments_data()

    def load_instruments_data(self):
        # .csv获取交易所USDT合约列表
        try:
            csv_dir = get_csv_path()
            exhcanges = [Exchange.OKX, Exchange.BINANCE, Exchange.BYBIT]
            for exchange in exhcanges:
                exchange_instruments_data = {}
                file_path = f"{csv_dir}{exchange.value}{DIR_SYMBOL}instruments.csv"
                if not os.path.exists(file_path):
                    continue

                df = pd.read_csv(file_path)
                for _, row in df.iterrows():
                    instrument = dict(row)
                    symbol = instrument["symbol"]
                    exchange_instruments_data[symbol] = instrument
                self.exchange_instruments_data[exchange.value] = exchange_instruments_data

        except Exception as e:
            msg = f"HitNewPortfolio 获取交易所USDT合约列表出错\n\n{e}"
            self.send_ding_talk(msg)

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

    def download_data(self):
        # 下载Bar数据
        download_success = False
        try_count = 0
        while try_count < 5:
            try:
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

                # 检查下载结果
                all_success = True
                target_time = self.data_update_hour_time - timedelta(minutes=1)
                for symbol in self.strategy_symbols:
                    client = MongoClient("localhost", 27017)
                    db = client[MINUTE_DB_NAME]
                    collection = db[symbol]

                    end_data = collection.find_one(sort=[("datetime", DESCENDING)])
                    db_end_dt = end_data["datetime"] if end_data else None
                    if db_end_dt < target_time:
                        all_success = False
                        break
                
                if all_success:
                    download_success = True
                    break

            except Exception as e:
                msg = f"HitNewPortfolio 下载Bar数据出错\n\n{e}"
                self.send_ding_talk(msg)
        
        if not download_success:
            msg = f"HitNewPortfolio 下载Bar数据失败"
            self.send_ding_talk(msg)

        # 下载合约列表数据
        okx_instruments_data = []
        okx_new = []
        binance_instruments_data = []
        binance_new = []
        bybit_instruments_data = []
        bybit_new = []

        download_success = False
        try_count = 0
        while try_count < 5:
            try_count += 1
            try:
                if not okx_instruments_data:
                    okx_history_instruments_data = self.exchange_instruments_data.get(Exchange.OKX.value, {})
                    okx_instruments_data, okx_new = self.download_engine.download_instruments_list(Exchange.OKX, okx_history_instruments_data)
                
                if not binance_instruments_data:
                    binance_history_instruments_data = self.exchange_instruments_data.get(Exchange.BINANCE.value, {})
                    binance_instruments_data, binance_new = self.download_engine.download_instruments_list(Exchange.BINANCE, binance_history_instruments_data)

                if not bybit_instruments_data:
                    bybit_history_instruments_data = self.exchange_instruments_data.get(Exchange.BYBIT.value, {})
                    bybit_instruments_data, bybit_new = self.download_engine.download_instruments_list(Exchange.BYBIT, bybit_history_instruments_data)

                if len(okx_instruments_data) and len(binance_instruments_data) and len(bybit_instruments_data):
                    download_success = True
                    break

            except Exception as e:
                msg = f"HitNewPortfolio 下载合约列表数据出错\n\n{e}"
                self.send_ding_talk(msg)

        # OKX新上市合约
        for instrument in okx_new:
            symbol = instrument["symbol"]
            vt_symbol = f"{symbol}.OKX"
            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)
        
        # BINANCE新上市合约
        for instrument in binance_new:
            symbol = instrument["symbol"]
            vt_symbol = f"{symbol}.BINANCE"
            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)

        # BYBIT新上市合约
        for instrument in bybit_new:
            symbol = instrument["symbol"]
            vt_symbol = f"{symbol}.BYBIT"
            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)

        # 重新获取交易所USDT合约列表
        self.load_instruments_data()

        if not download_success:
            msg = f"HitNewPortfolio 下载合约列表数据失败"
            self.send_ding_talk(msg)

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)