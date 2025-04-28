# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
import time
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
from vnpy.trader.object import BarData
from vnpy.event import Event
from .base import EVENT_BAR_UPDATED

class HitNewPortfolio(object):
    parameters = ["name",
                  "portfolio_value"]

    syncs = [
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.portfolio_value = 0
        self.inited = False
        self.starting = False
        self.strategy_symbols = set()
        self.exchange_instruments_data = {}
        self.new_signal_settings = []
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.download_bar_time: datetime = None
        self.bar_downloading = False
        self.download_instruments_time: datetime = None
        self.instruments_downloading = False

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        self.load_instruments_data()

    def on_start(self):
        pass

    def on_timer(self):
        # 下载Bar数据
        current_minute_time = datetime.now().replace(second=0, microsecond=0)
        download_bar_minute_time = self.download_bar_time.replace(second=0, microsecond=0) if self.download_bar_time else None
        if download_bar_minute_time != current_minute_time and not self.bar_downloading:
            # 核查执行新策略
            for setting in self.new_signal_settings:
                self.cta_engine.hit_new_strategy(setting)
            self.new_signal_settings = []

            self.download_bar_time = datetime.now()
            thread = Thread(target=self.download_bar_data)
            thread.start()

        # 下载合约列表数据
        current_hour_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        download_instruments_hour_time = self.download_instruments_time.replace(minute=0, second=0, microsecond=0) if self.download_instruments_time else None
        download_bar_after = True if self.download_bar_time and datetime.now() >= self.download_bar_time + timedelta(seconds=5) else False
        if download_instruments_hour_time != current_hour_time and download_bar_after and not self.instruments_downloading:
            self.download_instruments_time = datetime.now()
            thread = Thread(target=self.download_instruments_data)
            thread.start()

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

    def download_bar_data(self):
        # 下载Bar数据
        self.bar_downloading = True
        download_success = False
        result_bar_list = []
        try_count = 0
        while try_count < 5:
            try_count += 1
            try:
                # 按交易所分类合约
                contract_exchange_dict = {}
                for symbol in self.strategy_symbols.copy():
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
                all_downloaded = True
                for symbol in self.strategy_symbols.copy():
                    client = MongoClient("localhost", 27017)
                    db = client[MINUTE_DB_NAME]
                    collection = db[symbol]

                    now = datetime.now().replace(second=0, microsecond=0)
                    dt_from = now - timedelta(minutes=10)
                    dt_to = now - timedelta(minutes=1)
                    flt = {"datetime": {"$gte": dt_from, "$lte": dt_to}}
                    bar_list = list(collection.find(flt).sort("datetime", DESCENDING))
                    if bar_list:
                        data = bar_list[0]
                        bar = BarData(
                            gateway_name="",
                            symbol="",
                            exchange=Exchange.NONE,
                            datetime=None,
                            endDatetime=None)
                        bar.__dict__ = data
                        if bar.datetime == dt_to:
                            result_bar_list.append(copy(bar))

                        else:
                            all_downloaded = False
                            break

                if all_downloaded:
                    download_success = True
                    break

            except Exception as e:
                msg = f"HitNewPortfolio 下载Bar数据出错\n\n{e}"
                self.send_ding_talk(msg)

        if download_success:
            # 输出结果
            for bar in result_bar_list:
                print(f"{bar.datetime}\t{bar.vt_symbol}\t{bar.open_price}\t{bar.high_price}\t{bar.low_price}\t{bar.close_price}")

            msg = f"Bar数据已更新！\n"
            self.print_(msg)

            # 发送事件
            event = Event(type=EVENT_BAR_UPDATED, data="")
            self.cta_engine.event_engine.put(event)

        else:
            msg = f"HitNewPortfolio 下载Bar数据失败"
            self.send_ding_talk(msg)

        self.bar_downloading = False

    def download_instruments_data(self):
        # 下载合约列表数据
        self.instruments_downloading = True
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
            market_on = instrument["on"]
            pure_symbol = symbol.split("-USDT-")[0]
            vt_symbol = f"{symbol}.OKX"

            signal_long_setting = {
                "strategy_name": f"HIT_NEW_LONG_{pure_symbol}_OKX",
                "vt_symbol": vt_symbol,
                "exchange_user": "lo-e",
                "direction": "LONG",
                "market_on": market_on,
                "start": False
                }

            signal_short_setting = {
                "strategy_name": f"HIT_NEW_SHORT_{pure_symbol}_OKX",
                "vt_symbol": vt_symbol,
                "exchange_user": "lo-e",
                "direction": "SHORT",
                "market_on": market_on,
                "start": False
                }
            self.new_signal_settings.extend([signal_long_setting, signal_short_setting])

            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)
        
        # BINANCE新上市合约
        for instrument in binance_new:
            symbol = instrument["symbol"]
            market_on = instrument["on"]
            pure_symbol = symbol.split("USDT")[0]
            vt_symbol = f"{symbol}.BINANCE"

            signal_long_setting = {
                "strategy_name": f"HIT_NEW_LONG_{pure_symbol}_BINANCE",
                "vt_symbol": vt_symbol,
                "exchange_user": "lo-e",
                "direction": "LONG",
                "market_on": market_on,
                "start": True
                }

            signal_short_setting = {
                "strategy_name": f"HIT_NEW_SHORT_{pure_symbol}_BINANCE",
                "vt_symbol": vt_symbol,
                "exchange_user": "lo-e",
                "direction": "SHORT",
                "market_on": market_on,
                "start": True
                }
            self.new_signal_settings.extend([signal_long_setting, signal_short_setting])

            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)

        # BYBIT新上市合约
        for instrument in bybit_new:
            symbol = instrument["symbol"]
            pure_symbol = symbol.split("USDT")[0]
            vt_symbol = f"{symbol}.BYBIT"

            signal_long_setting = {
                "strategy_name": f"HIT_NEW_LONG_{pure_symbol}_BYBIT",
                "vt_symbol": vt_symbol,
                "exchange_user": "loesuperman",
                "direction": "LONG",
                "market_on": market_on,
                "start": False
                }

            signal_short_setting = {
                "strategy_name": f"HIT_NEW_SHORT_{pure_symbol}_BYBIT",
                "vt_symbol": vt_symbol,
                "exchange_user": "loesuperman",
                "direction": "SHORT",
                "market_on": market_on,
                "start": False
                }
            self.new_signal_settings.extend([signal_long_setting, signal_short_setting])
            
            msg = f"{vt_symbol} 合约上新"
            self.send_ding_talk(msg)

        # 重新获取交易所USDT合约列表
        self.load_instruments_data()

        if download_success:
            msg = f"合约列表数据已更新！\n"
            self.print_(msg)
        
        else:
            msg = f"HitNewPortfolio 下载合约列表数据失败"
            self.send_ding_talk(msg)

        self.instruments_downloading = False

    def print_(self, msg: str):
        dt = datetime.now().replace(microsecond=0)
        print(f"{dt}\t{msg}")

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)