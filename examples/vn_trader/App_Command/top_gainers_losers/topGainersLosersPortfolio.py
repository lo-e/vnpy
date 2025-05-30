# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
import time
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from vnpy.trader.constant import Direction, Offset, Exchange
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from App.Turtle_crypto.dataservice.utility import get_csv_path
import pandas as pd
import os
from vnpy.trader.object import BarData, TickData
from vnpy.event import Event
from vnpy.trader.object import SubscribeRequest
from .topGainersLosersStrategy import TopGainersLosersStrategy
from queue import Empty, Queue
from vnpy.trader.utility import DIR_SYMBOL
import json
from .utility import Chrome

class TopGainersLosersPortfolio(object):
    parameters = ["name",
                  "portfolio_value"]

    syncs = [
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.portfolio_value = 0
        self.inited = False
        self.started = False
        self.strategy_symbols = set()
        self.exchange_instruments_data = {}
        self.tick_queue = Queue()
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.download_instruments_time: datetime = None
        self.instruments_downloading = False

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        # 导入交易所合约
        self.load_instruments_data()

        # 启动Chrome获取涨跌幅排行榜
        chrome = Chrome(cta_engine=None)
        Thread(target=chrome.fetch_top_gainers_losers, args=(self.on_top_gainers_losers, 60)).start()

    def on_start(self):
        pass

    def on_timer(self):
        if not self.started:
            return
        now = datetime.now()

        # 下载合约列表数据
        current_hour_time = now.replace(minute=0, second=0, microsecond=0)
        if self.download_instruments_time != current_hour_time:
            self.download_instruments_time = current_hour_time
            self.check_download_instruments()

    def on_top_gainers_losers(self, data: tuple):
        gainers, losers = data
        gainers_data = {}
        losers_data = {}
        close_strategies = []
        for data in gainers:
            gainers_data[data["token"]] = data["percent"]

        for data in losers:
            losers_data[data["token"]] = data["percent"]

        for name in self.strategies.keys():
            strategy: TopGainersLosersStrategy = self.strategies[name]
            pure_symbol = ""
            if strategy.exchange == Exchange.OKX:
                pure_symbol = strategy.vt_symbol.split("-")[0]
            
            else:
                pure_symbol = strategy.vt_symbol.split("USDT")[0]
            
            if strategy.direction == Direction.LONG:
                if pure_symbol in gainers_data:
                    gainers_data.pop(pure_symbol)
                
                else:
                    close_strategies.append(strategy)
            
            if strategy.direction == Direction.SHORT:
                if pure_symbol in losers_data:
                    losers_data.pop(pure_symbol)
                
                else:
                    close_strategies.append(strategy)

        # 停止关闭策略
        for i in range(len(close_strategies)):
            strategy: TopGainersLosersStrategy = close_strategies[i]
            strategy.on_close()

        # 执行新策略
        for token in gainers_data.keys():
            self.new_strategy(token, Direction.LONG)

        for token in losers_data.keys():
            self.new_strategy(token, Direction.SHORT)

        # 更新策略合约
        self.strategy_symbols = set()
        for name in self.cta_engine.strategies.keys():
            strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
            self.strategy_symbols.add(strategy.vt_symbol)

    def new_strategy(self, token:str, direction: Direction):
        # 确认合约
        vt_symbol = ""
        exchange = ""
        exchange_user = ""

        okx_symbols = list(self.exchange_instruments_data.get("OKX", {}).keys())
        symbol = f"{token}-USDT-SWAP"
        if symbol in okx_symbols:
            vt_symbol = f"{symbol}.OKX"
            exchange = "OKX"
            exchange_user = "lo-e"

        bybit_symbols = list(self.exchange_instruments_data.get("BYBIT", {}).keys())
        symbol = f"{token}USDT"
        if symbol in bybit_symbols:
            vt_symbol = f"{symbol}.BYBIT"
            exchange = "BYBIT"
            exchange_user = "loesuperman"

        binance_symbols = list(self.exchange_instruments_data.get("BINANCE", {}).keys())
        symbol = f"{token}USDT"
        if symbol in binance_symbols:
            vt_symbol = f"{symbol}.BINANCE"
            exchange = "BINANCE"
            exchange_user = "lo-e"
                
        # 启动策略
        direction_str = "LONG" if direction == Direction.LONG else "SHORT"
        setting = {"strategy_name": f"TOP_GAINERS_LOSERS_{token}_{exchange}",
                   "vt_symbol": vt_symbol,
                   "exchange": exchange,
                   "exchange_user": exchange_user,
                   "direction": direction_str,
                   "start": True
                   }
        self.cta_engine.new_strategy(setting)

    def check_download_instruments(self):
        if not self.instruments_downloading:
            self.instruments_downloading = True
            Thread(target=self.download_instruments).start()

    def download_instruments(self):
        # 下载合约列表数据
        self.instruments_downloading = True
        try:
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

            # 交易所合约上新，更新Gateway合约列表
            update_contract_gateway_names = set()
            if okx_new:
                update_contract_gateway_names.add("OKX")
            
            if binance_new:
                update_contract_gateway_names.add("BINANCE")

            if bybit_new:
                update_contract_gateway_names.add("BYBIT")

            if len(update_contract_gateway_names):
                # gateway更新合约
                self.query_gateway_contract(list(update_contract_gateway_names))

                # 导入更新交易所合约
                self.load_instruments_data()

            if download_success:
                msg = f"合约列表数据已更新！\n"
                print_(msg)
            
            else:
                msg = f"TopGainersLosersPortfolio 下载合约列表数据失败"
                self.send_ding_talk(msg)
        
        except Exception as e:
            pass

        self.instruments_downloading = False

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
            msg = f"TopGainersLosersPortfolio 获取交易所USDT合约列表出错\n\n{e}"
            self.send_ding_talk(msg)

    def process_tick(self):
        error_notice_ts = 0
        queue_size_ts = time.time()
        process_count = 0
        while True:
            try:
                tick: TickData = self.tick_queue.get(block=True, timeout=1)
                process_count += 1
                if time.time() >= queue_size_ts + 10:
                    queue_size_ts = time.time()
                    print_(f"Tick队列数 {self.tick_queue.qsize()} 最近处理 {process_count}")
                    process_count = 0

                strategies = self.cta_engine.symbol_strategy_map[tick.vt_symbol]
                for i in range(len(strategies)):
                    strategy: TopGainersLosersStrategy = strategies[i]
                    if strategy.inited:
                        strategy.on_tick(tick)

            except Empty:
                pass

            # except Exception as e:
            #     msg = f"处理Tick数据出错\t{tick.vt_symbol}\t{tick.datetime}\n{e}"
            #     print_(msg)
            #     if time.time() >= error_notice_ts + 60:
            #         error_notice_ts = time.time()
            #         self.send_ding_talk(msg)

    def subscribe(self, vt_symbol: str):
        # 订阅合约
        start = time.time()
        success = False
        while not success:
            contract = self.cta_engine.main_engine.get_contract(vt_symbol)
            if contract:
                req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
                self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
                success = True
            
            if time.time() - start >= 5:
                break

        if not success:
            print(f"行情订阅失败，找不到合约{vt_symbol}")

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")