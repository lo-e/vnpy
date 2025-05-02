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
from .base import EVENT_BAR_UPDATED
from vnpy.trader.object import SubscribeRequest
from .trendignSniperStrategy import TrendignSniperStrategy
from queue import Empty, Queue
from vnpy.trader.utility import DIR_SYMBOL
import json

class TrendingSniperPortfolio(object):
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
        self.coins = set()
        self.strategy_symbols = set()
        self.exchange_instruments_data = {}
        self.tick_queue = Queue()
        self.strategy_monitor_time: datetime = None
        
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
        pass

    def on_start(self):
        self.update_strategy_symbols()
        Thread(target=self.process_tick).start()
        Thread(target=self.check_strategy_data_inited).start()
        # Thread(target=self.check_strategy_target_pos).start()

    def on_timer(self):
        if not self.started:
            return
        now = datetime.now()

        # 检测策略初始化状态
        if not self.strategy_monitor_time or self.strategy_monitor_time.minute != now.minute:
            self.strategy_monitor_time = now
            total_count = 0
            inited_count = 0
            for vt_symbol in self.strategy_symbols:
                strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                for i in range(len(strategies)):
                    total_count += 1
                    strategy: TrendignSniperStrategy = strategies[i]
                    if strategy.indicator_inited:
                        inited_count += 1
            print_(f"策略总数 {total_count} 已初始化 {inited_count}")

        # 下载合约列表数据
        current_hour_time = now.replace(minute=0, second=0, microsecond=0)
        download_instruments_hour_time = self.download_instruments_time.replace(minute=0, second=0, microsecond=0) if self.download_instruments_time else None
        if download_instruments_hour_time != current_hour_time:
            self.download_instruments_time = now
            self.check_download_instruments()

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
    
    def update_strategy_symbols(self):
        # 导入交易所合约
        self.load_instruments_data()

        # 确认新合约
        new_vt_symbols = set()
        okx_symbols = list(self.exchange_instruments_data.get("OKX", {}).keys())
        for symbol in okx_symbols:
            coin = symbol.split("-USDT")[0]
            if coin not in self.coins:
                self.coins.add(coin)
                new_vt_symbols.add(f"{symbol}.OKX")

        bybit_symbols = list(self.exchange_instruments_data.get("BYBIT", {}).keys())
        for symbol in bybit_symbols:
            coin = symbol.split("USDT")[0]
            if coin not in self.coins:
                self.coins.add(coin)
                new_vt_symbols.add(f"{symbol}.BYBIT")

        binance_symbols = list(self.exchange_instruments_data.get("BINANCE", {}).keys())
        for symbol in binance_symbols:
            coin = symbol.split("USDT")[0]
            if coin not in self.coins:
                self.coins.add(coin)
                new_vt_symbols.add(f"{symbol}.BINANCE")

        if new_vt_symbols:
            # 创建策略
            start = time.time()
            print_(f"开始创建策略（{len(new_vt_symbols)}）..")
            for vt_symbol in new_vt_symbols:
                self.strategy_symbols.add(vt_symbol)
                # if "BTC" in vt_symbol:
                self.new_strategy(vt_symbol)
            cost = time.time() - start
            print_(f"创建策略完成！（{len(new_vt_symbols)}）用时 {cost}s\n")

            # 订阅合约
            start = time.time()
            print_(f"开始订阅合约（{len(new_vt_symbols)}）..")
            for vt_symbol in new_vt_symbols:
                self.subscribe(vt_symbol)
            cost = time.time() - start
            print_(f"合约订阅完成！（{len(new_vt_symbols)}）用时 {cost}s\n")

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
                    strategy: TrendignSniperStrategy = strategies[i]
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

    def check_strategy_target_pos(self):
        while True:
            try:
                for vt_symbol in self.strategy_symbols:
                    strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(strategies)):
                        strategy: TrendignSniperStrategy = strategies[i]
                        if strategy.target_pos != strategy.pos:
                            if not strategy.target_pos_checking:
                                strategy.target_pos_checking = True
                                strategy.target_pos_check_ts = time.time() - 10
                                Thread(target=strategy.check_target_pos).start()

            except Exception as e:
                pass

            time.sleep(1)
    
    def check_strategy_data_inited(self):
        while True:
            try:
                # 检查策略指标初始化
                indicator_init_need = False
                for vt_symbol in self.strategy_symbols:
                    strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(strategies)):
                        strategy: TrendignSniperStrategy = strategies[i]
                        if not strategy.indicator_inited and len(strategy.live_bars):
                            indicator_init_need = True
                            break
                    
                    if indicator_init_need:
                        break

                if indicator_init_need:
                    # 请求下载
                    print(f"请求下载Bar数据")
                    download_setting = get_download_setting()
                    download_setting["request"] = True
                    save_download_setting(download_setting)

                    # 等待下载完成
                    bar_updated = False
                    check_datetime = datetime.now()
                    check_count = 0
                    while check_count < 10:
                        check_count += 1
                        print(f"等待Bar数据下载完成（{check_count}）..")
                        time.sleep(60)

                        try:
                            download_setting = get_download_setting()
                            download_at = download_setting["download_at"]
                            download_at = datetime.strptime(download_at, f"%Y-%m-%d %H:%M:%S") if download_at else download_at
                            if download_at and download_at > check_datetime:
                                bar_updated = True
                                break
                        
                        except Exception as e:
                            pass

                    if bar_updated:
                        # 策略指标初始化
                        print_(f"策略指标初始化..")
                        start = time.time()
                        total_count = 0
                        success_count = 0
                        notice_ts = time.time()
                        for vt_symbol in self.strategy_symbols:
                            strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                            for i in range(len(strategies)):
                                strategy: TrendignSniperStrategy = strategies[i]
                                if not strategy.indicator_inited:
                                    total_count += 1
                                    if len(strategy.live_bars):
                                        strategy.load_database_bar()
                                        if strategy.indicator_inited:
                                            success_count += 1
                                        time.sleep(1)

                            # 提示进度
                            if time.time() >= notice_ts + 20:
                                notice_ts = time.time()
                                cost = time.time() - start
                                print_(f"策略指标初始化..\t总数 {total_count} 成功 {success_count} 用时 {cost:.2f}s")

                        cost = time.time() - start
                        print_(f"策略指标初始化完成！ 总数 {total_count} 成功 {success_count} 用时 {cost:.2f}s\n")

            except Exception as e:
                pass

            time.sleep(1)

    def check_download_bar(self):
        if not self.bar_downloading:
            self.bar_downloading = True
            Thread(target=self.download_bar).start()

    def download_bar(self):
        # 下载Bar数据
        print_(f"Bar数据下载中..")
        self.bar_downloading = True
        start = time.time()
        success = False
        try:
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
                                contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                        elif exchange == "OKX":
                            self.download_engine.download_from_okx(
                                contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BYBIT":
                            self.download_engine.download_from_bybit(
                                contract_list=exchange_symbols, days=1, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                    success = True
                    break

                except Exception as e:
                    msg = f"TrendingSniperPortfolio 下载Bar数据出错\n\n{e}"
                    self.send_ding_talk(msg)

            cost = time.time() - start
            msg = f"Bar数据已更新！ 用时 {cost}s\n"
            print_(msg)

        except Exception as e:
            pass

        self.bar_downloading = False
        return success

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

            # 交易所合约上新，更新Gateway合约列表、更新订阅
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

                # 组合策略更新合约
                self.update_strategy_symbols()

            if download_success:
                msg = f"合约列表数据已更新！\n"
                print_(msg)
            
            else:
                msg = f"HitNewPortfolio 下载合约列表数据失败"
                self.send_ding_talk(msg)
        
        except Exception as e:
            pass

        self.instruments_downloading = False
        
    def query_gateway_contract(self, gateway_names: list):
        for gateway_name in gateway_names:
            gateway = self.cta_engine.main_engine.get_default_gateway(gateway_name)
            if gateway:
                gateway.query_contract()

    def new_strategy(self, vt_symbol: str):
        # 启动策略
        exchange = vt_symbol.split(".")[-1]
        if exchange == "OKX":
            pure_symbol = vt_symbol.split("-USDT-")[0]
            exchange_user = "lo-e"
        
        elif exchange == "BINANCE":
            pure_symbol = vt_symbol.split("USDT")[0]
            exchange_user = "lo-e"

        elif exchange == "BYBIT":
            pure_symbol = vt_symbol.split("USDT")[0]
            exchange_user = "loesuperman"

        setting = {"strategy_name": f"TRENDING_SNIPER_{pure_symbol}_{exchange}",
                    "vt_symbol": vt_symbol,
                    "exchange_user": exchange_user,
                    "start": True
                    }
        self.cta_engine.new_strategy(setting)

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

def get_minute_time(dt: datetime, gap: int):
    minute = dt.minute
    while minute % gap:
        minute -= 1
    result = dt.replace(minute=minute, second=0, microsecond=0)
    return result

def get_download_setting_file_path():
    current_file_path = os.path.abspath(__file__)
    dir_path = current_file_path.split("App_Command")[0]
    dir_path = f"{dir_path}App{DIR_SYMBOL}Turtle_crypto"
    os.makedirs(dir_path, exist_ok=True)
    return f"{dir_path}{DIR_SYMBOL}download_setting.json"

def get_download_setting():
    setting = {}
    file_path = get_download_setting_file_path()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            setting = json.load(f)
    return setting

def save_download_setting(setting: dict):
    file_path = get_download_setting_file_path()
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(setting, ensure_ascii=False))