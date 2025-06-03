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
        self.gainers_data = {}
        self.losers_data = {}
        self.strategy_status_check_ts = {}
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.download_instruments_time: datetime = None
        self.instruments_downloading = False
        self.bar_download_queue = Queue()

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        # 导入交易所合约
        self.load_instruments_data()

        # 启动Chrome获取涨跌幅排行榜
        chrome = Chrome(cta_engine=None)
        Thread(target=chrome.fetch_top_gainers_losers, args=(self.on_top_gainers_losers, 10)).start()

        # Bar下载
        Thread(target=self.download_bar).start()

    def on_start(self):
        # tick 处理
        Thread(target=self.process_tick).start()

        # 策略仓位检查
        Thread(target=self.check_strategy_status).start()

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
        try:
            gainers, losers = data
            mean_gainers_percent = pd.DataFrame(gainers)["percent"].mean()
            mean_losers_percent = pd.DataFrame(losers)["percent"].mean()

            trending_long = False
            trending_short = False
            if mean_gainers_percent >= 0.5 and len(gainers) >= 10:
                trending_long = True
            
            if mean_losers_percent <= -0.5 and len(losers) >= 10:
                trending_short = True

            # 保存到文件
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = datetime.now().strftime(f"%Y-%m-%d")
            hour = datetime.now().hour
            time = datetime.now().strftime(f"%H_%M_%S")

            gainer_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}gainers{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(gainer_dir_path, exist_ok=True)
            gainer_file_path = f"{gainer_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(gainers)
            df.to_csv(gainer_file_path, index=False)

            loser_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}losers{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(loser_dir_path, exist_ok=True)
            loser_file_path = f"{loser_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(losers)
            df.to_csv(loser_file_path, index=False)

            # 判断上新、停止策略
            gainers_data = {}
            losers_data = {}
            close_long_strategies = []
            close_short_strategies = []
            msg = ""

            for data in gainers:
                gainers_data[data["token"]] = data["percent"]

            for data in losers:
                losers_data[data["token"]] = data["percent"]

            last_gainers_data = self.gainers_data.copy() if self.gainers_data else gainers_data.copy()
            last_losers_data = self.losers_data.copy() if self.losers_data else losers_data.copy()
            self.gainers_data = gainers_data.copy()
            self.losers_data = losers_data.copy()

            for name in self.cta_engine.strategies.keys():
                strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
                pure_symbol = ""
                if strategy.exchange == Exchange.OKX:
                    pure_symbol = strategy.vt_symbol.split("-")[0]
                
                else:
                    pure_symbol = strategy.vt_symbol.split("USDT")[0]
                
                if strategy.direction == Direction.LONG:
                    if not trending_long or pure_symbol not in gainers_data:
                        close_long_strategies.append(strategy)

                    if pure_symbol in gainers_data:
                        gainers_data.pop(pure_symbol)
                
                if strategy.direction == Direction.SHORT:
                    if not trending_short or pure_symbol not in losers_data:
                        close_short_strategies.append(strategy)

                    if pure_symbol in losers_data:
                        losers_data.pop(pure_symbol)

            # 停止关闭策略
            for i in range(len(close_long_strategies)):
                strategy: TopGainersLosersStrategy = close_long_strategies[i]
                strategy.on_close()

            for i in range(len(close_short_strategies)):
                strategy: TopGainersLosersStrategy = close_short_strategies[i]
                strategy.on_close()

            if len(close_long_strategies):
                msg = f"{msg}关闭多头合约：{len(close_long_strategies)}\n"

            if len(close_short_strategies):
                msg = f"{msg}关闭空头合约：{len(close_short_strategies)}\n"

            # 执行新策略
            new_gainer_count = 0
            if trending_long:
                for token in gainers_data.keys():
                    if token not in last_gainers_data:
                        result = self.new_strategy(token, Direction.LONG)
                        if result:
                            new_gainer_count += 1

            if new_gainer_count:
                msg = f"{msg}执行多头合约：{new_gainer_count}\n"

            new_loser_count = 0
            if trending_short:
                for token in losers_data.keys():
                    if token not in last_losers_data:
                        result = self.new_strategy(token, Direction.SHORT)
                        if result:
                            new_loser_count += 1

            if new_loser_count:
                msg = f"{msg}执行空头合约：{new_loser_count}\n"

            # 更新策略合约
            self.strategy_symbols = set()
            for name in self.cta_engine.strategies.keys():
                strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
                self.strategy_symbols.add(strategy.vt_symbol)

            if msg:
                msg = f"上涨：{mean_gainers_percent:.2f}%\t{len(gainers)}\n下跌：{mean_losers_percent:.2f}%\t{len(losers)}\n\n{msg}当前策略总数：{len(self.cta_engine.strategies)}"
                self.send_ding_talk(msg)
                print(msg)

        except Exception as e:
            msg = f"处理涨跌代币数据出错\n\n{e}"
            self.send_ding_talk(msg)
            print(msg)

    def new_strategy(self, token:str, direction: Direction):
        # 确认合约
        vt_symbol = ""
        exchange = ""
        exchange_user = ""

        filter_tokens = ["USDC", "USDT", "USDE", "USD1", "PYUSD", "USDS", "DAI", "FTN", "PI"]
        if token not in filter_tokens:
            okx_symbols = list(self.exchange_instruments_data.get("OKX", {}).keys())
            symbol = f"{token}-USDT-SWAP"
            if symbol in okx_symbols:
                vt_symbol = f"{symbol}.OKX"
                exchange = "OKX"
                exchange_user = "lo-e"

            if not vt_symbol:
                bybit_symbols = list(self.exchange_instruments_data.get("BYBIT", {}).keys())
                symbol = f"{token}USDT"
                if symbol in bybit_symbols:
                    vt_symbol = f"{symbol}.BYBIT"
                    exchange = "BYBIT"
                    exchange_user = "loesuperman"

            # if not vt_symbol:
            #     binance_symbols = list(self.exchange_instruments_data.get("BINANCE", {}).keys())
            #     symbol = f"{token}USDT"
            #     if symbol in binance_symbols:
            #         vt_symbol = f"{symbol}.BINANCE"
            #         exchange = "BINANCE"
            #         exchange_user = "lo-e"

        if not vt_symbol:
            return False
        
        # 启动策略
        if direction == Direction.LONG:
            strategy_name = f"TOP_GAINERS_{token}_{exchange}"
            direction_str = "LONG"
        
        else:
            strategy_name = f"TOP_LOSERS_{token}_{exchange}"
            direction_str = "SHORT"

        setting = {"strategy_name": strategy_name,
                   "vt_symbol": vt_symbol,
                   "exchange": exchange,
                   "exchange_user": exchange_user,
                   "direction": direction_str,
                   "start": True
                   }
        result, msg = self.cta_engine.new_strategy_setting(setting)
        if result:
            self.cta_engine.new_strategy(setting)
            self.bar_download_queue.put(vt_symbol)
        
        else:
            msg = f"执行新策略失败\n\n{msg}"
            self.send_ding_talk(msg)
            print(msg)
        return result

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

    def download_bar(self):
        while True:
            try:
                vt_symbol = self.bar_download_queue.get(block=True, timeout=1)
                exchange = vt_symbol.split(".")[-1]
                symbol = vt_symbol.split(".")[0]

                success = False
                try_count = 0
                while not success and try_count < 5:
                    try_count += 1
                    try:
                        # 先清空历史下载数据 
                        self.download_engine.delete_history_data(target_dir=self.name)

                        # 开始下载
                        if exchange == "OKX":
                            self.download_engine.download_from_okx(
                                contract_list=[symbol], hours=2, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BYBIT":
                            self.download_engine.download_from_bybit(
                                contract_list=[symbol], hours=2, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BINANCE":
                            self.download_engine.download_from_binance(
                                contract_list=[symbol], hours=2, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                        success = True

                    except Exception as e:
                        msg = f"TopGainersLosersPortfolio 下载Bar数据出错\n\n{e}"
                        self.send_ding_talk(msg)

                if success:
                    symbol_strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(symbol_strategies)):
                        strategy: TopGainersLosersStrategy = symbol_strategies[i]
                        if not strategy.indicator_inited:
                            strategy.load_database_bar()

            except Empty:
                pass

            except Exception as e:
                pass

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

    def check_strategy_status(self):
        while True:
            try:
                for name in self.cta_engine.strategies.copy().keys():
                    strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
                    strategy_check_ts = self.strategy_status_check_ts.get(strategy.strategy_name, 0)
                    if time.time() >= strategy_check_ts + 10:
                        self.strategy_status_check_ts[strategy.strategy_name] = time.time()

                        # 检查仓位
                        if strategy.tick and strategy.target_pos != strategy.pos:
                            if strategy.direction == Direction.LONG:
                                if strategy.target_pos < 0 or strategy.pos < 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy.target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = strategy.target_pos - strategy.pos
                                if gap > 0:
                                    # 多头开仓
                                    trade_price = strategy.tick.last_price * 1.005
                                    strategy.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                                
                                elif gap < 0:
                                    # 多头平仓
                                    trade_price = strategy.tick.last_price * 0.995
                                    strategy.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                            if strategy.direction == Direction.SHORT:
                                if strategy.target_pos > 0 or strategy.pos > 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy.target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = abs(strategy.target_pos) - abs(strategy.pos)
                                if gap > 0:
                                    # 空头开仓
                                    trade_price = strategy.tick.last_price * 0.995
                                    strategy.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                                
                                elif gap < 0:
                                    # 空头平仓
                                    trade_price = strategy.tick.last_price * 1.005
                                    strategy.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                        # 检查关闭策略
                        if strategy.target_pos == strategy.pos and strategy.close:
                            try:
                                # 移除策略
                                result, msg = self.cta_engine.remove_strategy_setting(strategy.strategy_name)
                                strategy.check_save_data_()
                                self.cta_engine.remove_strategy(strategy.strategy_name)
                                if not result:
                                    msg = f"停止关闭策略异常\n\n{msg}"
                                    self.send_ding_talk(msg)
                                    print_(msg)

                            except Exception as e:
                                msg = f"停止关闭策略出错\n\n{e}"
                                self.send_ding_talk(msg)
                                print_(msg)

            except Exception as e:
                # msg = f"核查策略目标仓位出错\n\n{e}"
                # self.send_ding_talk(msg)
                # break
                pass

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