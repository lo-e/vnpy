# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
import time
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from vnpy.trader.constant import Direction, Offset, Exchange
from App.Turtle_crypto.dataservice.utility import get_csv_path
import pandas as pd
import os
from vnpy.trader.object import BarData, TickData
from vnpy.event import Event
from vnpy.trader.object import SubscribeRequest
from .topGainersLosersStrategy import TopGainersLosersStrategy
from queue import Empty, Queue
from vnpy.trader.event import EVENT_TICK_DELAY, EVENT_ACCOUNT
from vnpy.trader.object import AccountData
import copy

class TopGainersLosersPortfolio(object):
    parameters = ["name",
                  "portfolio_value"]

    syncs = [
        "account_ath",
        "account_drawdown",
        "trending_tokens",
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.portfolio_value = 0
        self.inited = False
        self.started = False
        self.exchange_instruments_data = {}
        self.tick_queue = Queue()
        self.gainers_data = {}
        self.losers_data = {}
        self.strategy_status_check_ts = {}
        self.rise_data_list_5m = []
        self.fall_data_list_5m = []
        self.rise_data_list_24h = []
        self.fall_data_list_24h = []
        self.sync_data = {}
        self.unsubscribe_time = 0
        self.account_ath = 0
        self.account_drawdown = {}
        self.account_balance_data = {}
        self.account_notice_ts = 0
        self.fast_rise_tokens = []
        self.fast_fall_tokens = []
        self.strategy_long_tokens = []
        self.strategy_short_tokens = []
        self.trending_tokens = {}
        self.history_trending_data = {}
        self.rise_onboard_symbol_time_dict = {}
        self.fall_onboard_symbol_time_dict = {}
        
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

        # 导入最近趋势数据
        self.load_recent_trending_data()

        # 监控行情数据延迟事件
        self.cta_engine.event_engine.register(EVENT_TICK_DELAY, self.resubscribe)
        self.cta_engine.event_engine.register(EVENT_ACCOUNT, self.on_account)

        # Bar下载
        Thread(target=self.download_bar).start()

    def on_start(self):
        # tick 处理
        Thread(target=self.process_tick).start()

        # 策略仓位检查
        Thread(target=self.check_strategy_status).start()

        # 获取涨跌幅排行榜数据
        Thread(target=self.check_rank_file_data).start()

    def on_timer(self):
        if not self.started:
            return

        # 下载合约列表数据
        now = datetime.now()
        current_hour_time = now.replace(minute=0, second=0, microsecond=0)
        if self.download_instruments_time != current_hour_time:
            self.download_instruments_time = current_hour_time
            self.check_download_instruments()

        # 重新订阅
        if self.unsubscribe_time and time.time() - self.unsubscribe_time >= 5:
            self.unsubscribe_time = 0
            self.subscribe_strategies()

        # 保存同步数据
        self.check_save_data()

    def on_account(self, event: Event):
        # 筛选USDT
        account:AccountData = event.data
        if account.accountid != "USDT":
            return
        
        # 账户名称
        account_name = f"{account.gateway_name}({account.exchange_user})"
        
        # 判断是否正在交易
        on_trading = False
        for strategy_name in self.cta_engine.strategies.keys():
            strategy: TopGainersLosersStrategy = self.cta_engine.strategies[strategy_name]
            if strategy.open_count and strategy.open_allowed and (strategy.pos or not strategy.close):
                on_trading = True
                break

        # 统计所有账户余额
        self.account_balance_data[account_name] = account.balance
        if len(self.account_balance_data) >= 3:
            total_balance = 0
            for _, balance in self.account_balance_data.items():
                total_balance += balance
            total_balance = round(total_balance, 2)
            
            if total_balance > self.account_ath:
                # 净值新高
                if self.account_ath:
                    msg = f"恭喜！净值新高\n\nATH {total_balance}USDT"
                    self.send_ding_talk(msg)
                    print_(msg)

                self.account_ath = total_balance
                self.account_drawdown = 0

            elif not on_trading:
                # 回撤
                self.account_drawdown = self.account_ath - total_balance

            if int(time.time()) > self.account_notice_ts + 60:
                self.account_notice_ts = int(time.time())

                print("-"*12)
                for account_name, balance in self.account_balance_data.items():
                    print_(f"余额：{balance:.2f}\t{account_name}")
                print_(f"ATH：{self.account_ath}\t回撤：{self.account_drawdown}")
                print("-"*12)

    def resubscribe(self, event: Event):
        return
    
        # 取消订阅
        self.subscribe_strategies(unsubscribe=True)

        # 记录取消订阅时间
        self.unsubscribe_time = time.time()

    def subscribe_strategies(self, unsubscribe: bool = False):
        vt_symbols = set()
        for strategy_name in self.cta_engine.strategies.keys():
            strategy: TopGainersLosersStrategy = self.cta_engine.strategies[strategy_name]
            vt_symbols.add(strategy.vt_symbol)
        
        if vt_symbols:
            if unsubscribe:
                self.cta_engine.unsubscribe(list(vt_symbols))
            
            else:
                self.cta_engine.subscribe(list(vt_symbols))

    def on_trending_data(self, data: tuple):
        rise_trending_list, fall_trending_list = data
        data_time = rise_trending_list[0]["change"]
        rise_trending_list = rise_trending_list[3:]
        fall_trending_list = fall_trending_list[3:]
        
        trending_tokens = set()
        for i in range(min(len(rise_trending_list), 20)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            if abs(change) >= 20.0 or (abs(change) >= 15.0 and i < 10):
                trending_tokens.add(symbol)
                if symbol not in self.trending_tokens:
                    on_board_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"change": change,
                                     "on_board_ts": data_time,
                                     "on_board": on_board_time}
                    
                    history_data = self.history_trending_data.get(symbol, {})
                    if history_data:
                        pop_ts = history_data["pop_ts"]
                        if data_time <= pop_ts + 1 * 60 * 60:
                            trending_data = {"change": change,
                                             "on_board_ts": history_data["on_board_ts"],
                                             "on_board": history_data["on_board"]}
                            
                    self.trending_tokens[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 20)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            if abs(change) >= 20.0 or (abs(change) >= 15.0 and i < 10):
                trending_tokens.add(symbol)
                if symbol not in self.trending_tokens:
                    on_board_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"change": change,
                                     "on_board_ts": data_time,
                                     "on_board": on_board_time}
                    
                    history_data = self.history_trending_data.get(symbol, {})
                    if history_data:
                        pop_ts = history_data["pop_ts"]
                        if data_time <= pop_ts + 1 * 60 * 60:
                            trending_data = {"change": change,
                                             "on_board_ts": history_data["on_board_ts"],
                                             "on_board": history_data["on_board"]}
                            
                    self.trending_tokens[symbol] = trending_data
        
        for symbol in self.trending_tokens.copy().keys():
            if symbol not in trending_tokens:
                trending_data = self.trending_tokens[symbol]
                self.history_trending_data[symbol] = {"pop_ts": data_time,
                                                      "on_board_ts": trending_data["on_board_ts"],
                                                      "on_board": trending_data["on_board"]}
                self.trending_tokens.pop(symbol)

    def on_rise_fall_data(self, data: tuple):
        rise_list, fall_list = data
        rise_data_time = rise_list[0]["change"]
        fall_data_time = fall_list[0]["change"]
        mean_rise_change = rise_list[1]["change"]
        mean_fall_change = rise_list[2]["change"]

        rise_list = rise_list[3:]
        fall_list = fall_list[3:]
        rise_list = sorted(rise_list, key=lambda x: x["change"], reverse=True)
        fall_list = sorted(fall_list, key=lambda x: x["change"], reverse=False)

        new_settings = []
        close_long_tokens = set()
        close_short_tokens = set()
        new_long_count = 0
        new_short_count = 0
        
        # 是否初始数据状态
        initial = False
        if not self.rise_onboard_symbol_time_dict or not self.fall_onboard_symbol_time_dict:
            initial = True

        if rise_list and fall_list:
            # 更新代币上榜时间
            rise_symbol_change_dict = {}
            for rise_data in rise_list:
                symbol = rise_data["symbol"]
                rise_symbol_change_dict[symbol] = rise_data["change"]
                if symbol not in self.rise_onboard_symbol_time_dict:
                    if initial:
                        self.rise_onboard_symbol_time_dict[symbol] = 0
                    
                    else:
                        self.rise_onboard_symbol_time_dict[symbol] = rise_data_time
            
            fall_symbol_change_dict = {}
            for fall_data in fall_list:
                symbol = fall_data["symbol"]
                fall_symbol_change_dict[symbol] = fall_data["change"]
                if symbol not in self.fall_onboard_symbol_time_dict:
                    if initial:
                        self.fall_onboard_symbol_time_dict[symbol] = 0
                    
                    else:
                        self.fall_onboard_symbol_time_dict[symbol] = fall_data_time

            # 检索退出排行榜的代币
            for symbol in self.rise_onboard_symbol_time_dict.copy().keys():
                if symbol not in rise_symbol_change_dict:
                    self.rise_onboard_symbol_time_dict.pop(symbol)
                    if symbol in self.fast_rise_tokens:
                        self.fast_rise_tokens.remove(symbol)

                        if symbol in self.strategy_long_tokens:
                            self.strategy_long_tokens.remove(symbol)

                            close_long_tokens.add(symbol)
                            stop_msg = f"{symbol} 停止上涨"
                            self.send_ding_talk(stop_msg)
            
            for symbol in self.fall_onboard_symbol_time_dict.copy().keys():
                if symbol not in fall_symbol_change_dict:
                    self.fall_onboard_symbol_time_dict.pop(symbol)
                    if symbol in self.fast_fall_tokens:
                        self.fast_fall_tokens.remove(symbol)
                        
                        if symbol in self.strategy_short_tokens:
                            self.strategy_short_tokens.remove(symbol)

                            close_short_tokens.add(symbol)
                            stop_msg = f"{symbol} 停止下跌"
                            self.send_ding_talk(stop_msg)

            # 判断上涨Top1代币
            rise_top_1_symbol = rise_list[0]["symbol"]
            rise_top_1_change = rise_list[0]["change"]
            rise_top_2_change = rise_list[1]["change"]
            onboard_time = self.rise_onboard_symbol_time_dict.get(rise_top_1_symbol, time.time())
            from_onboard_time = rise_data_time - onboard_time
            if rise_top_1_symbol not in self.fast_rise_tokens and from_onboard_time <= 100*60 and abs(rise_top_1_change) > 1.0:
                self.fast_rise_tokens.append(rise_top_1_symbol)

                if rise_top_1_symbol in self.trending_tokens:
                    setting = self.new_strategy(rise_top_1_symbol, Direction.LONG)
                    if setting:
                        setting["trending_ts"] = self.trending_tokens[rise_top_1_symbol]["on_board_ts"]
                        setting["trending_time"] = self.trending_tokens[rise_top_1_symbol]["on_board"]
                        if rise_top_1_symbol not in self.strategy_long_tokens:
                            self.strategy_long_tokens.append(rise_top_1_symbol)

                        new_long_count += 1
                        new_settings.append(setting)

                        vt_symbol = setting["vt_symbol"]
                        onboard_time_str = datetime.fromtimestamp(onboard_time).strftime(f"%H:%M:%S")
                        top_time_str = datetime.fromtimestamp(rise_data_time).strftime(f"%H:%M:%S")
                        top_msg = f"{rise_top_1_symbol} 上涨\n{vt_symbol}（{rise_top_1_change} {rise_top_2_change}）\nfrom {onboard_time_str}\nto {top_time_str}\nin {from_onboard_time}s"
                        self.send_ding_talk(top_msg)

            # 判断下跌Top1代币
            fall_top_1_symbol = fall_list[0]["symbol"]
            fall_top_1_change = fall_list[0]["change"]
            fall_top_2_change = fall_list[1]["change"]
            onboard_time = self.fall_onboard_symbol_time_dict.get(fall_top_1_symbol, time.time())
            from_onboard_time = fall_data_time - onboard_time
            if fall_top_1_symbol not in self.fast_fall_tokens and from_onboard_time <= 100*60 and abs(fall_top_1_change) > 1.0:
                self.fast_fall_tokens.append(fall_top_1_symbol)

                if fall_top_1_symbol in self.trending_tokens:
                    setting = self.new_strategy(fall_top_1_symbol, Direction.SHORT)
                    if setting:
                        setting["trending_ts"] = self.trending_tokens[fall_top_1_symbol]["on_board_ts"]
                        setting["trending_time"] = self.trending_tokens[fall_top_1_symbol]["on_board"]
                        if fall_top_1_symbol not in self.strategy_short_tokens:
                            self.strategy_short_tokens.append(fall_top_1_symbol)

                        new_short_count += 1
                        new_settings.append(setting)

                        vt_symbol = setting["vt_symbol"]
                        onboard_time_str = datetime.fromtimestamp(onboard_time).strftime(f"%H:%M:%S")
                        top_time_str = datetime.fromtimestamp(fall_data_time).strftime(f"%H:%M:%S")
                        top_msg = f"{fall_top_1_symbol} 下跌\n{vt_symbol}（{fall_top_1_change} {fall_top_2_change}）\nfrom {onboard_time_str}\nto {top_time_str}\nin {from_onboard_time}s"
                        self.send_ding_talk(top_msg)

        if close_long_tokens or close_short_tokens:
            # 停止当前策略
            remove_strategy_names = []
            remove_vt_symbols = set()
            for name in self.cta_engine.strategies.keys():
                strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
                if "OKX" in strategy.vt_symbol:
                    symbol = strategy.vt_symbol.split("-USDT")[0]

                else:
                    symbol = strategy.vt_symbol.split("USDT")[0]
                if (symbol in close_long_tokens and strategy.direction == Direction.LONG) or (symbol in close_short_tokens and strategy.direction == Direction.SHORT):
                    strategy.on_close()
                    remove_strategy_names.append(strategy.strategy_name)
                    remove_vt_symbols.add(strategy.vt_symbol)
            
            # 清除setting
            self.cta_engine.remove_strategy_setting(remove_strategy_names)

            # 订阅合约
            # self.cta_engine.subscribe(list(remove_vt_symbols))

        if new_settings:
            # 执行新策略
            new_vt_symbols = set()
            for setting in new_settings:
                vt_symbol = setting["vt_symbol"]
                direction = setting["direction"]
                new_vt_symbols.add(vt_symbol)
                self.cta_engine.new_strategy(setting)
                self.bar_download_queue.put((vt_symbol, direction))

            # 添加setting
            self.cta_engine.new_strategy_setting(new_settings)

            # 订阅合约
            self.cta_engine.subscribe(list(new_vt_symbols))

    def new_strategy(self, token:str, direction: Direction):
        # 确认合约
        vt_symbol = ""
        exchange = ""
        exchange_user = ""

        filter_tokens = ["USDC", "USDT", "USDE", "SUSDE", "SUSDS", "USD1", "USDT0", "PYUSD", "USDS", "FDUSD", "DAI", "FTN", "PI", "WETH", "WEETH", "STETH", "WSTETH", "RETH", "RSETH", "METH", "OSETH", "EZETH", "WBTC", "CBBTC", "LBTC", "SOLVBTC", "BUIDL", "WBNB"]
        if token not in filter_tokens:
            if not vt_symbol:
                binance_symbols = list(self.exchange_instruments_data.get("BINANCE", {}).keys())
                symbol = f"{token}USDT"
                if symbol in binance_symbols:
                    vt_symbol = f"{symbol}.BINANCE"
                    exchange = "BINANCE"
                    exchange_user = "lo-e"

            if not vt_symbol:
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

        if not vt_symbol:
            return {}
        
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
                   "start": True,
                   "datetime": datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
                   }
       
        return setting

    def check_rank_file_data(self):
        while True:
            try:
                current_dir = os.path.dirname(os.path.abspath(__file__))

                # 获取涨跌幅排行榜数据
                for duration in ["24h", "5m"]:
                    rise_list = []
                    fall_list = []

                    rise_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(rise_latest_file_path)
                    for _, row in df.iterrows():
                        rise_list.append(dict(row))

                    fall_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_fall{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(fall_latest_file_path)
                    for _, row in df.iterrows():
                        fall_list.append(dict(row))
                    
                    # 生成信号
                    if duration == "24h" and (self.rise_data_list_24h != rise_list or self.fall_data_list_24h != fall_list):
                        self.rise_data_list_24h= rise_list
                        self.fall_data_list_24h = fall_list
                        self.on_trending_data((rise_list, fall_list))

                    if duration == "5m" and (self.rise_data_list_5m != rise_list or self.fall_data_list_5m != fall_list):
                        self.rise_data_list_5m = rise_list
                        self.fall_data_list_5m = fall_list
                        self.on_rise_fall_data((rise_list, fall_list))

            except Exception as e:
                pass
            time.sleep(3)

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
                vt_symbol, direction = self.bar_download_queue.get(block=True, timeout=1)
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
                                contract_list=[symbol], hours=1, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BYBIT":
                            self.download_engine.download_from_bybit(
                                contract_list=[symbol], hours=1, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BINANCE":
                            self.download_engine.download_from_binance(
                                contract_list=[symbol], hours=1, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                        success = True

                    except Exception as e:
                        msg = f"TopGainersLosersPortfolio 下载Bar数据出错\n\n{e}"
                        self.send_ding_talk(msg)

                if success:
                    symbol_strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(symbol_strategies)):
                        strategy: TopGainersLosersStrategy = symbol_strategies[i]
                        if not strategy.indicator_inited and ((strategy.direction == Direction.LONG and direction == "LONG") or (strategy.direction == Direction.SHORT and direction == "SHORT")):
                            strategy.load_database_bar()

            except Empty:
                pass

            except Exception as e:
                pass
    
    def query_gateway_contract(self, gateway_names: list):
        for gateway_name in gateway_names:
            gateway = self.cta_engine.main_engine.get_default_gateway(gateway_name)
            if gateway:
                gateway.query_contract()
                
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

    def load_recent_trending_data(self):
        print_(f"加载历史趋势数据..")
        self.trending_tokens = {}
        hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=5)
        while hour_time < datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour
            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}24h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        rise_list = []
                        fall_list = []

                        rise_file_path = f"{root}{DIR_SYMBOL}{file}"
                        fall_file_path = rise_file_path.replace("rank_rise", "rank_fall")
                        if os.path.exists(fall_file_path):
                            df_rise = pd.read_csv(rise_file_path)
                            for _, row in df_rise.iterrows():
                                rise_list.append(dict(row))

                            df_fall = pd.read_csv(fall_file_path)
                            for _, row in df_fall.iterrows():
                                fall_list.append(dict(row))

                            # 生成信号
                            if self.rise_data_list_24h != rise_list or self.fall_data_list_24h != fall_list:
                                self.rise_data_list_24h= rise_list
                                self.fall_data_list_24h = fall_list
                                self.on_trending_data((rise_list, fall_list))

            hour_time += timedelta(hours=1)
        
        print_(f"历史趋势数据加载完成！")

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

            except Exception as e:
                msg = f"处理Tick数据出错\t{tick.vt_symbol}\t{tick.datetime}\n{e}"
                print_(msg)
                if time.time() >= error_notice_ts + 60:
                    error_notice_ts = time.time()
                    self.send_ding_talk(msg)

    def check_save_data(self):
        try:
            sync_data = {}
            for key in self.syncs:
                sync_data[key] = self.__getattribute__(key)

            if self.sync_data != sync_data:
                self.sync_data = copy.deepcopy(sync_data)
                self.cta_engine.put_portfolio_event()

                msg = f"同步组合数据.."
                print_(msg)
        
        except Exception as e:
            msg = f"保存组合数据出错\n\n{e}"
            self.send_ding_talk(msg)
            print_(msg)

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
                                # if gap > 0 and not strategy.insufficient_value:
                                #     # 多头开仓
                                #     trade_price = strategy.tick.last_price * 1.005
                                #     strategy.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                                
                                # elif gap < 0:
                                #     # 多头平仓
                                #     trade_price = strategy.tick.last_price * 0.995
                                #     strategy.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                                if gap < 0:
                                    # 多头平仓
                                    trade_price = strategy.tick.last_price * 0.995
                                    strategy.cancel_all()
                                    strategy.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap), market=True)

                            if strategy.direction == Direction.SHORT:
                                if strategy.target_pos > 0 or strategy.pos > 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy.target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = abs(strategy.target_pos) - abs(strategy.pos)
                                # if gap > 0 and not strategy.insufficient_value:
                                #     # 空头开仓
                                #     trade_price = strategy.tick.last_price * 0.995
                                #     strategy.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                                
                                # elif gap < 0:
                                #     # 空头平仓
                                #     trade_price = strategy.tick.last_price * 1.005
                                #     strategy.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                                if gap < 0:
                                    # 空头平仓
                                    trade_price = strategy.tick.last_price * 1.005
                                    strategy.cancel_all()
                                    strategy.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap), market=True)
                        
                        if strategy.close and not strategy.pos and (strategy.closed or not strategy.open_count):
                            # 取消订阅
                            symbol = ""
                            if "OKX" in strategy.vt_symbol:
                                symbol = strategy.vt_symbol.split("-USDT")[0]

                            else:
                                symbol = strategy.vt_symbol.split("USDT")[0]
                                
                            if symbol not in self.strategy_long_tokens and symbol not in self.strategy_short_tokens:
                                self.cta_engine.unsubscribe([strategy.vt_symbol])

                            # 策略引擎关闭策略
                            strategy.cta_engine.remove_strategy(strategy.strategy_name)

                        # 同步策略数据
                        strategy.check_save_data()

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
        # content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")