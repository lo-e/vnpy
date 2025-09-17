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
import re
import csv
import shutil

class TopGainersLosersPortfolio(object):
    parameters = ["name",
                  "funds"]

    syncs = [
        "account_ath",
        "account_drawdown",
        "signal_tokens_1h",
        "loss_list",
        "pnl"
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.funds = 0
        self.portfolio_value = 0
        self.inited = False
        self.started = False
        self.exchange_instruments_data = {}
        self.tick_queue = Queue()
        self.tick_ts = time.time()
        self.strategy_status_check_ts = {}
        self.rise_data_list_5m = []
        self.fall_data_list_5m = []
        self.rise_data_list_1h = []
        self.fall_data_list_1h = []
        self.rise_data_list_24h = []
        self.fall_data_list_24h = []
        self.liquidation_data = {}
        self.sync_data = {}
        self.unsubscribe_time = 0
        self.account_ath = 0
        self.account_drawdown = 0
        self.account_balance_data = {}
        self.account_notice_ts = 0
        self.fast_rise_tokens = []
        self.fast_fall_tokens = []
        self.strategy_long_tokens = []
        self.strategy_short_tokens = []
        self.trending_tokens_1h = {}
        self.rise_onboard_data_1h = {}
        self.fall_onboard_data_1h = {}
        self.signal_tokens_1h = {}
        self.trending_tokens_24h = {}
        self.history_trending_data = {}
        self.rise_onboard_symbol_time_dict = {}
        self.fall_onboard_symbol_time_dict = {}
        self.trade_enable = True
        self.pnl_data = {}
        self.loss_list = []
        self.pnl = 0
        
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
        # ------ fake ------
        # if not self.loss_list:
        #     loss_data = {"datetime": "2025-09-07 08:32:49",
        #                 "vt_symbol": "ALUUSDT.BYBIT",
        #                 "phase": 1,
        #                 "phase_lose": -1.24}
        #     self.loss_list.append(loss_data)
            
        # if abs(self.account_drawdown) < self.portfolio_value * 0.30:
        #     self.trade_enable = True

        # else:
        #     self.trade_enable = False

        # 导入交易所合约
        self.load_instruments_data()

        # 导入历史交易PNL
        self.load_history_pnl_data()

        # 导入最近趋势数据
        # self.load_recent_trending_data()

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
        # Thread(target=self.backtesting).start()

        # 下载当前策略Bar数据
        for name in self.cta_engine.strategies.copy().keys():
            strategy: TopGainersLosersStrategy = self.cta_engine.strategies[name]
            direction = ""
            if strategy.direction == Direction.LONG:
                direction = "LONG"

            elif strategy.direction == Direction.SHORT:
                direction = "SHORT"

            self.bar_download_queue.put((strategy.vt_symbol, direction))

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
            if strategy.pos:
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
                    # print_(msg)

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

        # 确认组合交易金额
        self.portfolio_value = self.account_ath - self.funds

        # 回撤过大停止交易
        # if abs(self.account_drawdown) < self.portfolio_value * 0.30:
        #     self.trade_enable = True

        # else:
        #     self.trade_enable = False

    def on_pnl(self, strategy: TopGainersLosersStrategy, pnl: float):
        # 记录盈亏
        dt = datetime.strptime(strategy.datetime, f"%Y-%m-%d %H:%M:%S")
        data = {"datetime": strategy.datetime,
                "timestamp": dt.timestamp(),
                "vt_symbol": strategy.vt_symbol,
                "direction": strategy.direction.value,
                "trending_mean_1h": strategy.trending_mean_1h,
                "reverse_mean_1h": strategy.reverse_mean_1h,
                "trending_mean_24h": strategy.trending_mean_24h,
                "reverse_mean_24h": strategy.reverse_mean_24h,
                "pnl": f"{pnl:.2f}%"}
        
        date_str = dt.strftime(f"%Y-%m-%d")
        date_data = self.pnl_data.get(date_str, {})
        date_data["updated"] = True
        data_list = date_data.get("data", [])
        data_list.append(data)
        date_data["data"] = data_list
        self.pnl_data[date_str] = date_data

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

    def on_trending_data_1h(self, data: tuple):
        rise_trending_list, fall_trending_list = data
        data_time = rise_trending_list[0]["change"]
        rise_trending_list = rise_trending_list[3:]
        fall_trending_list = fall_trending_list[3:]
        
        # 上榜时间
        is_init = True if not self.rise_onboard_data_1h else False
        rise_onboard_tokens = set()
        for i in range(len(rise_trending_list)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            rise_onboard_tokens.add(symbol)

            if symbol not in self.rise_onboard_data_1h:
                if is_init:
                    self.rise_onboard_data_1h[symbol] = 0

                else:
                    self.rise_onboard_data_1h[symbol] = data_time

        for symbol in self.rise_onboard_data_1h.copy().keys():
            if symbol not in rise_onboard_tokens:
                self.rise_onboard_data_1h.pop(symbol)

        fall_onboard_tokens = set()
        for i in range(len(fall_trending_list)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            fall_onboard_tokens.add(symbol)

            if symbol not in self.fall_onboard_data_1h:
                if is_init:
                    self.fall_onboard_data_1h[symbol] = 0

                else:
                    self.fall_onboard_data_1h[symbol] = data_time

        for symbol in self.fall_onboard_data_1h.copy().keys():
            if symbol not in fall_onboard_tokens:
                self.fall_onboard_data_1h.pop(symbol)
        
        # 1H趋势
        trending_tokens = set()
        for i in range(min(len(rise_trending_list), 10)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            volume = data["volume"]
            trending_tokens.add(symbol)

            trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
            trending_data = {
                "direction": "LONG",
                "change": change,
                "volume_24h": volume,
                "trending_1h_rank": i + 1,
                "trending_1h_ts": data_time,
                "trending_1h_time": trending_time,
                "trending_1h_start": data_time,
                "onboard_ts": self.rise_onboard_data_1h.get(symbol, 0)
                }
            
            history_trending_data = self.trending_tokens_1h.get(symbol, {})
            if history_trending_data:
                trending_data["trending_1h_start"] = history_trending_data["trending_1h_start"]
                    
            self.trending_tokens_1h[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 10)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            volume = data["volume"]
            trending_tokens.add(symbol)

            trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
            trending_data = {
                "direction": "SHORT",
                "change": change,
                "volume_24h": volume,
                "trending_1h_rank": i + 1,
                "trending_1h_ts": data_time,
                "trending_1h_time": trending_time,
                "trending_1h_start": data_time,
                "onboard_ts": self.fall_onboard_data_1h.get(symbol, 0)
                }
            
            history_trending_data = self.trending_tokens_1h.get(symbol, {})
            if history_trending_data:
                trending_data["trending_1h_start"] = history_trending_data["trending_1h_start"]
                    
            self.trending_tokens_1h[symbol] = trending_data

        # 信号判断
        new_settings = []
        for symbol, trending_data in self.trending_tokens_1h.copy().items():
            if symbol not in trending_tokens:
                self.trending_tokens_1h.pop(symbol)
            
            else:
                direction = trending_data["direction"]
                change = trending_data["change"]
                volume_24h = trending_data["volume_24h"]
                trending_1h_rank = trending_data["trending_1h_rank"]
                trending_1h_ts = trending_data["trending_1h_ts"]
                trending_1h_time = trending_data["trending_1h_time"]
                onboard_ts = trending_data["onboard_ts"]

                if abs(change) >= 1:
                    # 24h趋势数据
                    trending_mean_1h = 0
                    reverse_mean_1h = 0
                    trending_mean_24h = 0
                    reverse_mean_24h = 0
                    if change >= 0:
                        trending_mean_1h = self.rise_data_list_1h[1]["change"]
                        reverse_mean_1h = self.rise_data_list_1h[2]["change"]
                        trending_mean_24h = self.rise_data_list_24h[1]["change"]
                        reverse_mean_24h = self.rise_data_list_24h[2]["change"]
                    
                    else:
                        trending_mean_1h = self.rise_data_list_1h[2]["change"]
                        reverse_mean_1h = self.rise_data_list_1h[1]["change"]
                        trending_mean_24h = self.rise_data_list_24h[2]["change"]
                        reverse_mean_24h = self.rise_data_list_24h[1]["change"]

                    if abs(trending_mean_1h) >= abs(reverse_mean_1h) * 2 or abs(trending_mean_24h) >= abs(reverse_mean_24h) * 2:
                        # 信号生成
                        signal_dt_str = self.signal_tokens_1h.get(symbol, "")
                        signal_ts = datetime.strptime(signal_dt_str, f"%Y-%m-%d %H:%M:%S").timestamp() if signal_dt_str else 0
                        self.signal_tokens_1h[symbol] = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                        for signal_symbol, dt_str in self.signal_tokens_1h.copy().items():
                            ts = datetime.strptime(dt_str, f"%Y-%m-%d %H:%M:%S").timestamp() if dt_str else 0
                            if data_time >= ts + 0 * 60:
                                self.signal_tokens_1h.pop(signal_symbol)

                        if data_time >= signal_ts + 0 * 60:
                            # 过滤正在交易的相同代币
                            pure_symbol = re.sub(r'[^a-zA-Z]', '', symbol)
                            if direction == "LONG":
                                flt = False
                                for target_token in self.strategy_long_tokens:
                                    pure_target_token = re.sub(r'[^a-zA-Z]', '', target_token)
                                    if pure_symbol == pure_target_token:
                                        flt = True
                                        break

                                if not flt:
                                    setting = self.new_strategy(symbol, Direction.LONG, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                                    strategy_name = setting.get("strategy_name", "")
                                    pure_strategy_name = "_".join(strategy_name.split("_")[1:])
                                    for name in self.cta_engine.strategies.keys():
                                        pure_name = "_".join(name.split("_")[1:])
                                        if pure_strategy_name == pure_name:
                                            flt = True
                                            break
                                    
                                    if setting and not flt:
                                        vt_symbol = setting["vt_symbol"]
                                        instrument_data = self.exchange_instruments_data.get(vt_symbol.split(".")[-1], {}).get(vt_symbol.split(".")[0], {})
                                        on_timestamp = instrument_data["on_timestamp"]
                                        if on_timestamp and data_time >= on_timestamp + 5 * 24 * 60 * 60:
                                            new_settings.append(setting)
                                            if symbol not in self.strategy_long_tokens:
                                                self.strategy_long_tokens.append(symbol)

                                            # msg = f"{symbol} 上涨过热\n{vt_symbol} {change}%\ntime {trending_1h_time}\nvolume_24h {volume_24h}\nrank_1h {trending_1h_rank}"
                                            # self.send_ding_talk(msg)
                            
                            elif direction == "SHORT":
                                flt = False
                                for target_token in self.strategy_short_tokens:
                                    pure_target_token = re.sub(r'[^a-zA-Z]', '', target_token)
                                    if pure_symbol == pure_target_token:
                                        flt = True
                                        break
                                
                                if not flt:
                                    setting = self.new_strategy(symbol, Direction.SHORT, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                                    strategy_name = setting.get("strategy_name", "")
                                    pure_strategy_name = "_".join(strategy_name.split("_")[1:])
                                    for name in self.cta_engine.strategies.keys():
                                        pure_name = "_".join(name.split("_")[1:])
                                        if pure_strategy_name == pure_name:
                                            flt = True
                                            break
                                    
                                    if setting and not flt:
                                        vt_symbol = setting["vt_symbol"]
                                        instrument_data = self.exchange_instruments_data.get(vt_symbol.split(".")[-1], {}).get(vt_symbol.split(".")[0], {})
                                        on_timestamp = instrument_data["on_timestamp"]
                                        if on_timestamp and data_time >= on_timestamp + 5 * 24 * 60 * 60:
                                            new_settings.append(setting)
                                            if symbol not in self.strategy_short_tokens:
                                                self.strategy_short_tokens.append(symbol)

                                            # msg = f"{symbol} 下跌过热\n{vt_symbol} {change}%\ntime {trending_1h_time}\nvolume_24h {volume_24h}\nrank_1h {trending_1h_rank}"
                                            # self.send_ding_talk(msg)

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
            self.cta_engine.setting_update_queue.put(("new", new_settings))

            # 订阅合约
            self.cta_engine.subscribe(list(new_vt_symbols))

        # 排序
        self.trending_tokens_1h = dict(sorted(self.trending_tokens_1h.items()))

    def on_trending_data_24h(self, data: tuple):
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
                if symbol not in self.trending_tokens_24h:
                    on_board_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"change": change,
                                     "on_board_ts": data_time,
                                     "on_board": on_board_time}

                    history_data = self.history_trending_data.get(symbol, {})
                    if history_data:
                        pop_ts = history_data["pop_ts"]
                        if data_time <= pop_ts + 1 * 60 * 60:
                            trending_data = {"change": history_data["change"],
                                             "on_board_ts": history_data["on_board_ts"],
                                             "on_board": history_data["on_board"]}
                            
                        self.history_trending_data.pop(symbol)
                            
                    self.trending_tokens_24h[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 20)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            if abs(change) >= 20.0 or (abs(change) >= 15.0 and i < 10):
                trending_tokens.add(symbol)
                if symbol not in self.trending_tokens_24h:
                    on_board_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"change": change,
                                     "on_board_ts": data_time,
                                     "on_board": on_board_time}
                    
                    history_data = self.history_trending_data.get(symbol, {})
                    if history_data:
                        pop_ts = history_data["pop_ts"]
                        if data_time <= pop_ts + 1 * 60 * 60:
                            trending_data = {"change": history_data["change"],
                                             "on_board_ts": history_data["on_board_ts"],
                                             "on_board": history_data["on_board"]}
                            
                        self.history_trending_data.pop(symbol)
                            
                    self.trending_tokens_24h[symbol] = trending_data
        
        for symbol in self.trending_tokens_24h.copy().keys():
            if symbol not in trending_tokens:
                trending_data = self.trending_tokens_24h[symbol]
                self.history_trending_data[symbol] = {"pop_ts": data_time,
                                                      "change": trending_data["change"],
                                                      "on_board_ts": trending_data["on_board_ts"],
                                                      "on_board": trending_data["on_board"]}
                self.trending_tokens_24h.pop(symbol)
        
        # 排序
        self.trending_tokens_24h = dict(sorted(self.trending_tokens_24h.items()))

        # 趋势停止判断
        # for symbol, data in self.history_trending_data.copy().items():
        #     pop_ts = data["pop_ts"]
        #     if data_time > pop_ts + 1 * 60 * 60:
        #         change = data["change"]
        #         on_board_ts = data["on_board_ts"]
        #         on_board = data["on_board"]
        #         self.history_trending_data.pop(symbol)

        #         if not self.inited:
        #             off_board = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
        #             boarding_time = int(data_time - on_board_ts)
        #             boarding_hour = int(boarding_time / 3600)
        #             boarding_minute = int((boarding_time - (boarding_hour * 3600)) / 60)
        #             boarding_second = int(boarding_time - boarding_hour * 3600 - boarding_minute * 60)
        #             msg = f"趋势停止 {symbol}\nchange：{change}\non：{on_board}\noff：{off_board}\ntime：{boarding_hour}h {boarding_minute}m {boarding_second}s\n"
        #             print(msg)
        #             # self.send_ding_talk(msg)

    def on_trending_data_5m(self, data: tuple):
        pass

    def new_strategy(self, token:str, direction: Direction, trending_mean_1h: float, reverse_mean_1h: float, trending_mean_24h: float, reverse_mean_24h: float):
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

            # if not vt_symbol:
            #     okx_symbols = list(self.exchange_instruments_data.get("OKX", {}).keys())
            #     symbol = f"{token}-USDT-SWAP"
            #     if symbol in okx_symbols:
            #         vt_symbol = f"{symbol}.OKX"
            #         exchange = "OKX"
            #         exchange_user = "lo-e"

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
        dt = datetime.now().strftime(f"%m%d%H%M%S")
        if direction == Direction.LONG:
            strategy_name = f"{dt}_LONG_1H_{token}_{exchange}"
            direction_str = "LONG"
        
        else:
            strategy_name = f"{dt}_SHORT_1H_{token}_{exchange}"
            direction_str = "SHORT"

        setting = {"strategy_name": strategy_name,
                   "vt_symbol": vt_symbol,
                   "exchange": exchange,
                   "exchange_user": exchange_user,
                   "direction": direction_str,
                   "trending_mean_1h": trending_mean_1h,
                   "reverse_mean_1h": reverse_mean_1h,
                   "trending_mean_24h": trending_mean_24h,
                   "reverse_mean_24h": reverse_mean_24h,
                   "start": True,
                   "manual_close": False,
                   "datetime": datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
                   }
       
        return setting

    def check_rank_file_data(self):
        while True:
            try:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                
                # 获取清算数据
                liquidation_data = {}
                liquidation_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}liquidation{DIR_SYMBOL}latest.csv"
                df = pd.read_csv(liquidation_latest_file_path)
                for _, row in df.iterrows():
                    liquidation_data = dict(row)
                if self.liquidation_data != liquidation_data:
                    self.liquidation_data = liquidation_data

                # 获取涨跌幅排行榜数据
                for duration in ["24h", "1h"]:
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
                        # self.on_trending_data_24h((rise_list, fall_list))

                    if duration == "1h" and (self.rise_data_list_1h != rise_list or self.fall_data_list_1h != fall_list):
                        self.rise_data_list_1h= rise_list
                        self.fall_data_list_1h = fall_list
                        self.on_trending_data_1h((rise_list, fall_list))

                    if duration == "5m" and (self.rise_data_list_5m != rise_list or self.fall_data_list_5m != fall_list):
                        self.rise_data_list_5m = rise_list
                        self.fall_data_list_5m = fall_list
                        # self.on_trending_data_5m((rise_list, fall_list))

                # 检查tick行情推送是否异常
                tick_wait = time.time() - self.tick_ts
                if self.cta_engine.strategies and tick_wait > 60:
                    self.tick_ts = time.time()
                    msg = f"TICK推送异常，检查线程阻塞"
                    self.send_ding_talk(msg)

            except Exception as e:
                pass
            time.sleep(3)

    def backtesting(self):
        # 1小时趋势数据
        print(f"加载1H历史趋势数据..")
        self.trending_tokens_1h = {}
        hour_time = datetime.strptime(f"2025-08-23 20:00:00", f"%Y-%m-%d %H:%M:%S")
        # hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=5)
        while hour_time < datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
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

                            data_time = rise_list[0]["change"]

                            # 获取24小时趋势数据
                            hour_time_24h = hour_time - timedelta(hours=1)
                            searching_end_24h = False
                            while not searching_end_24h:
                                date_24h = hour_time_24h.strftime(f"%Y-%m-%d")
                                hour_24h = hour_time_24h.hour
                                dir_path_24h = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}24h{DIR_SYMBOL}{date_24h}{DIR_SYMBOL}{hour_24h}"
                                if os.path.exists(dir_path_24h):
                                    for root_24h, _, files_24h in os.walk(dir_path_24h):
                                        for file_24h in files_24h:
                                            rise_list_24h = []
                                            fall_list_24h = []

                                            rise_file_path_24h = f"{root_24h}{DIR_SYMBOL}{file_24h}"
                                            fall_file_path_24h = rise_file_path_24h.replace("rank_rise", "rank_fall")
                                            if os.path.exists(fall_file_path_24h):
                                                df_rise_24h = pd.read_csv(rise_file_path_24h)
                                                for _, row in df_rise_24h.iterrows():
                                                    rise_list_24h.append(dict(row))

                                                df_fall_24h = pd.read_csv(fall_file_path_24h)
                                                for _, row in df_fall_24h.iterrows():
                                                    fall_list_24h.append(dict(row))
                                                
                                                data_time_24h = rise_list_24h[0]["change"]
                                                if data_time_24h and data_time_24h >= data_time:
                                                    searching_end_24h = True
                                                    break
                                                
                                                else:
                                                    self.rise_data_list_24h = rise_list_24h
                                                    self.fall_data_list_24h = fall_list_24h

                                            else:
                                                raise(f"24h文件状态异常，检查代码")
                                            
                                hour_time_24h += timedelta(hours=1)

                            # 生成信号
                            self.on_trending_data_1h((rise_list, fall_list))
                        
                        else:
                            raise(f"1h文件状态异常，检查代码")
            
            hour_time += timedelta(hours=1)
        
        print(f"历史趋势数据加载完成！")

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
                                contract_list=[symbol], hours=7, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BYBIT":
                            self.download_engine.download_from_bybit(
                                contract_list=[symbol], hours=7, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BINANCE":
                            self.download_engine.download_from_binance(
                                contract_list=[symbol], hours=7, from_data_base=False, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                        success = True

                    except Exception as e:
                        msg = f"TopGainersLosersPortfolio 下载Bar数据出错\n\n{e}"
                        self.send_ding_talk(msg)

                if success:
                    symbol_strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(symbol_strategies)):
                        strategy: TopGainersLosersStrategy = symbol_strategies[i]
                        if (strategy.direction == Direction.LONG and direction == "LONG") or (strategy.direction == Direction.SHORT and direction == "SHORT"):
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

    def load_history_pnl_data(self):
        # 获取历史交易日志
        current_dir = os.path.dirname(os.path.abspath(__file__))
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for i in range(7):
            date_str = (today - timedelta(days=i)).strftime(f"%Y-%m-%d")
            file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_pnls{DIR_SYMBOL}{date_str}.csv"
            if os.path.exists(file_path):
                data_list = []
                df = pd.read_csv(file_path)
                for _, row in df.iterrows():
                    data_list.append(dict(row))
                self.pnl_data[date_str] = {"updated": False,
                                           "data": data_list}

    def load_recent_trending_data(self):
        # 1小时趋势数据
        print_(f"加载1H历史趋势数据..")
        self.trending_tokens_1h = {}
        hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)
        while hour_time < datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
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
                            if self.rise_data_list_1h != rise_list or self.fall_data_list_1h != fall_list:
                                self.rise_data_list_1h= rise_list
                                self.fall_data_list_1h = fall_list
                                self.on_trending_data_1h((rise_list, fall_list))

            hour_time += timedelta(hours=1)

        # 24小时趋势数据
        print_(f"加载24H历史趋势数据..")
        self.trending_tokens_24h = {}
        hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=3)
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
                                # self.on_trending_data_24h((rise_list, fall_list))
            
            hour_time += timedelta(hours=1)
        
        print_(f"历史趋势数据加载完成！")

    def process_tick(self):
        error_notice_ts = 0
        queue_size_ts = time.time()
        process_count = 0
        while True:
            try:
                tick: TickData = self.tick_queue.get(block=True, timeout=1)
                self.tick_ts = time.time()
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
            # 保存变量、同步数据
            sync_data = {}
            for key in self.syncs:
                sync_data[key] = self.__getattribute__(key)

            if self.sync_data != sync_data:
                self.sync_data = copy.deepcopy(sync_data)
                self.cta_engine.put_portfolio_event()

                msg = f"同步组合数据.."
                print_(msg)

            # 保存组合盈亏数据
            for date_str, date_data in self.pnl_data.items():
                updated = date_data["updated"]
                if updated:
                    current_dir = os.path.dirname(os.path.abspath(__file__))
                    file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_pnls{DIR_SYMBOL}{date_str}.csv"

                    data_list = date_data["data"]
                    data_list = sorted(data_list, key=lambda x: x['timestamp'])
                    field_names = list(data_list[0].keys())
                    self.save_csv_data(field_names, data_list, file_path, True)
                    date_data["updated"] = False
        
        except Exception as e:
            msg = f"保存组合数据出错\n\n{e}"
            self.send_ding_talk(msg)
            print_(msg)

    def save_csv_data(self, field_names: list, data: list, file_path:str, check_dir: bool = True):
        # 确保文件夹存在
        if check_dir:
            file_elements = file_path.split(DIR_SYMBOL)
            dir_path = DIR_SYMBOL.join(file_elements[:-1])
            os.makedirs(dir_path, exist_ok=True)

        # 保存到临时csv文件
        temp_file_path = file_path.split(".csv")[0] + f"_temp.csv"
        with open(temp_file_path, "w", encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerows(data)

        # 将临时文件替换为目标文件
        shutil.move(temp_file_path, file_path)

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
                        
                        if strategy.closed and not strategy.pos:
                            vt_orderids = self.cta_engine.strategy_orderid_map[strategy.strategy_name]
                            if vt_orderids:
                                strategy.cancel_all()
                            
                            else:
                                # 取消订阅
                                symbol = ""
                                if "OKX" in strategy.vt_symbol:
                                    symbol = strategy.vt_symbol.split("-USDT")[0]

                                else:
                                    symbol = strategy.vt_symbol.split("USDT")[0]
                                    
                                if strategy.direction == Direction.LONG and symbol in self.strategy_long_tokens:
                                    self.strategy_long_tokens.remove(symbol)

                                if strategy.direction == Direction.SHORT and symbol in self.strategy_short_tokens:
                                    self.strategy_short_tokens.remove(symbol)

                                if symbol not in self.strategy_long_tokens and symbol not in self.strategy_short_tokens:
                                    self.cta_engine.unsubscribe([strategy.vt_symbol])

                                # 清除setting
                                self.cta_engine.setting_update_queue.put(("remove", [strategy.strategy_name]))

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