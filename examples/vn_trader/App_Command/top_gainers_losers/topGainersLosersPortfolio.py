# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
import time
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL, round_to
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from vnpy.trader.constant import Direction, Offset, Exchange, OrderType
from App.Turtle_crypto.dataservice.utility import get_csv_path
import pandas as pd
import os
from vnpy.trader.object import BarData, TickData, TradeData, CancelRequest, ContractData
from vnpy.event import Event
from vnpy.trader.object import SubscribeRequest
from .trendingStrategy import TrendingStrategy, get_strategy_pure_name, get_strategy_type
from .trendingMultiStrategy import TrendingMultiStrategy, SignalData, SIGNALS
from queue import Empty, Queue
from vnpy.trader.event import EVENT_TICK_DELAY, EVENT_ACCOUNT, EVENT_GATEWAY_LEVERAGE_FAILED, EVENT_GATEWAY_FUNDING_RATES, EVENT_TRADE
from vnpy.trader.object import AccountData
import copy
import re
import requests
import csv
import shutil

PORTFOLIO_VALUE_DEFAULT = 100

class TopGainersLosersPortfolio(object):
    parameters = ["name",
                  "funds"]

    syncs = [
        "account_ath",
        "account_drawdown",
        "loss_list",
        "pnl"
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.funds = 0
        self.portfolio_value = PORTFOLIO_VALUE_DEFAULT
        self.inited = False
        self.started = False
        self.exchange_instruments_data = {}
        self.tick_queue = Queue()
        self.tick_ts = time.time()
        self.strategy_status_check_ts = {}
        self.rise_data_list_5m_coinglass = []
        self.fall_data_list_5m_coinglass = []
        self.rise_data_list_1h_coinglass = []
        self.fall_data_list_1h_coinglass = []
        self.rise_data_list_24h_coinglass = []
        self.fall_data_list_24h_coinglass = []
        self.rise_data_list_24h_bybit = []
        self.fall_data_list_24h_bybit = []
        self.liquidation_data = {}
        self.sync_data = {}
        self.unsubscribe_time = 0
        self.account_ath = 0
        self.account_drawdown = 0
        self.account_balance_data = {}
        self.account_notice_ts = 0
        self.fast_rise_tokens = []
        self.fast_fall_tokens = []
        self.trending_tokens_1h = {}
        self.trending_tokens_24h = {}
        self.history_trending_data = {}
        self.rise_onboard_symbol_time_dict = {}
        self.fall_onboard_symbol_time_dict = {}
        self.trade_enable = True
        self.pnl_data = {}
        self.loss_list = []
        self.pnl = 0
        self.setting_update_needed = False
        self.default_leverage = 20
        self.optional_leverage = 10

        """ fake """
        # self.test_order_time = 0
        # self.test_orderid = ""

        # 监听事件
        self.cta_engine.event_engine.register(EVENT_GATEWAY_LEVERAGE_FAILED, self.process_leverage_failed_event)    
        self.cta_engine.event_engine.register(EVENT_GATEWAY_FUNDING_RATES, self.process_funding_rates_event)
        # self.cta_engine.event_engine.register(EVENT_TRADE, self.process_trade_event)
        
        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()
        self.download_instruments_time: datetime = None
        self.query_funding_rate_time: datetime = None
        self.account_dingtalk_ts: float = 0
        self.update_leverage_time: datetime = None
        self.instruments_downloading = False
        self.bar_download_queue = Queue()

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        # 导入交易所合约
        self.load_instruments_data()

        # 导入历史交易PNL
        self.load_history_pnl_data()

        # 监控行情数据延迟事件
        # self.cta_engine.event_engine.register(EVENT_TICK_DELAY, self.resubscribe)
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
        download_vt_symbols = set()
        for name in self.cta_engine.strategies.copy().keys():
            strategy: TrendingMultiStrategy = self.cta_engine.strategies[name]
            download_vt_symbols.add(strategy.vt_symbol)
        
        for vt_symbol in download_vt_symbols:
            self.bar_download_queue.put(vt_symbol)

    def on_timer(self):
        if not self.started:
            return

        # 下载合约列表数据
        now = datetime.now()
        current_hour_time = now.replace(minute=0, second=0, microsecond=0)
        if self.download_instruments_time != current_hour_time:
            self.download_instruments_time = current_hour_time
            self.check_download_instruments()

        # 查询资金费率
        if self.query_funding_rate_time != current_hour_time and now.minute >= 59:
            self.query_funding_rate_time = current_hour_time

            # gateway = self.cta_engine.main_engine.get_default_gateway("OKX")
            # if gateway:
            #     gateway.query_funding_rate()
            
            gateway = self.cta_engine.main_engine.get_default_gateway("BINANCE")
            if gateway:
                gateway.query_funding_rate()

            # gateway = self.cta_engine.main_engine.get_default_gateway("BYBIT")
            # if gateway:
            #     gateway.query_funding_rate()

        # 定期更新交易所合约杠杆
        if self.update_leverage_time != current_hour_time and now.hour == 1 and now.minute >= 5:
            self.update_leverage_time = current_hour_time
            self.update_leverage()

        # 重新订阅
        if self.unsubscribe_time and time.time() - self.unsubscribe_time >= 5:
            self.unsubscribe_time = 0
            self.subscribe_strategies()

        # 保存同步数据
        self.check_save_data()

        """ fake """
        """
        symbol = "LIGHTUSDT"
        exchange = Exchange.BINANCE
        vt_symbol = f"{symbol}.{exchange.value}"
        if not self.test_order_time:
            self.test_order_time = time.time()
            open_direction = Direction.LONG
            close_direction = Direction.SHORT
            vt_orderids = self.cta_engine.send_simple_order(vt_symbol,
                                                            close_direction,
                                                            Offset.CLOSE,
                                                            0.5340,
                                                            10,
                                                            OrderType.STOP)
            self.test_orderid = vt_orderids[0].split(".")[1]
        
        # if self.test_order_time and time.time() - self.test_order_time >= 5:
        #     self.test_order_time = time.time() + 9999999
        #     req = CancelRequest(orderid=self.test_orderid,
        #                         symbol=symbol,
        #                         exchange=exchange)
        #     self.cta_engine.main_engine.cancel_order(req, "BINANCE")
        """

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
            strategy: TrendingMultiStrategy = self.cta_engine.strategies[strategy_name]
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
        # self.portfolio_value = self.account_ath - self.funds

        # 回撤过大停止交易
        # if abs(self.account_drawdown) < self.portfolio_value * 0.30:
        #     self.trade_enable = True

        # else:
        #     self.trade_enable = False

    def on_pnl(self, strategy: TrendingMultiStrategy, signal: SignalData, pnl: float):
        # 记录盈亏
        dt = datetime.strptime(strategy.datetime, f"%Y-%m-%d %H:%M:%S")
        data = {"datetime": strategy.datetime,
                "timestamp": dt.timestamp(),
                "vt_symbol": strategy.vt_symbol,
                "direction": strategy.direction.value,
                "change": strategy.change,
                "volume_24h": signal.open_volume_24h,
                "trending_mean_1h": strategy.trending_mean_1h,
                "reverse_mean_1h": strategy.reverse_mean_1h,
                "trending_mean_24h": strategy.trending_mean_24h,
                "reverse_mean_24h": strategy.reverse_mean_24h,
                "pnl": f"{pnl:.2f}%",
                "open_tags": signal.open_tags}
        
        date_str = dt.strftime(f"%Y-%m-%d")
        signal_data = self.pnl_data.get(signal.name, {})
        date_data = signal_data.get(date_str, {})

        date_data["updated"] = True
        data_list = date_data.get("data", [])
        data_list.append(data)
        date_data["data"] = data_list

        signal_data[date_str] = date_data
        self.pnl_data[signal.name] = signal_data

    def resubscribe(self, event: Event):
        # 取消订阅
        self.subscribe_strategies(unsubscribe=True)

        # 记录取消订阅时间
        self.unsubscribe_time = time.time()

    def subscribe_strategies(self, unsubscribe: bool = False):
        vt_symbols = set()
        for strategy_name in self.cta_engine.strategies.keys():
            strategy: TrendingMultiStrategy = self.cta_engine.strategies[strategy_name]
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
        
        # 1H趋势
        trending_tokens = set()
        for i in range(min(len(rise_trending_list), 20)):
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
                "trending_1h_start": data_time
                }
            
            history_trending_data = self.trending_tokens_1h.get(symbol, {})
            if history_trending_data:
                trending_data["trending_1h_start"] = history_trending_data["trending_1h_start"]
                    
            self.trending_tokens_1h[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 20)):
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
                "trending_1h_start": data_time
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
                trending_1h_rank = trending_data["trending_1h_rank"]
                trending_1h_ts = trending_data["trending_1h_ts"]
                trending_1h_time = trending_data["trending_1h_time"]
                volume_24h = trending_data["volume_24h"]
                volume_24h_v = float(re.sub(r'[^\d.]', '', volume_24h))
                volume_24h_u = re.sub(r'[\d.]', '', volume_24h)
                volume_cross = True
                if volume_24h_u == "万" and volume_24h_v < 100:
                    volume_cross = False

                if abs(change) >= 1 and volume_cross:
                    # 24h趋势数据
                    trending_mean_1h = 0
                    reverse_mean_1h = 0
                    trending_mean_24h = 0
                    reverse_mean_24h = 0
                    if change >= 0:
                        trending_mean_1h = self.rise_data_list_1h_coinglass[1]["change"]
                        reverse_mean_1h = self.rise_data_list_1h_coinglass[2]["change"]
                        trending_mean_24h = self.rise_data_list_24h_coinglass[1]["change"]
                        reverse_mean_24h = self.rise_data_list_24h_coinglass[2]["change"]
                    
                    else:
                        trending_mean_1h = self.rise_data_list_1h_coinglass[2]["change"]
                        reverse_mean_1h = self.rise_data_list_1h_coinglass[1]["change"]
                        trending_mean_24h = self.rise_data_list_24h_coinglass[2]["change"]
                        reverse_mean_24h = self.rise_data_list_24h_coinglass[1]["change"]

                    """
                    # T1信号生成
                    if (abs(trending_mean_1h) >= abs(reverse_mean_1h) * 2) or (abs(trending_mean_24h) >= abs(reverse_mean_24h) * 2):
                        if direction == "LONG":
                            setting = self.new_strategy(trending_1h_time, "T1", symbol, Direction.LONG, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)
                        
                        elif direction == "SHORT":
                            setting = self.new_strategy(trending_1h_time, "T1", symbol, Direction.SHORT, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)

                    # T2信号生成
                    if (abs(trending_mean_1h) >= abs(reverse_mean_1h) * 2) or (abs(trending_mean_24h) >= abs(reverse_mean_24h) * 2):
                        if direction == "LONG":
                            setting = self.new_strategy(trending_1h_time, "T2", symbol, Direction.LONG, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)
                        
                        elif direction == "SHORT":
                            setting = self.new_strategy(trending_1h_time, "T2", symbol, Direction.SHORT, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)

                    # T3信号生成
                    if (abs(trending_mean_1h) >= abs(reverse_mean_1h) * 2) or (abs(trending_mean_24h) >= abs(reverse_mean_24h) * 2):
                        if direction == "LONG":
                            setting = self.new_strategy(trending_1h_time, "T3", symbol, Direction.LONG, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)
                        
                        elif direction == "SHORT":
                            setting = self.new_strategy(trending_1h_time, "T3", symbol, Direction.SHORT, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)

                    # T4信号生成
                    if (abs(trending_mean_1h) >= abs(reverse_mean_1h) * 2) or (abs(trending_mean_24h) >= abs(reverse_mean_24h) * 2):
                        if direction == "LONG":
                            setting = self.new_strategy(trending_1h_time, "T4", symbol, Direction.LONG, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)
                        
                        elif direction == "SHORT":
                            setting = self.new_strategy(trending_1h_time, "T4", symbol, Direction.SHORT, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                            self.generate_new_setting(data_time, direction, setting, new_settings)

                    # T5信号生成
                    if direction == "LONG":
                        setting = self.new_strategy(trending_1h_time, "T5", symbol, Direction.LONG, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                        self.generate_new_setting(data_time, direction, setting, new_settings)
                    
                    elif direction == "SHORT":
                        setting = self.new_strategy(trending_1h_time, "T5", symbol, Direction.SHORT, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                        self.generate_new_setting(data_time, direction, setting, new_settings)
                    """

                    setting = self.new_strategy(trending_1h_time, "MULTI", symbol, direction, round(change, 2), volume_24h, round(trending_mean_1h, 2), round(reverse_mean_1h, 2), round(trending_mean_24h, 2), round(reverse_mean_24h, 2))
                    self.generate_new_setting(data_time, direction, setting, new_settings)

        if new_settings:
            # 统计当前已订阅的代币
            subscribed_vt_symbols = set()
            for name in self.cta_engine.strategies.copy().keys():
                strategy: TrendingMultiStrategy = self.cta_engine.strategies[name]
                subscribed_vt_symbols.add(strategy.vt_symbol)

            # 执行新策略
            subscribe_vt_symbols = set()
            download_vt_symbols = set()
            for setting in new_settings:
                self.cta_engine.new_strategy(setting)

                vt_symbol = setting["vt_symbol"]
                if vt_symbol not in subscribed_vt_symbols:
                    subscribe_vt_symbols.add(vt_symbol)
                download_vt_symbols.add(vt_symbol)

            # 更新setting.json
            self.setting_update_needed = True

            # 订阅合约
            self.cta_engine.subscribe(list(subscribe_vt_symbols))

            # 下载bar数据
            for vt_symbol in download_vt_symbols:
                self.bar_download_queue.put(vt_symbol)

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

    def generate_new_setting(self, data_time: float, direction: str, setting: dict, new_settings: list):
        strategy_name = setting.get("strategy_name", "")
        strategy_pure_name = get_strategy_pure_name(strategy_name)

        # 过滤正在交易的相同代币
        flt = False
        for target_name in self.cta_engine.strategies.copy().keys():
            target_pure_name = get_strategy_pure_name(target_name)
            if strategy_pure_name == target_pure_name:
                flt = True
                break

        for target_setting in new_settings:
            target_name = target_setting.get("strategy_name", "")
            target_pure_name = get_strategy_pure_name(target_name)
            if strategy_pure_name == target_pure_name:
                flt = True
                break
        
        if setting and not flt:
            vt_symbol = setting["vt_symbol"]
            instrument_data = self.exchange_instruments_data.get(vt_symbol.split(".")[-1], {}).get(vt_symbol.split(".")[0], {})
            on_timestamp = instrument_data["on_timestamp"]
            if on_timestamp and data_time >= on_timestamp + 3 * 24 * 60 * 60:
                new_settings.append(setting)

        # if direction == "LONG":
        #     msg = f"{symbol} 上涨过热\n{vt_symbol} {change}%\ntime {trending_1h_time}\nvolume_24h {volume_24h}\nrank_1h {trending_1h_rank}"
        #     self.send_ding_talk(msg)
        
        # elif direction == "SHORT":
        #     msg = f"{symbol} 下跌过热\n{vt_symbol} {change}%\ntime {trending_1h_time}\nvolume_24h {volume_24h}\nrank_1h {trending_1h_rank}"
        #     self.send_ding_talk(msg)

    def new_strategy(self, data_dt: str, type: str, token: str, direction: Direction, change: float, volume_24h: str, trending_mean_1h: float, reverse_mean_1h: float, trending_mean_24h: float, reverse_mean_24h: float):
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
        strategy_name = f"{dt}_{direction}_{type}_{token}_{exchange}"
        setting = {"strategy_name": strategy_name,
                   "vt_symbol": vt_symbol,
                   "exchange": exchange,
                   "exchange_user": exchange_user,
                   "direction": direction,
                   "change": change,
                   "volume_24h": volume_24h,
                   "trending_mean_1h": trending_mean_1h,
                   "reverse_mean_1h": reverse_mean_1h,
                   "trending_mean_24h": trending_mean_24h,
                   "reverse_mean_24h": reverse_mean_24h,
                   "start": True,
                   "manual_close": False,
                   "data_dt": data_dt,
                   "datetime": datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
                   }
       
        return setting

    def check_rank_file_data(self):
        while True:
            try:
                current_dir = os.path.dirname(os.path.abspath(__file__))
                
                # 获取清算数据
                liquidation_data = {}
                liquidation_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}coinglass{DIR_SYMBOL}liquidation{DIR_SYMBOL}latest.csv"
                df = pd.read_csv(liquidation_latest_file_path)
                for _, row in df.iterrows():
                    liquidation_data = dict(row)
                if self.liquidation_data != liquidation_data:
                    self.liquidation_data = liquidation_data

                # 获取coinglass涨跌幅排行榜数据
                for duration in ["24h", "1h"]:
                    rise_list = []
                    fall_list = []

                    rise_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}coinglass{DIR_SYMBOL}rank_rise{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(rise_latest_file_path)
                    for _, row in df.iterrows():
                        rise_list.append(dict(row))
                    rise_ts = rise_list[0]["change"] if rise_list else 0

                    fall_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}coinglass{DIR_SYMBOL}rank_fall{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(fall_latest_file_path)
                    for _, row in df.iterrows():
                        fall_list.append(dict(row))
                    fall_ts = fall_list[0]["change"] if fall_list else 0
                    
                    # 生成信号
                    rise_data_24h_ts = self.rise_data_list_24h_coinglass[0]["change"] if self.rise_data_list_24h_coinglass else 0
                    fall_data_24h_ts = self.fall_data_list_24h_coinglass[0]["change"] if self.fall_data_list_24h_coinglass else 0
                    if duration == "24h" and (rise_data_24h_ts != rise_ts or fall_data_24h_ts != fall_ts):
                        self.rise_data_list_24h_coinglass = rise_list
                        self.fall_data_list_24h_coinglass = fall_list
                        # self.on_trending_data_24h((rise_list, fall_list))

                    rise_data_1h_ts = self.rise_data_list_1h_coinglass[0]["change"] if self.rise_data_list_1h_coinglass else 0
                    fall_data_1h_ts = self.fall_data_list_1h_coinglass[0]["change"] if self.fall_data_list_1h_coinglass else 0
                    if duration == "1h" and (rise_data_1h_ts != rise_ts or fall_data_1h_ts != fall_ts):
                        self.rise_data_list_1h_coinglass = rise_list
                        self.fall_data_list_1h_coinglass = fall_list
                        self.on_trending_data_1h((rise_list, fall_list))

                    rise_data_5m_ts = self.rise_data_list_5m_coinglass[0]["change"] if self.rise_data_list_5m_coinglass else 0
                    fall_data_5m_ts = self.fall_data_list_5m_coinglass[0]["change"] if self.fall_data_list_5m_coinglass else 0
                    if duration == "5m" and (rise_data_5m_ts != rise_ts or fall_data_5m_ts != fall_ts):
                        self.rise_data_list_5m_coinglass = rise_list
                        self.fall_data_list_5m_coinglass = fall_list
                        # self.on_trending_data_5m((rise_list, fall_list))

                # 获取bybit涨跌幅排行榜数据
                for duration in ["24h"]:
                    rise_list = []
                    fall_list = []

                    rise_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}bybit{DIR_SYMBOL}rank_rise{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(rise_latest_file_path)
                    for _, row in df.iterrows():
                        rise_list.append(dict(row))
                    rise_ts = rise_list[0]["change"] if rise_list else 0

                    fall_latest_file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}bybit{DIR_SYMBOL}rank_fall{DIR_SYMBOL}{duration}{DIR_SYMBOL}latest.csv"
                    df = pd.read_csv(fall_latest_file_path)
                    for _, row in df.iterrows():
                        fall_list.append(dict(row))
                    fall_ts = fall_list[0]["change"] if fall_list else 0

                    rise_data_24h_ts = self.rise_data_list_24h_bybit[0]["change"] if self.rise_data_list_24h_bybit else 0
                    fall_data_24h_ts = self.fall_data_list_24h_bybit[0]["change"] if self.fall_data_list_24h_bybit else 0
                    if duration == "24h" and (rise_data_24h_ts != rise_ts or fall_data_24h_ts != fall_ts):
                        self.rise_data_list_24h_bybit = rise_list
                        self.fall_data_list_24h_bybit = fall_list

                # 检查tick行情推送是否异常
                tick_wait = time.time() - self.tick_ts
                if self.cta_engine.strategies and tick_wait > 60:
                    self.tick_ts = time.time()
                    msg = f"TICK推送异常，检查线程阻塞"
                    self.send_ding_talk(msg)

            except Exception as e:
                msg = f"处理涨跌幅排行数据出错\n\n{e}"
                self.send_ding_talk(msg)
                
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

            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}coinglass{DIR_SYMBOL}rank_rise{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
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
                                dir_path_24h = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}coinglass{DIR_SYMBOL}rank_rise{DIR_SYMBOL}24h{DIR_SYMBOL}{date_24h}{DIR_SYMBOL}{hour_24h}"
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
                                                    self.rise_data_list_24h_coinglass = rise_list_24h
                                                    self.fall_data_list_24h_coinglass = fall_list_24h

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
                    msg = f"TopGainersLosersPortfolio 下载合约列表数据出错\n\n{e}"
                    self.send_ding_talk(msg)

            # 交易所合约上新，更新Gateway合约列表
            update_contract_gateway_names = set()
            new_vt_symbols = set()
            new_vt_symbols_okx = set()
            new_vt_symbols_binance = set()
            new_vt_symbols_bybit = set()
            if okx_new:
                update_contract_gateway_names.add("OKX")
                for instrument in okx_new:
                    symbol = instrument["symbol"]
                    vt_symbol = f"{symbol}.OKX"
                    new_vt_symbols.add(vt_symbol)
                    new_vt_symbols_okx.add(vt_symbol)
            
            if binance_new:
                update_contract_gateway_names.add("BINANCE")
                for instrument in binance_new:
                    symbol = instrument["symbol"]
                    vt_symbol = f"{symbol}.BINANCE"
                    new_vt_symbols.add(vt_symbol)
                    new_vt_symbols_binance.add(vt_symbol)

            if bybit_new:
                update_contract_gateway_names.add("BYBIT")
                for instrument in bybit_new:
                    symbol = instrument["symbol"]
                    vt_symbol = f"{symbol}.BYBIT"
                    new_vt_symbols.add(vt_symbol)
                    new_vt_symbols_bybit.add(vt_symbol)

            if len(update_contract_gateway_names):
                self.query_gateway_contract(list(update_contract_gateway_names))

            if len(new_vt_symbols_okx):
                Thread(target=self.set_leverage, args=(list(new_vt_symbols_okx), 20, 5,)).start()
            
            if len(new_vt_symbols_binance):
                Thread(target=self.set_leverage, args=(list(new_vt_symbols_binance), 20, 5,)).start()

            if len(new_vt_symbols_bybit):
                Thread(target=self.set_leverage, args=(list(new_vt_symbols_bybit), 20, 5,)).start()

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
                        hour_count = 3*24+1
                        if exchange == "OKX":
                            self.download_engine.download_from_okx(
                                contract_list=[symbol], hours=hour_count, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BYBIT":
                            self.download_engine.download_from_bybit(
                                contract_list=[symbol], hours=hour_count, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )
                        
                        elif exchange == "BINANCE":
                            self.download_engine.download_from_binance(
                                contract_list=[symbol], hours=hour_count, from_data_base=True, save_to=self.name, delete_history_data=False, show_progress=False
                            )

                        success = True

                    except Exception as e:
                        msg = f"TopGainersLosersPortfolio 下载Bar数据出错\n\n{e}"
                        self.send_ding_talk(msg)

                if success:
                    symbol_strategies = self.cta_engine.symbol_strategy_map[vt_symbol]
                    for i in range(len(symbol_strategies)):
                        strategy: TrendingMultiStrategy = symbol_strategies[i]
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

    def set_leverage(self, vt_symbols: str, default_leverage: int, optional_leverage: int, account_name: str = ""):
        self.default_leverage = default_leverage
        self.optional_leverage = optional_leverage
        try:
            for vt_symbol in vt_symbols:
                gateway_name = vt_symbol.split(".")[-1]

                # 设置杠杆
                if account_name:
                    gateway = self.cta_engine.main_engine.get_gateway(gateway_name, account_name)
                
                else:
                    gateway = self.cta_engine.main_engine.get_default_gateway(gateway_name)

                if gateway:
                    gateway.set_leverage(vt_symbol, self.default_leverage)

                time.sleep(1)
        
        except Exception as e:
            msg = f"设置杠杆出错: {vt_symbols}\n\n{e}"
            self.send_ding_talk(msg)
        
        # msg = f"交易所合约杠杆已更新\n{gateway_name}({len(vt_symbols)})"
        # self.send_ding_talk(msg)

    def process_leverage_failed_event(self, event: Event):
        try:
            data = event.data
            symbol = data["symbol"]
            leverage = data["leverage"]
            gateway_name = data["gateway_name"]
            account_name = data["account_name"]
            vt_symbol = f"{symbol}.{gateway_name}"

            if leverage > self.optional_leverage:
                next_leverage = leverage - 5

                # 设置备用杠杆
                gateway = self.cta_engine.main_engine.get_gateway(gateway_name, account_name)
                if gateway:
                    gateway.set_leverage(vt_symbol, next_leverage)

            else:
                msg = f"设置杠杆失败: {vt_symbol}\n杠杆: {leverage}"
                self.send_ding_talk(msg)

        except Exception as e:
            msg = f"处理设置杠杆失败事件出错\n\n{e}"
            self.send_ding_talk(msg)

    def process_funding_rates_event(self, event: Event):
        try:
            targets = []
            event_gateway = event.data.get("gateway_name", "")
            data = event.data.get("funding_rates", {})
            for d in data:
                symbol = d["symbol"]
                funding_rate = d["funding_rate"]
                next_funding_datetime = d["next_funding_datetime"]
                gateway_name = d["gateway_name"]
                vt_symbol = f"{symbol}.{gateway_name}"

                next_hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                if abs(funding_rate) > 0.005 and next_funding_datetime <= next_hour_time:
                    targets.append(d)
            
            if targets:
                # 按 funding_rate 降序排序
                targets.sort(key=lambda x: abs(x.get("funding_rate", 0)), reverse=True)

                # 通知当前钱包余额
                if time.time() > self.account_dingtalk_ts + 60:
                    self.account_dingtalk_ts = time.time()

                    msg = "钱包余额\n"
                    for account_name, balance in self.account_balance_data.items():
                        msg += f"\n{account_name} {balance:.2f}"
                    self.send_ding_talk(msg)

                # 设置杠杆
                vt_symbols = []
                for d in targets:
                    symbol = d["symbol"]
                    gateway_name = d["gateway_name"]
                    vt_symbol = f"{symbol}.{gateway_name}"
                    vt_symbols.append(vt_symbol)
                
                if event_gateway == "OKX":
                    Thread(target=self.set_leverage, args=(list(vt_symbols), 50, 10, "lo-e",)).start()
                
                elif event_gateway == "BINANCE":
                    Thread(target=self.set_leverage, args=(list(vt_symbols), 50, 10, "wawjlc",)).start()

                elif event_gateway == "BYBIT":
                    Thread(target=self.set_leverage, args=(list(vt_symbols), 50, 10, "loesuperman",)).start()

                # 通知资金费率信息
                msg = f"狙击资金费率（{event_gateway} {len(targets)}）\n"
                for d in targets:
                    symbol = d["symbol"]
                    funding_rate = d["funding_rate"] * 100
                    msg += f"\n{symbol} {funding_rate:.2f}%"
                self.send_ding_talk(msg)

                # 启动线程狙击资金费率
                Thread(target=self.snipe_funding_rate, args=(targets,)).start()

        except Exception as e:
            msg = f"处理资金费率事件出错\n\n{e}"
            self.send_ding_talk(msg)

    def process_trade_event(self, event: Event):
        try:
            trade: TradeData = event.data
            if trade.offset == Offset.OPEN:
                for vt_symbol, open_data in self.snipe_open_data.items():
                    if vt_symbol == trade.vt_symbol:
                        self.snipe_open_data.pop(vt_symbol)
                        volume = open_data["volume"]
                        direction = open_data["direction"]
                        funding_rate = open_data["funding_rate"]
                        close_direction = Direction.SHORT if direction == Direction.LONG else Direction.LONG
                        
                        # 止盈
                        self.cta_engine.send_simple_order(vt_symbol,
                                                          close_direction,
                                                          Offset.CLOSE,
                                                          trade.price,
                                                          volume,
                                                          OrderType.LIMIT)
                        
                        # 止损
                        stop_loss_price = trade.price * (1 - funding_rate*2 if direction == Direction.LONG else 1 + funding_rate*2)
                        self.cta_engine.send_simple_order(vt_symbol,
                                                          close_direction,
                                                          Offset.CLOSE,
                                                          stop_loss_price,
                                                          volume,
                                                          OrderType.STOP)
        except Exception as e:
            msg = f"处理成交事件出错\n\n{e}"
            self.send_ding_talk(msg)

    def snipe_funding_rate(self, targets: list):
        try:
            # 确认并获取mark_price
            for d in targets.copy():
                symbol = d["symbol"]
                mark_price = d["mark_price"]
                if not mark_price:
                    gateway_name = d["gateway_name"]
                    if gateway_name == "OKX":
                        url = f"https://www.okx.com/api/v5/public/mark-price?instId={symbol}"
                        try:
                            resp = requests.get(url, timeout=5)
                            data = resp.json().get("data", [])[0]
                            mark_price = float(data.get("markPx", 0))

                        except Exception as e:
                            pass
                
                if mark_price:
                    d["mark_price"] = mark_price
                
                else:
                    targets.remove(d)

            # 限制最大总资金费率和总数量
            total_funding_rate = 0
            count = 0
            for d in targets.copy():
                total_funding_rate += abs(d["funding_rate"])
                count += 1
                if total_funding_rate > 0.05 or count > 3:
                    count -= 1
                    break
            targets = targets[:count]

            # 准备狙击
            open = False
            close = False
            open_data = {}
            open_ts = 0
            while True:
                now = datetime.now()
                if not open and now.minute == 59 and now.second >= 59 and now.microsecond >= 900000:
                # if not open:
                    open = True
                    open_ts = time.time()
                    for d in targets:
                        symbol = d["symbol"]
                        funding_rate = d["funding_rate"]
                        mark_price = d["mark_price"]
                        gateway_name = d["gateway_name"]
                        vt_symbol = f"{symbol}.{gateway_name}"
                        account_name = ""
                        if gateway_name == "OKX":
                            account_name = "lo-e"

                        elif gateway_name == "BINANCE":
                            account_name = "wawjlc"

                        elif gateway_name == "BYBIT":
                            account_name = "loesuperman"

                        contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                        price = round_to(mark_price, contract.pricetick)
                        volume = 6 / mark_price
                        volume = round_to(volume, contract.min_volume)
                        if not price or not volume:
                            return

                        direction = Direction.LONG if funding_rate < 0 else Direction.SHORT
                        self.cta_engine.send_simple_order(vt_symbol,
                                                          direction,
                                                          Offset.OPEN,
                                                          price,
                                                          volume,
                                                          OrderType.MARKET,
                                                          account_name=account_name)
                        
                        open_data[vt_symbol] = {"price": price,
                                                "volume": volume,
                                                "direction": direction,
                                                "account_name": account_name,
                                                "funding_rate": funding_rate}
                
                if open and not close and now.minute == 0 and (now.second >= 1 or now.microsecond >= 100000):
                # if open and not close and time.time() >= open_ts + 1:
                    close = True
                    for vt_symbol, data in open_data.items():
                        price = data["price"]
                        volume = data["volume"]
                        direction = data["direction"]
                        account_name = data["account_name"]
                        
                        close_direction = Direction.SHORT if direction == Direction.LONG else Direction.LONG
                        self.cta_engine.send_simple_order(vt_symbol,
                                                          close_direction,
                                                          Offset.CLOSE,
                                                          price,
                                                          volume,
                                                          OrderType.MARKET,
                                                          account_name=account_name)
                
                if open and close:
                    break
                    
                else:
                    time.sleep(0.01)

        except Exception as e:
            msg = f"狙击资金费率出错\n\n{e}"
            self.send_ding_talk(msg)

    def update_leverage(self):
        for gateway_name in ["BINANCE", "BYBIT"]:
            vt_symbols = set()
            contracts = self.cta_engine.main_engine.engines["oms"].contracts
            for key in contracts.keys():
                contract: ContractData = contracts[key]
                if contract.gateway_name == gateway_name:
                    if contract.gateway_name == "BYBIT" and "-" in contract.vt_symbol:
                        # 过滤BYBIT交割合约
                        continue
                    vt_symbols.add(contract.vt_symbol)
            
            Thread(target=self.set_leverage, args=(list(vt_symbols), 20, 5,)).start()

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
            for signal_name in SIGNALS.keys():
                signal_data = self.pnl_data.get(signal_name, {})
                file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_pnls{DIR_SYMBOL}{signal_name}{DIR_SYMBOL}{date_str}.csv"
                if os.path.exists(file_path):
                    data_list = []
                    df = pd.read_csv(file_path)
                    for _, row in df.iterrows():
                        data_list.append(dict(row))

                    signal_data[date_str] = {"updated": False,
                                             "data": data_list}

                if signal_data:
                    self.pnl_data[signal_name] = signal_data

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
                for strategy in strategies.copy():
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
            for signal_name, signal_data in self.pnl_data.items():
                for date_str, date_data in signal_data.items():
                    updated = date_data["updated"]
                    if updated:
                        current_dir = os.path.dirname(os.path.abspath(__file__))
                        file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_pnls{DIR_SYMBOL}{signal_name}{DIR_SYMBOL}{date_str}.csv"

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
        setting_update_ts = 0
        trading_signal_ts = 0
        while True:
            try:
                for name in self.cta_engine.strategies.copy().keys():
                    strategy: TrendingMultiStrategy = self.cta_engine.strategies[name]
                    strategy_check_ts = self.strategy_status_check_ts.get(strategy.strategy_name, 0)
                    if time.time() >= strategy_check_ts + 10:
                        self.strategy_status_check_ts[strategy.strategy_name] = time.time()

                        # 检查仓位
                        strategy_target_pos = 0
                        for signal_name in strategy.signal_data.keys():
                            signal: SignalData = strategy.signal_data[signal_name]
                            strategy_target_pos += signal.target_pos
                            
                        if strategy.tick and strategy_target_pos != strategy.pos:
                            if strategy.direction == Direction.LONG:
                                if strategy_target_pos < 0 or strategy.pos < 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy_target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = strategy_target_pos - strategy.pos
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

                                    # 重新发送自动止损订单
                                    if strategy_target_pos and self.exchange == Exchange.BINANCE:
                                        strategy.send_order(Direction.SHORT, Offset.CLOSE, strategy.stop_price, abs(strategy_target_pos), stop=True)

                            if strategy.direction == Direction.SHORT:
                                if strategy_target_pos > 0 or strategy.pos > 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy_target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = abs(strategy_target_pos) - abs(strategy.pos)
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

                                    # 重新发送自动止损订单
                                    if strategy_target_pos and self.exchange == Exchange.BINANCE:
                                        strategy.send_order(Direction.LONG, Offset.CLOSE, strategy.stop_price, abs(strategy_target_pos), stop=True)
                        
                        # 长时间没有行情数据，关闭策略
                        if strategy.tick:
                            strategy_data_time = time.time() - strategy.tick.datetime.timestamp()
                            if strategy_data_time >= 20 * 60:
                                if not strategy_target_pos:
                                    strategy.on_close(strategy.tick)

                                msg = f"{strategy.strategy_name}\n\n长时间没有行情数据，检查代码\ntick_time: {strategy.tick.datetime}\ntarget_pos: {strategy_target_pos}\npos: {strategy.pos}\ns_count: {len(self.cta_engine.strategies)}\nu_symbols: {self.cta_engine.unsubscribed_symbols}"
                                self.send_ding_talk(msg)

                        # 长时间没有数据初始化
                        strategy_init_ts = datetime.strptime(strategy.init_dt, f"%Y-%m-%d %H:%M:%S").timestamp()
                        if not strategy.database_loaded and time.time() >= strategy_init_ts + 20 * 60:
                            if not strategy_target_pos:
                                strategy.on_close(strategy.tick)

                            msg = f"{strategy.strategy_name}\n\n长时间没有数据初始化，检查代码\ninit_time: {strategy.init_dt}\ntarget_pos: {strategy_target_pos}\npos: {strategy.pos}\ns_count: {len(self.cta_engine.strategies)}\nu_symbols: {self.cta_engine.unsubscribed_symbols}"
                            self.send_ding_talk(msg)

                        # 关闭已完成策略
                        if strategy.closed and not strategy.pos:
                            vt_orderids = self.cta_engine.strategy_orderid_map[strategy.strategy_name]
                            if vt_orderids:
                                strategy.cancel_all()
                            
                            else:
                                # 取消订阅
                                unsubscribe = True
                                for target_name in self.cta_engine.strategies.copy().keys():
                                    target_strategy: TrendingMultiStrategy = self.cta_engine.strategies[target_name]
                                    if strategy.strategy_name != target_strategy.strategy_name and strategy.vt_symbol == target_strategy.vt_symbol:
                                        unsubscribe = False
                                        break

                                if unsubscribe:
                                    self.cta_engine.unsubscribe([strategy.vt_symbol])

                                # 策略引擎关闭策略
                                strategy.cta_engine.remove_strategy(strategy.strategy_name)

                                # 更新setting.json
                                self.setting_update_needed = True

                        # 同步策略数据
                        strategy.check_save_data()

                # 引擎更新setting.json
                if self.setting_update_needed and time.time() > setting_update_ts + 5:
                    self.setting_update_needed = False
                    setting_update_ts = time.time()
                    self.cta_engine.update_setting()

                # 显示当前交易信号详情
                if time.time() > trading_signal_ts + 20:
                    trading_signal_ts = time.time()
                    trading_signals = {}
                    for name in self.cta_engine.strategies.copy().keys():
                        strategy: TrendingMultiStrategy = self.cta_engine.strategies[name]
                        for signal_name in strategy.signal_data.keys():
                            signal: SignalData = strategy.signal_data[signal_name]
                            if signal.target_pos:
                                trading_signals[f"{name}_{signal.name}"] = signal.open_tick_dt.replace(microsecond=0)

                    print("\n")
                    for signal_name, open_tick_dt in trading_signals.items():
                        print_(f"{open_tick_dt}\t{signal_name}")
                    print_(f"当前交易：{len(trading_signals)}\n")

                time.sleep(0.1)

            except Exception as e:
                msg = f"核查策略目标仓位出错\n\n{e}"
                self.send_ding_talk(msg)
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