# encoding: UTF-8

"""
打新策略
"""

from vnpy.trader.constant import Direction, Offset, Interval
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
from datetime import datetime, timedelta
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import round_to
from utilities.BarGenerator import BarGenerator
from vnpy.trader.constant import Exchange
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from threading import Thread
import time
from copy import copy
from vnpy.trader.utility import DIR_SYMBOL
import os
import csv
import shutil
import pandas as pd
import numpy as np
from vnpy.trader.object import ContractData
from queue import Empty, Queue
class TopGainersLosersStrategy(CtaTemplate):
    className = "TopGainersLosersStrategy"
    author = "loe"

    # 参数列表
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange",
        "exchange_user",
        "direction",
        "change",
        "volume_24h",
        "trending_mean_1h",
        "reverse_mean_1h",
        "trending_mean_24h",
        "reverse_mean_24h",
        "manual_close",
        "datetime"
    ]

    # 变量列表
    variables = []

    # 同步列表
    syncs = [
        "datetime",
        "target_pos",
        "stop_open",
        "stop_open_dt",
        "closed",
        "open_tick_value",
        "open_tick_price",
        "open_tick_dt",
        "stop_price",
        "stop_tick_price",
        "stop_tick_dt",
        "profit_price",
        "close_tick_price",
        "close_tick_dt",
        "pnl",
        "leverage",
        "open_count",
        "indicator_inited",
        "indicator_inited_dt",
        "history_high_cross",
        "history_low_cross",
        "pos_trending_price",
        "hour_up",
        "hour_up_ts",
        "hour_up_dt",
        "hour_down",
        "hour_down_ts",
        "hour_down_dt",
        "history_up",
        "history_down",
        "minute_recent_up",
        "minute_recent_down",
        "minute_15_up",
        "minute_15_down",
        "minute_bar_dt",
        "insufficient_value",
        "database_history_loaded",
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        self.exchange: Exchange = Exchange.NONE
        self.exchange_user:str = ""
        self.direction: Direction = Direction.NET
        self.change = 0
        self.volume_24h = ""
        self.trending_mean_1h = 0
        self.reverse_mean_1h = 0
        self.trending_mean_24h = 0
        self.reverse_mean_24h = 0
        self.manual_close = False
        self.datetime = ""

        """ fake """
        # self.send_fake_order = False
        
        # 完成setting.json参数的配置
        super(TopGainersLosersStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )
        
        # 交易所识别
        exchange = self.vt_symbol.split(".")[-1]
        if exchange == "OKX":
            self.exchange = Exchange.OKX
        
        elif exchange == "BINANCE":
            self.exchange = Exchange.BINANCE
        
        elif exchange == "BYBIT":
            self.exchange = Exchange.BYBIT
        
        else:
            raise(f"合约交易所不支持：{exchange}")
        
        # 交易方向配置判断
        if self.direction == "LONG":
            self.direction = Direction.LONG
        
        elif self.direction == "SHORT":
            self.direction = Direction.SHORT

        else:
            raise(f"交易方向配置错误：{self.direction}")

        self.tick: TickData = None
        self.target_pos = 0
        self.stop_open = False
        self.stop_open_dt = ""
        self.closed = False
        self.open_tick_value = 0
        self.open_tick_price = 0
        self.open_tick_dt = None
        self.stop_price = 0
        self.stop_tick_price = 0
        self.stop_tick_dt = ""
        self.profit_price = 0
        self.close_tick_price = 0
        self.close_tick_dt = ""
        self.pnl = 0
        self.leverage = 0
        self.open_count = 0
        self.database_loaded = False
        self.database_history_loaded = False
        self.indicator_inited = False
        self.indicator_inited_dt = ""
        self.target_pos_check_ts = 0
        self.target_pos_checking = False
        self.strategy_data = {}                     # 策略数据（包括常量、变量、同步）
        self.trade_logs = []                        # 交易日志
        self.trade_logs_updated = False
        self.insufficient_value = False             # 开仓价值不满足最低
        self.loading_database = False               # 正在加载数据
        self.bar_lack = False                       # 数据缺失
        self.bar_lack_count = 0                     # 数据缺失计数
        self.history_high_cross = False             # 长周期最高价
        self.history_low_cross = False              # 长周期最低价
        self.pos_trending_price = 0

        self.database_minute_bar_list = []
        self.tick_minute_bar_list = []
        self.tick_minute_bar_generator: BarGenerator = BarGenerator(on_bar=self.on_tick_minute_bar)
        self.tick_minute_bar_generator.bar_start = True
        
        self.minute_bar: BarData = None
        self.minute_bar_dt: str = ""
        self.history_hour = 3*24
        self.history_minute_am: ArrayManager = ArrayManager(self.history_hour*60)

        self.hour_up_down_updated = False
        self.hour_up: float = 0
        self.hour_up_ts: float = 0
        self.hour_up_dt: str = ""
        self.hour_down: float = 0
        self.hour_down_ts: float = 0
        self.hour_down_dt: str = ""
        self.history_up: float = 0
        self.history_down: float = 0
        self.minute_recent_up: float = 0
        self.minute_recent_down: float = 0
        self.minute_15_up: float = 0
        self.minute_15_down: float = 0

        self.minute_5_bar: BarData = None
        self.minute_5_bar_dt: str = ""
        self.minute_5_bar_generator: BarGenerator = None
        self.minute_5_am: ArrayManager = None
        self.minute_5_atr = 0

    def on_init(self):
        # 交易所成功连接判断
        exchange = self.vt_symbol.split(".")[-1]
        if exchange != "OKX" and exchange != "BINANCE" and exchange != "BYBIT":
            msg = f"未知交易所：{exchange}"
            self.send_ding_talk(msg)
        
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=exchange, account_name=self.exchange_user)
        if not gateway:
            msg = f"交易所账户未连接\n\n交易所：{exchange}\n账户：{self.exchange_user}"
            self.send_ding_talk(msg)
        
        # 获取历史交易日志
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_logs{DIR_SYMBOL}{self.strategy_name}.csv"
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                self.trade_logs.append(dict(row))

    def load_database_bar(self):
        try:
            self.loading_database = True

            # 数据库加载Bar数据
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=self.history_hour+1)
            flt = {"datetime": {"$gte": data_from}}
            cursor = collection.find(flt).sort('datetime')

            bar_list = []
            next_bar_dt = None
            bar_lack = False

            data_list = list(cursor)
            # last_bar_datetime: datetime = data_list[-1]["datetime"]
            # if datetime.now() < last_bar_datetime.replace(second=50):
            #     data_list = data_list[:-1]

            for d in data_list:
                bar = BarData(gateway_name = '', symbol = '', exchange = Exchange.NONE, datetime = None, endDatetime = None)
                bar.__dict__ = d

                if next_bar_dt and bar.datetime != next_bar_dt:
                    # bar数据缺失
                    bar_lack = True
                    msg = f"Bar数据缺失\n合约 {self.vt_symbol}\n时间 {next_bar_dt}"
                    self.send_ding_talk(msg)
                    break
                
                next_bar_dt = bar.datetime + timedelta(minutes=1)
                bar_list.append(bar)
            
            if not bar_lack:
                self.database_minute_bar_list = bar_list

            else:
                self.bar_lack_count += 1
                if self.bar_lack_count >= 3:
                    if not self.target_pos:
                        self.on_close()
                    
                    msg = f"{self.vt_symbol} 初始化数据加载失败，检查代码"
                    self.send_ding_talk(msg)

                else:
                    # 重新下载数据
                    if self.direction == Direction.LONG:
                        direction = "LONG"

                    elif self.direction == Direction.SHORT:
                        direction = "SHORT"

                    self.portfolio.bar_download_queue.put((self.vt_symbol, direction))

                msg = f"{self.vt_symbol} 初始化数据缺失\n\ncount {len(data_list)}\nlack {bar_lack}"
                self.send_ding_talk(msg)

        except Exception as e:
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

        self.loading_database = False
    
    def on_tick_minute_bar(self, bar: BarData):
        Thread(target=self.process_tick_minute_bar, args=(bar,)).start()

    def process_tick_minute_bar(self, bar: BarData):
        try:
            self.tick_minute_bar_list.append(bar)
            if not self.database_loaded and not self.database_minute_bar_list:
                return

            # 回测数据库Bar数据
            if not self.database_loaded:
                bar_lack = True
                database_end = ""
                tick_start = ""
                if len(self.database_minute_bar_list) >= self.history_hour*60:
                    for i in range(len(self.database_minute_bar_list)):
                        database_minute_bar: BarData = self.database_minute_bar_list[i]
                        if i < len(self.database_minute_bar_list) - 1:
                            self.on_minute_bar(database_minute_bar)
                        
                        else:
                            database_end = database_minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                            for j in range(len(self.tick_minute_bar_list)):
                                tick_minute_bar: BarData = self.tick_minute_bar_list[j]
                                if j == 0:
                                    tick_start = tick_minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                                if tick_minute_bar.datetime < database_minute_bar.datetime:
                                    continue

                                elif tick_minute_bar.datetime == database_minute_bar.datetime:
                                    bar_lack = False
                                    database_minute_bar.high_price = max(database_minute_bar.high_price, tick_minute_bar.high_price)
                                    database_minute_bar.low_price = min(database_minute_bar.low_price, tick_minute_bar.low_price)
                                    self.on_minute_bar(database_minute_bar)

                                else:
                                    if bar_lack:
                                        if tick_minute_bar.datetime == database_minute_bar.datetime + timedelta(minutes=1):
                                            bar_lack = False
                                            self.on_minute_bar(database_minute_bar)
                                    
                                    if not bar_lack:
                                        self.on_minute_bar(tick_minute_bar)
                                    
                                    else:
                                        tick_first_bar_dt = self.tick_minute_bar_list[0].datetime
                                        msg = f"{self.vt_symbol} Bar数据缺失\ndatabase {database_minute_bar.datetime}\ntick {tick_first_bar_dt}"
                                        self.send_ding_talk(msg)
                                        break

                self.bar_lack = bar_lack
                if self.bar_lack:
                    self.bar_lack_count += 1
                    if self.bar_lack_count >= 3:
                        if not self.target_pos:
                            self.on_close()
                        
                        msg = f"{self.vt_symbol} 初始化数据加载失败，检查代码"
                        self.send_ding_talk(msg)
                    
                    else:

                        if self.direction == Direction.LONG:
                            direction = "LONG"

                        elif self.direction == Direction.SHORT:
                            direction = "SHORT"

                        self.portfolio.bar_download_queue.put((self.vt_symbol, direction))
                    
                    msg = f"{self.vt_symbol} 初始化数据缺失\n\ndatabase {len(self.database_minute_bar_list)}\ndatabase_end {database_end}\ntick {len(self.tick_minute_bar_list)}\ntick_start {tick_start}"
                    self.send_ding_talk(msg)

                    # 重新下载数据
                    self.database_minute_bar_list = []
                    self.history_minute_am: ArrayManager = ArrayManager(self.history_hour*60)

                else:
                    self.database_loaded = True
                    self.database_history_loaded = True

            else:
                self.on_minute_bar(bar)
            
            # 确认指标初始化
            self.check_indicator_inited()

            # 保留最近行情数据
            if len(self.tick_minute_bar_list) > 10:
                self.tick_minute_bar_list = self.tick_minute_bar_list[1:]

        except Empty:
            pass

        except Exception as e:
            msg = f"处理tick_minute_bar出错\n\n{e}"
            self.send_ding_talk(msg)

    def on_minute_bar(self, bar: BarData):
        self.minute_bar = bar
        self.history_minute_am.update_bar(bar)
        self.calculate_indicator()

    def on_minute_5_bar(self, bar: BarData):
        self.minute_5_bar = bar
        self.minute_5_am.update_bar(bar)

    def calculate_indicator(self):
        if self.minute_bar:
            self.minute_bar_dt = self.minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        if self.history_minute_am.inited:
            hour_up, hour_down = self.history_minute_am.donchian(60)
            history_up, history_down = self.history_minute_am.donchian(self.history_hour*60)
            self.minute_15_up, self.minute_15_down = self.history_minute_am.donchian(15)

            if self.hour_up_dt and self.minute_bar_dt:
                recent_seconds = (datetime.strptime(self.minute_bar_dt, f"%Y-%m-%d %H:%M:%S") - datetime.strptime(self.hour_up_dt, f"%Y-%m-%d %H:%M:%S")).seconds
                recent_minutes = int(recent_seconds / 60)
                if recent_minutes > 1:
                    self.minute_recent_up, self.minute_recent_down = self.history_minute_am.donchian(recent_minutes)

            if self.direction == Direction.LONG:
                if (not self.database_loaded and not self.database_history_loaded and hour_up != self.hour_up) or self.hour_up_down_updated:
                    self.hour_up = hour_up
                    self.hour_down = hour_down
                    self.hour_up_ts = self.minute_bar.datetime.timestamp()
                    self.hour_up_dt = datetime.fromtimestamp(self.hour_up_ts).strftime(f"%Y-%m-%d %H:%M:%S")

                    self.history_up = history_up
                    self.history_down = history_down
                    if self.hour_up and self.history_up and self.hour_up >= self.history_up * 0.98:
                        self.history_high_cross = True
                    
                    else:
                        self.history_high_cross = False

                    self.hour_up_down_updated = False
                    self.stop_open = False
            
            if self.direction == Direction.SHORT:
                if (not self.database_loaded and not self.database_history_loaded and hour_down != self.hour_down) or self.hour_up_down_updated:
                    self.hour_down = hour_down
                    self.hour_up = hour_up
                    self.hour_down_ts = self.minute_bar.datetime.timestamp()
                    self.hour_down_dt = datetime.fromtimestamp(self.hour_down_ts).strftime(f"%Y-%m-%d %H:%M:%S")

                    self.history_up = history_up
                    self.history_down = history_down
                    if self.hour_down and self.history_down and self.hour_down <= self.history_down * 1.02:
                        self.history_low_cross = True
                    
                    else:
                        self.history_low_cross = False

                    self.hour_up_down_updated = False
                    self.stop_open = False
    
    def check_indicator_inited(self):
        if not self.target_pos:
            if self.direction == Direction.LONG:
                if self.hour_up and self.hour_down and self.minute_15_up and self.minute_15_down and self.minute_recent_up and self.minute_recent_down and self.minute_bar.datetime.timestamp() >= self.hour_up_ts + 30 * 60 and self.minute_15_down >= self.hour_up - abs(self.hour_up - self.hour_down) / 4.0 and self.minute_recent_down >= self.hour_up - abs(self.hour_up - self.hour_down) / 3.0:
                    self.indicator_inited = True
                    self.indicator_inited_dt = self.minute_bar_dt
                
                else:
                    self.indicator_inited = False
                    self.indicator_inited_dt = ""

            if self.direction == Direction.SHORT:
                if self.hour_up and self.hour_down and self.minute_15_up and self.minute_15_down and self.minute_recent_up and self.minute_recent_down and self.minute_bar.datetime.timestamp() >= self.hour_down_ts + 30 * 60 and self.minute_15_up <= self.hour_down + abs(self.hour_up - self.hour_down) / 4.0 and self.minute_recent_up <= self.hour_down + abs(self.hour_up - self.hour_down) / 3.0:
                    self.indicator_inited = True
                    self.indicator_inited_dt = self.minute_bar_dt

                else:
                    self.indicator_inited = False
                    self.indicator_inited_dt = ""

    def on_tick(self, tick: TickData):
        self.tick = copy(tick)
        self.tick_minute_bar_generator.update_tick(tick)
        if not self.trading:
            return

        """ fake """
        """
        if not self.send_fake_order:
            self.send_fake_order = True

            self.add_unit_pos(tick.last_price)
            open_volume = abs(self.target_pos)
            if open_volume:
                if self.direction == Direction.SHORT:
                    trade_price = self.tick.last_price * 1.005
                    self.stop_price = tick.last_price * 0.9995
                    if self.exchange == Exchange.BINANCE:
                        self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True)
                        self.send_order(Direction.SHORT, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                    else:
                        self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)
                
                elif self.direction == Direction.LONG:
                    trade_price = self.tick.last_price * 0.995
                    self.stop_price = tick.last_price * 1.0005
                    if self.exchange == Exchange.BINANCE:
                        self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True)
                        self.send_order(Direction.LONG, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                    else:
                        self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)
        

        return
        """

        # 1h新高新低
        price_cross = False
        if self.database_loaded and ((self.direction == Direction.LONG and tick.last_price > self.hour_up) or (self.direction == Direction.SHORT and tick.last_price < self.hour_down)):
            price_cross = True
            self.hour_up_down_updated = True 

        # 开仓判断
        if not self.target_pos and self.database_loaded and self.indicator_inited and price_cross and not self.stop_open and not self.closed:
            open_allowed = False
            if (self.direction == Direction.LONG and self.history_high_cross) or (self.direction == Direction.SHORT and self.history_low_cross):
                open_allowed = True

            if open_allowed:
                self.add_unit_pos(tick.last_price)

                # 发送订单
                # if not self.pos and self.portfolio.trade_enable and time.time() <= tick.datetime.timestamp() + 3:
                #     open_volume = abs(self.target_pos)
                #     if open_volume:
                #         if self.direction == Direction.LONG:
                #             trade_price = self.tick.last_price * 1.005
                #             self.cancel_all()
                #             if self.exchange == Exchange.BINANCE:
                #                 self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True)
                #                 self.send_order(Direction.SHORT, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                #             else:
                #                 self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)
                        
                #         elif self.direction == Direction.SHORT:
                #             trade_price = self.tick.last_price * 0.995
                #             self.cancel_all()
                #             if self.exchange == Exchange.BINANCE:
                #                 self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True)
                #                 self.send_order(Direction.LONG, Offset.CLOSE, self.stop_price, abs(open_volume), stop=True)

                #             else:
                #                 self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(open_volume), market=True, stop_loss_price=self.stop_price)

                # 开仓日志
                self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {tick.datetime.replace(microsecond=0)} OPEN {self.leverage:.2f} {tick.last_price}"})
                self.trade_logs_updated = True

                msg = f"{self.vt_symbol} {self.direction.value}\n开仓（{self.open_count}）"
                self.cta_engine.main_engine.send_ding_talk(msg)

        # 止损判断
        if self.target_pos and ((self.direction == Direction.LONG and tick.last_price <= self.stop_price) or (self.direction == Direction.SHORT and tick.last_price >= self.stop_price)):
            self.stop_tick_price = tick.last_price
            self.stop_tick_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
            self.target_pos = 0
            self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
            if not self.stop_open:
                self.stop_open = True
                self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

            stop_pnl = 0
            if self.open_tick_price:
                stop_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                if self.direction == Direction.SHORT:
                    stop_pnl *= -1
                stop_pnl -= 0.2
                stop_pnl *= self.leverage
            self.pnl += stop_pnl

            self.on_close(tick)

            # 止损日志
            self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {tick.datetime.replace(microsecond=0)} STOP {self.pnl:.2f}% {tick.last_price}"})
            self.trade_logs_updated = True

            msg = f"{self.vt_symbol} {self.direction.value}\n止损 {self.pnl:.2f}%"
            self.cta_engine.main_engine.send_ding_talk(msg)

        # 平仓判断
        if self.target_pos and self.database_loaded and ((self.direction == Direction.LONG and tick.last_price < self.hour_down) or (self.direction == Direction.SHORT and tick.last_price > self.hour_up)):
            self.target_pos = 0
            self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
            if not self.stop_open:
                self.stop_open = True
                self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

            close_pnl = 0
            if self.open_tick_price:
                close_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                if self.direction == Direction.SHORT:
                    close_pnl *= -1
                close_pnl -= 0.2
                close_pnl *= self.leverage
            self.pnl += close_pnl

            self.on_close(tick)
            
            # 平仓日志
            self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} CLOSE {self.pnl:.2f}% {tick.last_price}"})
            self.trade_logs_updated = True

            msg = f"{self.vt_symbol} {self.direction.value}\n平仓 {self.pnl:.2f}%"
            self.cta_engine.main_engine.send_ding_talk(msg)

        # 手动平仓
        if self.manual_close:
            if self.target_pos:
                self.target_pos = 0
                self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
                if not self.stop_open:
                    self.stop_open = True
                    self.stop_open_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

                close_pnl = 0
                if self.open_tick_price:
                    close_pnl = ((tick.last_price / self.open_tick_price) - 1) * 100
                    if self.direction == Direction.SHORT:
                        close_pnl *= -1
                    close_pnl -= 0.2
                    close_pnl *= self.leverage
                self.pnl += close_pnl

                self.on_close(tick)
                
                # 平仓日志
                self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} MANUAL_CLOSE {self.pnl:.2f}% {tick.last_price}"})
                self.trade_logs_updated = True

                msg = f"{self.vt_symbol} {self.direction.value}\n手动平仓 {self.pnl:.2f}%"
                self.cta_engine.main_engine.send_ding_talk(msg)
            
            else:
                self.on_close(tick)

        # 无信号退出
        if not self.target_pos and self.database_loaded and self.hour_up and self.hour_down:
            if self.direction == Direction.LONG:
                if tick.last_price < self.hour_up - abs(self.hour_up - self.hour_down) / 3.0 or tick.datetime.timestamp() >= self.hour_up_ts + 6 * 60 * 60:
                    self.on_close(tick)
            
            if self.direction == Direction.SHORT:
                if tick.last_price > self.hour_down + abs(self.hour_up - self.hour_down) / 3.0 or tick.datetime.timestamp() >= self.hour_down_ts + 6 * 60 * 60:
                    self.on_close(tick)
    
    def add_unit_pos(self, tick_price: float):
        # 计算仓位大小
        order_value = 0
        if self.direction == Direction.LONG:
            self.leverage = 0.01 / abs((self.minute_15_down / tick_price) - 1)
            order_value = self.portfolio.portfolio_value * self.leverage
        
        if self.direction == Direction.SHORT:
            self.leverage = 0.01 / abs((self.minute_15_up / tick_price) - 1)
            order_value = self.portfolio.portfolio_value * self.leverage

        self.target_pos = order_value / tick_price
        if self.direction == Direction.SHORT:
            self.target_pos *= -1

        # 仓位精度处理
        contract: ContractData = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        self.target_pos = round_to(self.target_pos, contract.min_volume)

        # 开仓价值、价格、时间
        self.open_tick_value = order_value
        self.open_tick_price = tick_price
        self.open_tick_dt = self.tick.datetime

        # 更新止损价格
        if self.direction == Direction.LONG:
            self.stop_price = self.minute_15_down
        
        if self.direction == Direction.SHORT:
            self.stop_price = self.minute_15_up
        
        # 开仓计数
        self.open_count += 1

    def on_close(self, tick: TickData = None):
        if self.closed:
            return
        
        if tick:
            self.close_tick_price = tick.last_price
            self.close_tick_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        self.closed = True
        if self.pnl:
            self.portfolio.on_pnl(self, self.pnl)

    def check_save_data(self):
        try:
            # 保存变量、同步数据
            strategy_data = self.get_data()
            if self.strategy_data != strategy_data:
                self.strategy_data = strategy_data
                self.put_event()

                print_(f"同步数据 {self.strategy_name}..")

            # 保存交易日志
            if self.trade_logs_updated:
                self.trade_logs_updated = False
                current_dir = os.path.dirname(os.path.abspath(__file__))
                file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_logs{DIR_SYMBOL}{self.strategy_name}.csv"
                field_names = list(self.trade_logs[0].keys())
                self.save_csv_data(field_names, self.trade_logs, file_path, True)
        
        except Exception as e:
            msg = f"保存策略数据出错\n\n{e}"
            self.send_ding_talk(msg)
            print_(msg)

    def send_order(self, direction, offset, price, volume, stop: bool = False, market: bool = False, stop_loss_price: float = 0):
        # 精度处理
        contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if not price or not volume:
            return

        # 币安开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN and self.exchange == Exchange.BINANCE:
            oms_engine = self.cta_engine.main_engine.engines["oms"]
            tick = oms_engine.ticks.get(self.vt_symbol, None)
            if tick:
                value_cross = True
                order_value = tick.last_price * volume
                pure_symbol = self.vt_symbol.split("USDT")[0]
                if pure_symbol == "BTC" and order_value <= 100:
                    value_cross = False

                if pure_symbol == "ETH" and order_value <= 20:
                    value_cross = False
                
                if pure_symbol == "BCH" and order_value <= 20:
                    value_cross = False

                if pure_symbol == "ETC" and order_value <= 20:
                    value_cross = False

                if pure_symbol == "LINK" and order_value <= 20:
                    value_cross = False

                if pure_symbol == "LTC" and order_value <= 20:
                    value_cross = False

                if order_value <= 5:
                    value_cross = False
                
                if not value_cross:
                    # self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{self.vt_symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                    self.insufficient_value = True
                    return
                
        # BYBIT开仓有最低价值限制，判断是否满足
        if offset == Offset.OPEN and self.exchange == Exchange.BYBIT:
            oms_engine = self.cta_engine.main_engine.engines["oms"]
            tick = oms_engine.ticks.get(self.vt_symbol, None)
            if tick:
                value_cross = True
                order_value = tick.last_price * volume
                if order_value <= 5:
                    value_cross = False
                
                if not value_cross:
                    # self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{self.vt_symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
                    self.insufficient_value = True
                    return
        
        # 平仓订单数量不超过当前持仓
        if offset != Offset.OPEN:
            volume = min(volume, abs(self.pos))
        
        # 发出订单
        super().send_order(direction, offset, price, volume, stop=stop, market=market, stop_loss_price=stop_loss_price)

    def on_trade(self, trade):
        try:    
            # 持仓精度自动修正
            contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
            if contract:
                self.pos = round_to(self.pos, contract.min_volume)

            # 止损触发
            if not self.pos and self.target_pos:
                self.stop_tick_price = self.tick.last_price
                self.stop_tick_dt = self.tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                self.target_pos = 0
                self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
                if not self.stop_open:
                    self.stop_open = True
                    self.stop_open_dt = self.tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

                stop_pnl = 0
                if self.tick and self.open_tick_price:
                    stop_pnl = ((trade.price / self.open_tick_price) - 1) * 100
                    if self.direction == Direction.SHORT:
                        stop_pnl *= -1
                    stop_pnl -= 0.2
                    stop_pnl *= self.leverage
                self.pnl += stop_pnl

                self.on_close(self.tick)

                # 记录日志
                self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} AUTO_STOP {self.pnl:.2f}% {trade.price}"})
                self.trade_logs_updated = True

                msg = f"{self.vt_symbol} {self.direction.value}\n自动止损 {self.pnl:.2f}%"
                self.cta_engine.main_engine.send_ding_talk(msg)
        
        except Exception as e:
            msg = f"成交处理出错\n\n{e}"
            self.send_ding_talk(msg)
        
        # 邮件提醒
        super().on_trade(trade)

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")

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

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")