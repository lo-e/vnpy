from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.constant import Direction, Offset, Interval
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

SIGNALS = ["T1", "T2", "T3", "T4", "T5"]

class SignalData(object):
    def __init__(self, name: str):
        self.name = name
        self.target_pos = 0
        self.open_tick_value = 0
        self.open_tick_price = 0
        self.open_tick_dt = None
        self.stop_price = 0
        self.stop_tick_price = 0
        self.stop_tick_dt = ""
        self.close_tick_price = 0
        self.close_tick_dt = ""
        self.pnl = 0
        self.leverage = 0
        self.open_count = 0
        self.open_tags = []
        self.indicator_inited = False
        self.indicator_inited_dt = ""
        self.indicator_inited_hour_up = 0
        self.indicator_inited_hour_down = 0
        self.signal_dt_list = []
        self.signal_tag_count = 0
        self.pre_signal_dt = ""
        self.pre_signal_hour_up = 0
        self.pre_signal_hour_down = 0

class TrendingMultiStrategy(CtaTemplate):

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
        "closed",
        "close_tick_price",
        "close_tick_dt",
        "history_high_cross",
        "history_low_cross",
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
        "database_history_loaded"
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
        super(TrendingMultiStrategy, self).__init__(
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
        
        self.signal_data = {}
        for signal_name in SIGNALS:
            self.signal_data[signal_name] = SignalData(signal_name)

        self.tick: TickData = None
        self.closed = False
        self.close_tick_price = 0
        self.close_tick_dt = ""
        self.database_loaded = False
        self.database_history_loaded = False
        self.strategy_sync_data = {}                # 策略同步数据
        self.trade_logs = {}                        # 交易日志
        self.trade_logs_updated = False
        self.insufficient_value = False             # 开仓价值不满足最低
        self.loading_database = False               # 正在加载数据
        self.bar_lack = False                       # 数据缺失
        self.bar_lack_count = 0                     # 数据缺失计数
        self.history_high_cross = False             # 长周期最高价
        self.history_low_cross = False              # 长周期最低价

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
        for signal_name in SIGNALS:
            file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_logs{DIR_SYMBOL}{signal_name}{DIR_SYMBOL}{self.strategy_name}.csv"
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                signal_trade_logs = self.trade_logs.get(signal_name, [])
                for _, row in df.iterrows():
                    signal_trade_logs.append(dict(row))
                self.trade_logs[signal_name] = signal_trade_logs

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
                    
                    msg = f"{self.vt_symbol} 初始化数据缺失（{self.bar_lack_count}）\n\ncount {len(data_list)}\nlack {bar_lack}"
                    self.send_ding_talk(msg)

                    msg = f"{self.vt_symbol} 初始化数据加载失败，检查代码"
                    self.send_ding_talk(msg)

                else:
                    # 重新下载数据
                    self.portfolio.bar_download_queue.put(self.vt_symbol)

        except Exception as e:
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

        self.loading_database = False
    
    def on_tick_minute_bar(self, bar: BarData):
        # Thread(target=self.process_tick_minute_bar, args=(bar,)).start()
        self.process_tick_minute_bar(bar)

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
                        
                        msg = f"{self.vt_symbol} 初始化数据缺失（{self.bar_lack_count}）\n\ndatabase {len(self.database_minute_bar_list)}\ndatabase_end {database_end}\ntick {len(self.tick_minute_bar_list)}\ntick_start {tick_start}"
                        self.send_ding_talk(msg)

                        msg = f"{self.vt_symbol} 初始化数据加载失败，检查代码"
                        self.send_ding_talk(msg)
                    
                    else:
                        # 重新下载数据
                        self.portfolio.bar_download_queue.put(self.vt_symbol)

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

    def calculate_indicator(self):
        if self.minute_bar:
            self.minute_bar_dt = self.minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        if self.history_minute_am.inited:
            hour_up, hour_down = self.history_minute_am.donchian(60)
            history_up, history_down = self.history_minute_am.donchian(self.history_hour*60)
            self.minute_15_up, self.minute_15_down = self.history_minute_am.donchian(15)

            if self.direction == Direction.LONG:
                if (not self.database_loaded and not self.database_history_loaded and hour_up != self.hour_up) or self.hour_up_down_updated:
                    self.hour_up = hour_up
                    self.hour_down = hour_down
                    self.hour_up_ts = self.minute_bar.datetime.timestamp()
                    self.hour_up_dt = datetime.fromtimestamp(self.hour_up_ts).strftime(f"%Y-%m-%d %H:%M:%S")

                    self.history_up = history_up
                    self.history_down = history_down
                    if self.hour_up and self.history_up and self.hour_up >= self.history_up * 0.998:
                        self.history_high_cross = True
                    
                    else:
                        self.history_high_cross = False

                    self.hour_up_down_updated = False
            
            if self.direction == Direction.SHORT:
                if (not self.database_loaded and not self.database_history_loaded and hour_down != self.hour_down) or self.hour_up_down_updated:
                    self.hour_down = hour_down
                    self.hour_up = hour_up
                    self.hour_down_ts = self.minute_bar.datetime.timestamp()
                    self.hour_down_dt = datetime.fromtimestamp(self.hour_down_ts).strftime(f"%Y-%m-%d %H:%M:%S")

                    self.history_up = history_up
                    self.history_down = history_down
                    if self.hour_down and self.history_down and self.hour_down <= self.history_down * 1.002:
                        self.history_low_cross = True
                    
                    else:
                        self.history_low_cross = False

                    self.hour_up_down_updated = False

            if self.minute_bar_dt:
                recent_seconds = 0
                if self.direction == Direction.LONG and self.hour_up_dt:
                    recent_seconds = (datetime.strptime(self.minute_bar_dt, f"%Y-%m-%d %H:%M:%S") - datetime.strptime(self.hour_up_dt, f"%Y-%m-%d %H:%M:%S")).seconds

                if self.direction == Direction.SHORT and self.hour_down_dt:
                    recent_seconds = (datetime.strptime(self.minute_bar_dt, f"%Y-%m-%d %H:%M:%S") - datetime.strptime(self.hour_down_dt, f"%Y-%m-%d %H:%M:%S")).seconds
                
                recent_minutes = int(recent_seconds / 60)
                if recent_minutes > 1:
                    self.minute_recent_up, self.minute_recent_down = self.history_minute_am.donchian(recent_minutes)

                else:
                    self.minute_recent_up = 0
                    self.minute_recent_down = 0

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
    
    def add_unit_pos(self, tick_price: float, signal: SignalData):
        # 确定止损价格
        if self.direction == Direction.LONG and self.minute_15_down and self.minute_recent_down:
            signal.stop_price = max(self.minute_15_down, self.minute_recent_down)
        
        if self.direction == Direction.SHORT and self.minute_15_up and self.minute_recent_up:
            signal.stop_price = min(self.minute_15_up, self.minute_recent_up)

        # 计算仓位大小
        signal.leverage = 0.01 / abs((signal.stop_price / tick_price) - 1)
        order_value = self.portfolio.portfolio_value * signal.leverage
        
        signal.target_pos = order_value / tick_price
        if self.direction == Direction.SHORT:
            self.target_pos *= -1

        # 仓位精度处理
        contract: ContractData = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        signal.target_pos = round_to(signal.target_pos, contract.min_volume)

        # 开仓价值、价格、时间
        signal.open_tick_value = order_value
        signal.open_tick_price = tick_price
        signal.open_tick_dt = self.tick.datetime
        
        # 开仓计数
        signal.open_count += 1

    def on_close(self, tick: TickData = None):
        if self.closed:
            return
        
        if tick:
            self.close_tick_price = tick.last_price
            self.close_tick_dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        self.closed = True

    def get_syncs(self):
        strategy_syncs = {}
        for name in self.syncs:
            strategy_syncs[name] = getattr(self, name)
        
        signal_data = {}
        for signal_name, signal in self.signal_data.items():
            signal_data[signal_name] = signal.__dict__
        
        strategy_syncs["signal_data"] = signal_data
        return strategy_syncs

    def check_save_data(self):
        try:
            # 保存变量、同步数据
            strategy_sync_data = self.get_syncs()
            if self.strategy_sync_data != strategy_sync_data:
                self.strategy_sync_data = strategy_sync_data
                self.put_event()

                print_(f"同步数据 {self.strategy_name}..")

            # 保存交易日志
            if self.trade_logs_updated:
                self.trade_logs_updated = False
                current_dir = os.path.dirname(os.path.abspath(__file__))
                for signal_name, logs in self.trade_logs.items():
                    file_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}trade_logs{DIR_SYMBOL}{signal_name}{DIR_SYMBOL}{self.strategy_name}.csv"
                    field_names = list(logs[0].keys())
                    self.save_csv_data(field_names, logs, file_path, True)
        
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
            if not self.pos:
                for signal_name in self.signal_data.keys():
                    signal: SignalData = self.signal_data[signal_name]
                    if signal.target_pos:
                        signal.stop_tick_price = self.tick.last_price
                        signal.stop_tick_dt = self.tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                        signal.target_pos = 0

                        stop_pnl = 0
                        if signal.open_tick_price:
                            stop_pnl = ((trade.price / self.open_tick_price) - 1) * 100
                            if self.direction == Direction.SHORT:
                                stop_pnl *= -1
                            stop_pnl -= 0.2
                            stop_pnl *= self.leverage
                        signal.pnl += stop_pnl
                        self.portfolio.on_pnl(self, signal, stop_pnl)

                        if self.tick:
                            signal.close_tick_price = self.tick.last_price
                            signal.close_tick_dt = self.tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

                        # 记录日志
                        signal_trade_logs = self.trade_logs.get(signal_name, [])
                        signal_trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} {self.tick.datetime.replace(microsecond=0)} AUTO_STOP {self.pnl:.2f}% {trade.price}"})
                        self.trade_logs[signal_name] = signal_trade_logs
                        self.trade_logs_updated = True

                        msg = f"{self.vt_symbol} {self.direction.value} {signal_name}\n自动止损 {stop_pnl:.2f}%"
                        self.cta_engine.main_engine.send_ding_talk(msg)

                self.portfolio.strategy_status_check_ts[self.strategy_name] = 0

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

    def check_open_tags(self, signal: SignalData):
        # 24小时涨跌幅Top
        top = 10
        if signal.name == "T5":
            top = 5

        pure_symbol = get_strategy_symbol(self.strategy_name)
        trending_top_24h = []
        if self.direction == Direction.LONG and len(self.portfolio.rise_data_list_24h) >= top + 3:
            for i in range(3, top + 3, 1):
                trending_top_24h.append(self.portfolio.rise_data_list_24h[i]["symbol"])

        if self.direction == Direction.SHORT and len(self.portfolio.fall_data_list_24h) >= 13:
            for i in range(3, top + 3, 1):
                trending_top_24h.append(self.portfolio.fall_data_list_24h[i]["symbol"])
                
        if pure_symbol in trending_top_24h:
            signal.open_tags.append("1")

        # 前小时高低维持超过1小时且距离当前小时高低不超过前1小时高低1/2
        if signal.pre_signal_dt and ((self.direction == Direction.LONG and self.hour_up - signal.pre_signal_hour_up <= (signal.pre_signal_hour_up - signal.pre_signal_hour_down) * 0.5) or (self.direction == Direction.SHORT and signal.pre_signal_hour_down - self.hour_down <= (signal.pre_signal_hour_up - signal.pre_signal_hour_down) * 0.5)):
            signal.open_tags.append("3")

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")

def get_strategy_pure_name(strategy_name: str):
    if not strategy_name:
        return ""
    
    strategy_name_elements = strategy_name.split("_")[1:4]
    strategy_name_elements[2] = re.sub(r'[^a-zA-Z]', '', strategy_name_elements[2])
    strategy_pure_name = "_".join(strategy_name_elements)
    return strategy_pure_name

def get_strategy_symbol(strategy_name: str):
    if not strategy_name:
        return ""
    
    symbol = strategy_name.split("_")[3]
    return symbol

def get_strategy_type(strategy_name: str):
    if not strategy_name:
        return ""
    
    type = strategy_name.split("_")[2]
    return type

def get_full_volume(volume: str):
    if not volume:
        return 0
    
    volume_v = float(re.sub(r'[^\d.]', '', volume))
    volume_u = re.sub(r'[\d.]', '', volume)
    if volume_u == "亿":
        volume_v *= 100000000
    
    elif volume_u == "万":
        volume_v *= 10000
    
    return volume_v