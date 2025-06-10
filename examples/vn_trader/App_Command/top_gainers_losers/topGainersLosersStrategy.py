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
        "slot"
    ]

    # 变量列表
    variables = [
        "target_pos",
        "entry",
        "close",
        "leverage",
        "open_value",
        "open_price",
        "open_tick_price",
        "stop_price",
        "indicator_inited",
        "minute_5_bar_dt",
        "minute_5_atr",
        "insufficient_value"
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "entry",
        "close",
        "leverage",
        "open_value",
        "open_price",
        "open_tick_price",
        "stop_price",
        "indicator_inited",
        "minute_5_bar_dt",
        "minute_5_atr",
        "insufficient_value"
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        self.exchange: Exchange = Exchange.NONE
        self.exchange_user:str = ""
        self.direction: Direction = Direction.NET
        self.slot = 0
        
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
        self.entry = False
        self.close = False
        self.leverage = 0
        self.open_value = 0
        self.open_price = 0
        self.open_tick_price = 0
        self.stop_price = 0
        self.indicator_inited = False
        self.target_pos_check_ts = 0
        self.target_pos_checking = False
        self.strategy_data = {}                     # 策略数据（包括常量、变量、同步）
        self.trade_logs = []                        # 交易日志
        self.trade_logs_updated = False
        self.insufficient_value = False             # 开仓价值不满足最低
        
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

        # 定时检查保存数据
        Thread(target=self.check_save_data).start()

    def on_close(self):
        self.target_pos = 0
        self.portfolio.strategy_status_check_ts[self.strategy_name] = 0
        self.close = True

        # 记录日志
        pnl = 0
        if self.tick and self.open_tick_price:
            pnl = ((self.tick.last_price / self.open_tick_price) - 1) * 100
            if self.direction == Direction.SHORT:
                pnl = pnl * -1
        self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} CLOSE {pnl:.2f}%"})
        self.trade_logs_updated = True

    def load_database_bar(self):
        try:
            # 数据库加载Bar数据
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(second=0, microsecond=0) - timedelta(hours=2)
            flt = {"datetime": {"$gte": data_from}}
            cursor = collection.find(flt).sort('datetime')

            bar_list = []
            next_bar_dt = None
            bar_lack = False
            data_list = list(cursor)[:-1]
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

            if data_list and not bar_lack:
                # 初始化工具
                self.minute_5_am = ArrayManager(11)
                self.minute_5_bar_generator = BarGenerator(window=5, on_window_bar=self.on_minute_5_bar, interval=Interval.MINUTE)

                # 回测数据库Bar数据
                for bar in bar_list:
                    self.on_minute_bar(bar)
            
            else:
                pass

            # 指标完成初始化
            if self.minute_5_atr:
                self.indicator_inited = True

            else:
                self.portfolio.bar_download_queue.put(self.vt_symbol)
                msg = f"未完成指标初始化\nsymbol {self.vt_symbol}\nbar {self.minute_5_bar_dt}"
                self.send_ding_talk(msg)
                print_(msg)

        except Exception as e:
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

    def on_minute_bar(self, bar: BarData):
        self.minute_5_bar_generator.update_bar(bar)
        self.calculate_indicator()

    def on_minute_5_bar(self, bar: BarData):
        self.minute_5_bar = bar
        self.minute_5_am.update_bar(bar)

    def calculate_indicator(self):
        if self.minute_5_bar:
            self.minute_5_bar_dt = self.minute_5_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        if self.minute_5_am.inited:
            self.minute_5_atr = self.minute_5_am.atr(10)

    def on_tick(self, tick: TickData):
        if not self.trading:
            return
        
        self.tick = copy(tick)
        if not self.entry:
            # 开仓
            self.entry = True
            self.open_tick_price = tick.last_price
            self.target_pos = self.portfolio.portfolio_value / (tick.last_price * self.slot)
            if self.direction == Direction.SHORT:
                self.target_pos = self.target_pos * -1

            # 精度处理
            contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
            self.target_pos = round_to(self.target_pos, contract.min_volume)

            # if self.direction == Direction.LONG:
            #     # 多头开仓
            #     trade_price = self.tick.last_price * 1.005
            #     self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(self.target_pos))
            
            # elif self.direction == Direction.SHORT:
            #     # 空头开仓
            #     trade_price = self.tick.last_price * 0.995
            #     self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(self.target_pos))

            # 记录日志
            self.trade_logs.append({"LOG": f"{datetime.now().replace(microsecond=0)} OPEN {self.slot}"})
            self.trade_logs_updated = True
    
    def check_save_data(self):
        while not self.close:
            self.check_save_data_()
            time.sleep(1)

    def check_save_data_(self):
        return
    
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

    def send_order(self, direction, offset, price, volume):
        # 撤回历史订单
        self.cancel_all()

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
                if "BTC" in self.vt_symbol and order_value <= 100:
                    value_cross = False

                if "ETH" in self.vt_symbol and order_value <= 20:
                    value_cross = False
                
                if "BCH" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "ETC" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "LINK" in self.vt_symbol and order_value <= 20:
                    value_cross = False

                if "LTC" in self.vt_symbol and order_value <= 20:
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
        super().send_order(direction, offset, price, volume)

    def on_trade(self, trade):
        try:
            # 持仓精度自动修正
            contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
            if contract:
                self.pos = round_to(self.pos, contract.min_volume)

            trade_price = trade.price
            trade_volume = trade.volume
            if trade.offset == Offset.OPEN:
                # 开仓价值
                self.open_value += trade_price * trade_volume

                # 开仓均价
                self.open_price = self.open_value / abs(self.pos)

            else:
                # 开仓价值
                self.open_value = self.open_price * abs(self.pos)

            if not self.pos:
                # 重置
                self.open_value = 0
                self.open_price = 0
        
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