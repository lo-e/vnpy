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
class TopGainersLosersStrategy(CtaTemplate):
    className = "TopGainersLosersStrategy"
    author = "loe"

    # 参数列表
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange",
        "exchange_user",
        "direction"
    ]

    # 变量列表
    variables = [
        "target_pos",
        "open_value",
        "open_price",
        "indicator_inited",
        "minute_5_bar_dt",
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "open_value",
        "open_price",
        "indicator_inited"
        "minute_5_bar_dt",
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        self.exchange: Exchange = Exchange.NONE
        self.exchange_user:str = ""
        self.direction: Direction = Direction.NET
        
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
        self.open_value = 0
        self.open_price = 0
        self.indicator_inited = False
        self.target_pos_check_ts = 0
        self.target_pos_checking = False
        
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
            return
        
        gateway = self.cta_engine.main_engine.get_gateway(gateway_name=exchange, account_name=self.exchange_user)
        if not gateway:
            msg = f"交易所账户未连接\n\n交易所：{exchange}\n账户：{self.exchange_user}"
            self.send_ding_talk(msg)

    def on_close(self):
        result, msg = self.cta_engine.remove_strategy_setting(self.strategy_name)
        if result:
            self.cta_engine.remove_strategy(self.strategy_name)

        else:
            msg = f"停止关闭策略失败\n\n{msg}"
            self.send_ding_talk(msg)
            print(msg)

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

    def check_target_pos(self):
        self.target_pos_checking = True
        while True:
            try:
                if self.tick and self.target_pos != self.pos and time.time() >= self.target_pos_check_ts + 3:
                    self.target_pos_check_ts = time.time()

                    # 撮合交易
                    if self.direction == "LONG":
                        if self.target_pos < 0 or self.pos < 0:
                            msg = f"仓位异常\n\ntarget {self.target_pos}\npos {self.pos}"
                            self.send_ding_talk(msg)
                            break

                        gap = self.target_pos - self.pos
                        if gap > 0:
                            # 多头开仓
                            trade_price = self.tick.last_price * 1.005
                            self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                        
                        elif gap < 0:
                            # 多头平仓
                            trade_price = self.tick.last_price * 0.995
                            self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                    if self.direction == "SHORT":
                        if self.target_pos > 0 or self.pos > 0:
                            msg = f"仓位异常\n\ntarget {self.target_pos}\npos {self.pos}"
                            self.send_ding_talk(msg)
                            break

                        gap = abs(self.target_pos) - abs(self.pos)
                        if gap > 0:
                            # 空头开仓
                            trade_price = self.tick.last_price * 0.995
                            self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                        
                        elif gap < 0:
                            # 空头平仓
                            trade_price = self.tick.last_price * 1.005
                            self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                elif self.target_pos == self.pos and time.time() >= self.target_pos_check_ts + 3:
                    self.cancel_all()

                elif self.target_pos == self.pos and time.time() >= self.target_pos_check_ts + 60:
                    break

            except Exception as e:
                msg = f"核查目标仓位出错\n\n目标 {self.target_pos} 当前 {self.pos}\n{e}"
                self.send_ding_talk(msg)
                break
        
        self.target_pos_checking = False

    def on_tick(self, tick: TickData):
        if not self.trading:
            return
        self.tick = copy(tick)
        
        target_pos_updated = False
        if target_pos_updated:
            self.target_pos_check_ts = time.time() - 10
            if not self.target_pos_checking:
                self.target_pos_checking = True
                Thread(target=self.check_target_pos).start()
            
        # 同步数据
        self.put_timer_event()

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
                    self.send_ding_talk(f"开仓订单价值未满足要求\n合约：{self.vt_symbol}\n价格：{tick.last_price}\n数量：{volume}\n价值：{order_value}")
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
                # 统计开仓数量
                self.open_count += 1

                # 重置开平仓变量
                self.open_value = 0
        
        except Exception as e:
            msg = f"成交处理出错\n\n{e}"
            self.send_ding_talk(msg)
        
        # 邮件提醒
        super().on_trade(trade)

        # 同步数据
        self.put_timer_event()

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")