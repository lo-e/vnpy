# encoding: UTF-8

"""
打新策略
"""

from vnpy.trader.constant import Direction, Offset
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
from queue import Empty, Queue
from threading import Thread
import time
from copy import copy

class TrendignSniperStrategy(CtaTemplate):
    className = "TrendignSniperStrategy"
    author = "loe"

    # 参数列表
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange_user",
        "direction",
        "market_on"
    ]

    # 变量列表
    variables = [
        "target_pos",
        "open_count",
        "open_price",
        "tradable",
        "indicator_inited",
        "bar_lack",
        "minute_bar_dt",
        "hour_bar_dt",
        "hour_up",
        "hour_up_confirm",
        "hour_up_rebirth",
        "hour_down",
        "hour_down_confirm",
        "hour_down_rebirth",
        "exit_up",
        "exit_down",
        "hour_bar_close_price",
        "lowest_price_after_short",
        "stop_long",
        "stop_short"
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "open_count",
        "open_value",
        "open_price",
        "pnl",
        "initial_hour_up",
        "hour_up",
        "hour_up_confirm",
        "initial_hour_down",
        "hour_down",
        "hour_down_confirm",
        "lowest_price_after_short",
        "stop_long",
        "stop_short"
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        
        # 完成setting.json参数的配置
        super(TrendignSniperStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

        # 交易方向配置判断
        if self.direction == "LONG":
            self.direction = Direction.LONG
        
        elif self.direction == "SHORT":
            self.direction = Direction.SHORT

        else:
            raise(f"交易方向配置错误：{self.direction}")
        
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
        
        # 上市时间
        self.market_on = datetime.strptime(self.market_on, f"%Y-%m-%d %H:%M:%S")

        self.tick: TickData = None
        self.bar_lack = False
        self.minute_bar: BarData = None
        self.minute_bar_dt: str = ""
        self.hour_bar: BarData = None
        self.hour_bar_dt: str = ""
        self.hour_bar_generator: BarGenerator = None
        self.entry_am: ArrayManager = None
        self.exit_am: ArrayManager = None
        self.check_target_pos_queue = Queue()
        self.check_target_pos_ts = 0
        
        self.tradable = True
        self.indicator_inited = False
        self.initial_hour_up = 0
        self.hour_up = 0
        self.hour_up_confirm = False
        self.hour_up_rebirth = False
        self.initial_hour_down = 0
        self.hour_down = 0
        self.hour_down_confirm = False
        self.hour_down_rebirth = False
        self.exit_up = 0
        self.exit_down = 0
        self.hour_bar_close_price = 0
        self.lowest_price_after_short = 0
        self.stop_long = False
        self.stop_short = False
        
        self.target_pos = 0
        self.open_count = 0
        self.open_value = 0
        self.open_price = 0

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

    def on_start(self):
        Thread(target=self.check_target_pos).start()

    def load_bar_data(self):
        try:
            # 数据库加载Bar数据
            bar_lack = False
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=10)
            data_to = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)
            flt = {"datetime": {"$gte": data_from, "$lte": data_to}}
            cursor = collection.find(flt).sort('datetime')

            bar_list = []
            next_bar_dt = None
            for d in cursor:
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

            if not next_bar_dt or next_bar_dt - timedelta(minutes=1) != data_to:
                # bar数据缺失
                bar_lack = True
                msg = f"Bar数据缺失\n合约 {self.vt_symbol}\n时间 {data_to}"
                self.send_ding_talk(msg)

            if not bar_lack:
                # 回测Bar数据
                self.entry_am = ArrayManager(5)
                self.exit_am = ArrayManager(10)
                self.hour_bar_generator = BarGenerator(window=1, on_window_bar=self.on_hour_bar, interval=Interval.HOUR)
                for bar in bar_list:
                    self.on_bar(bar)

                # 计算指标
                self.calculate_indicator()

            self.bar_lack = bar_lack

        except Exception as e:
            self.bar_lack = True
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

        # 同步数据
        self.put_timer_event()

    def on_bar(self, bar):
        self.minute_bar = bar
        self.hour_bar_generator.update_bar(bar)

    def on_hour_bar(self, bar: BarData):
        self.hour_bar = bar
        self.entry_am.update_bar(bar)
        self.exit_am.update_bar(bar)

    def on_bar_updated(self, _):
        self.load_bar_data()

    def calculate_indicator(self):
        # 通用指标
        if self.minute_bar:
            self.minute_bar_dt = self.minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
            if self.minute_bar.datetime >= self.market_on + timedelta(days=5):
                self.tradable = False

        if self.hour_bar:
            self.hour_bar_dt = self.hour_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
            self.hour_bar_close_price = self.hour_bar.close_price

        # 入场指标
        if self.entry_am.inited:
            # 计算入场唐奇安通道
            hour_up, hour_down = self.entry_am.donchian_oc(5)

            # 确定初始通道
            if not self.initial_hour_up:
                self.initial_hour_up = hour_up

            if not self.initial_hour_down:
                self.initial_hour_down = hour_down

            # 判断实际通道
            if not self.hour_up_confirm and self.hour_up != hour_up and hour_up > self.initial_hour_down:
                self.hour_up = hour_up
                self.hour_up_rebirth = False
            
            if not self.hour_down_confirm and self.hour_down != hour_down and hour_down < self.initial_hour_up:
                self.hour_down = hour_down
                self.hour_down_rebirth = False
            
            # 指标完成初始化
            self.indicator_inited = True

        # 离场指标
        if self.direction == Direction.SHORT and self.target_pos and self.exit_up:
            self.lowest_price_after_short = min(self.lowest_price_after_short, self.hour_bar.low_price) if self.lowest_price_after_short else self.hour_bar.low_price
            rise_rate = self.hour_bar_close_price / self.lowest_price_after_short - 1
            if rise_rate >= 0.2 and self.hour_bar_close_price >= self.exit_up:
                self.stop_short = True

        if self.exit_am.inited:
            # 计算出场唐奇安通道
            self.exit_up, self.exit_down = self.exit_am.donchian(10)

    def check_target_pos(self):
        while True:
            try:
                _ = self.check_target_pos_queue.get(block=True, timeout=0.1)
                if not self.tick:
                    continue
                
                if self.target_pos == self.pos:
                    continue

                # 撮合交易
                if self.direction == Direction.LONG:
                    if self.target_pos < 0 or self.pos < 0:
                        msg = f"仓位异常\n\ntarget {self.target_pos}\npos {self.pos}"
                        self.send_ding_talk(msg)
                        continue

                    gap = self.target_pos - self.pos
                    if gap > 0:
                        # 多头开仓
                        trade_price = self.tick.last_price * 1.005
                        self.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                    
                    elif gap < 0:
                        # 多头平仓
                        trade_price = self.tick.last_price * 0.995
                        self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                if self.direction == Direction.SHORT:
                    if self.target_pos > 0 or self.pos > 0:
                        msg = f"仓位异常\n\ntarget {self.target_pos}\npos {self.pos}"
                        self.send_ding_talk(msg)
                        continue

                    gap = abs(self.target_pos) - abs(self.pos)
                    if gap > 0:
                        # 空头开仓
                        trade_price = self.tick.last_price * 0.995
                        self.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                    
                    elif gap < 0:
                        # 空头平仓
                        trade_price = self.tick.last_price * 1.005
                        self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))
            
            except Empty:
                pass
                
            except Exception as e:
                msg = f"核查目标仓位出错\n\n{e}"
                self.send_ding_talk(msg)

    def on_timer(self):
        # 每隔两秒核查目标仓位
        if time.time() >= self.check_target_pos_ts + 2:
            self.check_target_pos_ts = time.time()
            self.check_target_pos_queue.put("")

        super().on_timer()

    def on_tick(self, tick: TickData):
        if not self.trading:
            return
        
        # 最新Tick
        self.tick = copy(tick)
        
        # 判断Rebirth
        if self.hour_up and tick.last_price < self.hour_up:
            self.hour_up_rebirth = True
            self.stop_long = False

        if self.hour_down and tick.last_price > self.hour_down:
            self.hour_down_rebirth = True
            self.stop_short = False

        # 判断离场
        if self.direction == Direction.LONG and self.target_pos and self.exit_down and tick.last_price <= self.exit_down:
            self.stop_long = True
            
        if self.direction == Direction.SHORT and self.target_pos and self.open_price and tick.last_price <= self.open_price * 0.5:
            self.stop_short = True
        
        target_pos_updated = False
        if self.target_pos:
            if self.direction == Direction.LONG and (self.stop_long or (self.hour_up and tick.last_price <= self.hour_up * 0.99) or (self.open_price and tick.last_price <= self.open_price * 0.99)):
                # 多头平仓
                self.hour_up_rebirth = False
                self.target_pos = 0
                target_pos_updated = True

            if self.direction == Direction.SHORT and (self.stop_short or (self.hour_down and tick.last_price >= self.hour_down * 1.01) or (self.open_price and tick.last_price >= self.open_price * 1.01)):
                # 空头平仓
                self.hour_down_rebirth = False
                self.target_pos = 0
                target_pos_updated = True
                self.lowest_price_after_short = 0
        
        elif self.tradable and self.indicator_inited and not self.bar_lack:
            if self.direction == Direction.LONG and self.hour_up and not self.stop_long and ((self.hour_up_rebirth and tick.last_price >= self.hour_up) or (self.open_price and tick.last_price >= max(self.hour_up, self.open_price))):
                # 多头开仓
                self.hour_up_confirm = True
                self.target_pos = self.portfolio.portfolio_value / tick.last_price
                target_pos_updated = True

            if self.direction == Direction.SHORT and self.hour_down and not self.stop_short and ((self.hour_down_rebirth and tick.last_price <= self.hour_down) or (self.open_price and tick.last_price <= min(self.open_price, self.hour_down))):
                # 空头开仓
                self.hour_down_confirm = True
                self.target_pos = self.portfolio.portfolio_value / tick.last_price * -1
                target_pos_updated = True

        if target_pos_updated:
            self.check_target_pos_ts = time.time()
            self.check_target_pos_queue.put("")
            
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
