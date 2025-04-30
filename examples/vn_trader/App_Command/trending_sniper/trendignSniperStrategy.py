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
from queue import Empty, Queue
from threading import Thread
import time
from copy import copy
from collections import OrderedDict
class TrendignSniperStrategy(CtaTemplate):
    className = "TrendignSniperStrategy"
    author = "loe"

    # 参数列表
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange_user",
    ]

    # 变量列表
    variables = [
        "target_pos",
        "direction",
        "signal_price",
        "long_rebirth",
        "short_rebirth",
        "open_count",
        "open_price",
        "tradable",
        "indicator_inited",
        "minute_bar_dt",
        "minute_atr",
        "minute_5_bar_dt",
        "minute_5_atr",
        "hour_bar_dt",
        "hour_atr",
        "exit_up",
        "exit_down"
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "direction",
        "signal_price",
        "open_count",
        "open_value",
        "open_price"
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        
        # 完成setting.json参数的配置
        super(TrendignSniperStrategy, self).__init__(
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

        self.tick: TickData = None
        self.minute_bar_generator = BarGenerator(on_bar=self.on_live_minute_bar)
        self.live_bars: OrderedDict = OrderedDict()

        self.minute_bar: BarData = None
        self.minute_bar_dt: str = ""
        self.minute_am: ArrayManager = None
        self.minute_atr = 0

        self.minute_5_bar: BarData = None
        self.minute_5_bar_dt: str = ""
        self.minute_5_bar_generator: BarGenerator = None
        self.minute_5_am: ArrayManager = None
        self.minute_5_atr = 0

        self.hour_bar: BarData = None
        self.hour_bar_dt: str = ""
        self.hour_bar_generator: BarGenerator = None
        self.hour_am: ArrayManager = None
        self.hour_atr = 0

        self.check_target_pos_queue = Queue()
        self.check_target_pos_ts = 0
        self.tradable = True
        self.indicator_inited = False
        self.exit_up = 0
        self.exit_down = 0
        self.target_pos = 0
        self.direction = ""
        self.signal_price = 0
        self.long_rebirth = False
        self.short_rebirth = False
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

    def load_bar_data(self, data_to: datetime):
        try:
            # 数据库加载Bar数据
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=25)
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
                # 初始化工具
                self.minute_am = ArrayManager(21)

                self.minute_5_am = ArrayManager(21)
                self.minute_5_bar_generator = BarGenerator(window=5, on_window_bar=self.on_minute_5_bar, interval=Interval.MINUTE)

                self.hour_am = ArrayManager(21)
                self.hour_bar_generator = BarGenerator(window=1, on_window_bar=self.on_hour_bar, interval=Interval.HOUR)

                # 回测数据库Bar数据
                for bar in bar_list:
                    self.on_minute_bar(bar)
                last_bar: BarData = bar_list[-1]
                next_bar_dt = last_bar.datetime + timedelta(minutes=1)
                
                # 回测实时Bar数据
                data_valid = False
                for live_dt, live_bar in self.live_bars.items():
                    if live_dt == next_bar_dt:
                        data_valid = True
                        self.on_minute_bar(live_bar)
                        next_bar_dt = live_dt.datetime + timedelta(minutes=1)

                # 计算指标
                if data_valid:
                    self.calculate_indicator()

        except Exception as e:
            self.bar_lack = True
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

        # 同步数据
        self.put_timer_event()

    def on_live_minute_bar(self, bar: BarData):
        # 保存Bar数据
        self.live_bars[bar.datetime] = copy(bar)
        if len(self.live_bars) > 10:
            self.live_bars.popitem(last=0)

        # 初始化后用以生成指标
        if self.indicator_inited:
            self.on_minute_bar(bar)
        
        elif len(self.live_bars) >= 2:
            # 下载最新Bar数据
            thread = Thread(target=self.portfolio.download_bar_data)
            thread.start()

    def on_minute_bar(self, bar: BarData):
        self.minute_bar = bar
        self.minute_am.update_bar(bar)
        self.calculate_indicator()

        self.minute_5_bar_generator.update_bar(bar)
        self.hour_bar_generator.update_bar(bar)

    def on_minute_5_bar(self, bar: BarData):
        self.minute_5_bar = bar
        self.minute_5_am.update_bar(bar)
        self.calculate_indicator()


    def on_hour_bar(self, bar: BarData):
        self.hour_bar = bar
        self.hour_am.update_bar(bar)
        self.calculate_indicator()

    def on_bar_updated(self, _):
        if not self.indicator_inited and len(self.live_bars):
            live_bar: BarData = list(self.live_bars.values())[0]
            self.load_bar_data(data_to=live_bar.datetime)

    def calculate_indicator(self):
        # 通用指标
        if self.minute_bar:
            self.minute_bar_dt = self.minute_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        if self.minute_5_bar:
            self.minute_5_bar_dt = self.minute_5_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        if self.hour_bar:
            self.hour_bar_dt = self.hour_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")

        # 分钟指标
        if self.minute_am.inited:
            self.minute_atr = self.minute_am.atr(20)

        # 5分钟指标
        if self.minute_5_am.inited:
            self.minute_5_atr = self.minute_5_am.atr(20)
            self.exit_up, self.exit_down = self.minute_5_am.donchian(10)

        # 小时指标
        if self.hour_am.inited:
            self.hour_atr = self.hour_am.atr(20)
            
        # 指标完成初始化
        self.indicator_inited = True

    def check_target_pos(self):
        while True:
            try:
                _ = self.check_target_pos_queue.get(block=True, timeout=0.1)
                if not self.tick:
                    continue
                
                if self.target_pos == self.pos:
                    continue

                # 撮合交易
                if self.direction == "LONG":
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

                if self.direction == "SHORT":
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
        self.minute_bar_generator.update_tick(copy(tick))

        # 判断信号
        if not self.direction and not self.signal_price and self.indicator_inited:
            minute_high = self.minute_bar_generator.bar.high_price
            minute_low = self.minute_bar_generator.bar.low_price
            minute_rise = tick.last_price - minute_low
            minute_fall = minute_high - tick.last_price

            minute_5_high = self.minute_5_bar_generator.window_bar.high_price
            minute_5_low = self.minute_5_bar_generator.window_bar.low_price
            minute_5_rise = tick.last_price - minute_5_low
            minute_5_fall = minute_5_high - tick.last_price

            hour_high = self.hour_bar_generator.hour_bar.high_price
            hour_low = self.hour_bar_generator.hour_bar.low_price
            hour_rise = tick.last_price - hour_low
            hour_fall = hour_high - tick.last_price

            # 多头趋势
            if (self.minute_atr and minute_rise >= self.minute_atr * 3) or (self.minute_5_atr and minute_5_rise >= self.minute_5_atr * 3) or (self.hour_atr and hour_rise >= self.hour_atr * 3):
                self.direction = "LONG"
                self.signal_price = tick.last_price
                self.long_rebirth = True

            # 空头趋势
            if (self.minute_atr and minute_fall >= self.minute_atr * 3) or (self.minute_5_atr and minute_5_fall >= self.minute_5_atr * 3) or (self.hour_atr and hour_fall >= self.hour_atr * 3):
                self.direction = "SHORT"
                self.signal_price = tick.last_price
                self.short_rebirth = True
        
        # 判断Rebirth
        if self.direction == "LONG" and self.signal_price and tick.last_price <= self.signal_price * 0.99:
            self.long_rebirth = True

        if self.direction == "SHORT" and self.signal_price and tick.last_price >= self.signal_price * 1.01:
            self.short_rebirth = True

        # 判断离场
        if self.direction == "LONG" and self.target_pos and self.exit_down and tick.last_price <= self.exit_down:
            stop_long = True
            
        if self.direction == "SHORT" and self.target_pos and self.exit_up and tick.last_price >= self.exit_up:
            stop_short = True
        
        target_pos_updated = False
        if self.target_pos:
            if self.direction == "LONG" and (stop_long or (self.signal_price and tick.last_price <= self.signal_price * 0.99) or (self.open_price and tick.last_price <= self.open_price * 0.99)):
                # 多头平仓
                self.long_rebirth = False
                self.target_pos = 0
                target_pos_updated = True

            if self.direction == "SHORT" and (stop_short or (self.signal_price and tick.last_price >= self.hour_down * 1.01) or (self.open_price and tick.last_price >= self.open_price * 1.01)):
                # 空头平仓
                self.short_rebirth = False
                self.target_pos = 0
                target_pos_updated = True
        
        elif self.tradable and self.indicator_inited:
            if self.direction == "LONG" and self.signal_price and ((self.long_rebirth and tick.last_price >= self.signal_price) or (self.open_price and tick.last_price >= max(self.signal_price, self.open_price))):
                # 多头开仓
                self.target_pos = self.portfolio.portfolio_value / tick.last_price
                target_pos_updated = True

            if self.direction == "SHORT" and self.signal_price and ((self.short_rebirth and tick.last_price <= self.signal_price) or (self.open_price and tick.last_price <= min(self.signal_price, self.open_price))):
                # 空头开仓
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
