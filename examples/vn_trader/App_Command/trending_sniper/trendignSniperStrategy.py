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
        "signal_dt_str",
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
        "entry_up",
        "entry_down",
        "exit_up",
        "exit_down"
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "direction",
        "signal_price",
        "signal_dt_str",
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
        self.live_bars = []

        self.minute_bar: BarData = None
        self.minute_bar_dt: str = ""
        self.minute_am: ArrayManager = None
        self.minute_atr = 0
        self.minute_high = 0
        self.minute_low = 0

        self.minute_5_bar: BarData = None
        self.minute_5_bar_dt: str = ""
        self.minute_5_bar_generator: BarGenerator = None
        self.minute_5_am: ArrayManager = None
        self.minute_5_atr = 0
        self.minute_5_high = 0
        self.minute_5_low = 0

        self.hour_bar: BarData = None
        self.hour_bar_dt: str = ""
        self.hour_bar_generator: BarGenerator = None
        self.hour_am: ArrayManager = None
        self.hour_atr = 0
        self.hour_high = 0
        self.hour_low = 0

        self.tradable = True
        self.indicator_inited = False
        self.entry_up = 0
        self.entry_down = 0
        self.exit_up = 0
        self.exit_down = 0
        self.target_pos_checking = False
        self.target_pos_check_ts = 0
        self.target_pos = 0
        self.direction = ""
        self.signal_price = 0
        self.signal_dt_str = ""
        self.long_rebirth = False
        self.short_rebirth = False
        self.open_count = 0
        self.open_value = 0
        self.open_price = 0

        self.minute_tick_count = 0
        self.minute_tick_count_list = []

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

    def load_database_bar(self):
        try:
            # 数据库加载Bar数据
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=25)
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
                for bar in self.live_bars:
                    if bar.datetime == next_bar_dt or data_valid:
                        data_valid = True
                        self.on_minute_bar(bar)

                # 指标完成初始化
                if data_valid:
                    self.indicator_inited = True
                
                else:
                    pass
            
            else:
                pass

        except Exception as e:
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

        # 同步数据
        self.put_timer_event()

    def on_live_minute_bar(self, bar: BarData):
        # 保存Bar数据
        self.live_bars.append(copy(bar))
        if len(self.live_bars) > 60:
            self.live_bars.pop(0)

        # 初始化后用以生成指标
        if self.indicator_inited:
            self.on_minute_bar(bar)

    def on_minute_bar(self, bar: BarData):
        if self.indicator_inited and self.tick:
            self.minute_high = self.tick.last_price
            self.minute_low = self.tick.last_price

        self.minute_bar = bar
        self.minute_am.update_bar(bar)

        self.minute_5_bar_generator.update_bar(bar)
        self.hour_bar_generator.update_bar(bar)
        self.calculate_indicator()

    def on_minute_5_bar(self, bar: BarData):
        if self.indicator_inited and self.tick:
            self.minute_5_high = self.tick.last_price
            self.minute_5_low = self.tick.last_price

        self.minute_5_bar = bar
        self.minute_5_am.update_bar(bar)

    def on_hour_bar(self, bar: BarData):
        if self.indicator_inited and self.tick:
            self.hour_high = self.tick.last_price
            self.hour_low = self.tick.last_price

        self.hour_bar = bar
        self.hour_am.update_bar(bar)

        self.direction = ""
        self.signal_price = 0
        self.signal_dt_str = ""

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
            self.entry_up, self.entry_down = self.hour_am.donchian(10)

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
        
        # 记录分钟tick数
        if self.tick and self.tick.datetime.minute != tick.datetime.minute:
            self.minute_tick_count_list.append(self.minute_tick_count)
            if len(self.minute_tick_count_list) > 10:
                self.minute_tick_count_list.pop(0)
            self.minute_tick_count = 1
        
        else:
            self.minute_tick_count += 1
        
        # 保存最新Tick数据、生成实时Bar数据
        self.tick = copy(tick)
        self.minute_bar_generator.update_tick(copy(tick))
    
        # 判断信号
        if not self.direction and not self.signal_price and self.indicator_inited:
            self.minute_high = max(self.minute_high, tick.last_price)
            self.minute_low = min(self.minute_low, tick.last_price) if self.minute_low else tick.last_price
            minute_rise = tick.last_price - self.minute_low
            minute_fall = self.minute_high - tick.last_price

            self.minute_5_high = max(self.minute_5_high, tick.last_price)
            self.minute_5_low = min(self.minute_5_low, tick.last_price) if self.minute_5_low else tick.last_price
            minute_5_rise = tick.last_price - self.minute_5_low
            minute_5_fall = self.minute_5_high - tick.last_price

            self.hour_high = max(self.hour_high, tick.last_price)
            self.hour_low = min(self.hour_low, tick.last_price) if self.hour_low else tick.last_price
            hour_rise = tick.last_price - self.hour_low
            hour_fall = self.hour_high - tick.last_price

            # 交易额条件
            turnover = tick.turnover if tick.turnover else tick.volume * tick.last_price
            turnover_valid = True if turnover >= 10_000_000 else False

            # ATR条件（多头）
            long_atr_valid = False
            if self.minute_5_atr and minute_rise >= self.minute_5_atr * 3:
                long_atr_valid = True

            # ATR条件（空头）
            short_atr_valid = False
            if self.minute_5_atr and minute_fall >= self.minute_5_atr * 3:
                short_atr_valid = True

            # 多头趋势
            if tick.last_price >= self.entry_up and turnover_valid and long_atr_valid:
                self.direction = "LONG"
                self.signal_price = tick.last_price
                self.signal_dt_str = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                self.long_rebirth = True

                average_tick_count = sum(self.minute_tick_count_list) / len(self.minute_tick_count_list)
                msg = f"多头趋势\n\nsymbol {self.vt_symbol}\ndirection {self.direction}\nprice {tick.last_price}\nup {self.entry_up}\nturnover {turnover}\ntick_count {self.minute_tick_count}\naverage_count {average_tick_count}\n\nM_ATR {self.minute_atr}\nM_RISE {minute_rise}\n\nM_5_ATR {self.minute_5_atr}\nM_5_RISE {minute_5_rise}\n\nH_ATR {self.hour_atr}\nH_RISE {hour_rise}"
                self.send_ding_talk(msg)

            # 空头趋势
            if tick.last_price <= self.entry_down and turnover_valid and short_atr_valid:
                self.direction = "SHORT"
                self.signal_price = tick.last_price
                self.signal_dt_str = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                self.short_rebirth = True

                average_tick_count = sum(self.minute_tick_count_list) / len(self.minute_tick_count_list)
                msg = f"空头趋势\n\nsymbol {self.vt_symbol}\ndirection {self.direction}\nprice {tick.last_price}\ndown {self.entry_down}\nturnover {turnover}\ntick_count {self.minute_tick_count}\naverage_count {average_tick_count}\n\nM_ATR {self.minute_atr}\nM_FALL {minute_fall}\n\nM_5_ATR {self.minute_5_atr}\nM_5_FALL {minute_5_fall}\n\nH_ATR {self.hour_atr}\nH_FALL {hour_fall}"
                self.send_ding_talk(msg)

        # 判断Rebirth
        if self.direction == "LONG" and self.signal_price and tick.last_price <= self.signal_price * 0.99:
            self.long_rebirth = True

        if self.direction == "SHORT" and self.signal_price and tick.last_price >= self.signal_price * 1.01:
            self.short_rebirth = True

        # 判断离场
        stop_long = False
        stop_short = False
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

            if self.direction == "SHORT" and (stop_short or (self.signal_price and tick.last_price >= self.signal_price * 1.01) or (self.open_price and tick.last_price >= self.open_price * 1.01)):
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
            self.target_pos_check_ts = time.time() - 10
            # if not self.target_pos_checking:
            #     self.target_pos_checking = True
            #     Thread(target=self.check_target_pos).start()
            
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