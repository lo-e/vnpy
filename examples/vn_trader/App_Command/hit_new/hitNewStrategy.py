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
class HitNewStrategy(CtaTemplate):
    className = "HitNewStrategy"
    author = "loe"

    # 参数列表
    parameters = [
        "strategy_name",
        "vt_symbol",
        "exchange_user",
        "direction"
    ]

    # 变量列表
    variables = [
        "target_pos",
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
        "hour_down_rebirth"
    ]

    # 同步列表
    syncs = [
        "target_pos",
        "hour_up",
        "hour_up_confirm",
        "hour_down",
        "hour_down_confirm",
    ]

    def __init__(self, ctaEngine, setting):
        self.portfolio = ctaEngine.portfolio
        
        # 完成setting.json参数的配置
        super(HitNewStrategy, self).__init__(
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

        self.bar_lack = False
        self.minute_bar_dt: str = ""
        self.hour_bar_dt: str = ""
        self.hour_bar_generator = None
        self.hour_am = None
        
        self.target_pos = 0
        self.tradable = True
        self.indicator_inited = False
        self.hour_up = 0
        self.hour_up_confirm = False
        self.hour_up_rebirth = False
        self.hour_down = 0
        self.hour_down_confirm = False
        self.hour_down_rebirth = False

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

    def load_bar_data(self):
        try:
            # 数据库加载Bar数据
            bar_lack = False
            mc = MongoClient()
            db = mc[MINUTE_DB_NAME]
            collection = db[self.vt_symbol]
            data_from = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=5)
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
                self.hour_am = ArrayManager(5)
                self.hour_bar_generator = BarGenerator(window=1, on_window_bar=self.on_hour_bar, interval=Interval.HOUR)
                for bar in bar_list:
                    self.on_bar(bar)

                # 计算指标
                self.calculate_indicator()

            self.bar_lack = bar_lack

        except Exception as e:
            self.tradable = False
            msg = f"加载Bar数据出错\n\n{e}"
            self.send_ding_talk(msg)

    def on_bar(self, bar):
        self.minute_bar_dt = bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
        self.hour_bar_generator.update_bar(bar)

    def on_hour_bar(self, bar):
        self.hour_bar_dt = bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
        self.hour_am.update_bar(bar)

    def on_bar_updated(self, _):
        self.load_bar_data()

    def calculate_indicator(self):
        if self.hour_am.inited:
            # 计算上下趋势价格
            hour_up, hour_down = self.hour_am.donchian_oc(5)
            if not self.hour_up_confirm and self.hour_up != hour_up:
                self.hour_up = hour_up
                self.hour_up_rebirth = False
            
            if not self.hour_down_confirm and self.hour_down != hour_down:
                self.hour_down = hour_down
                self.hour_down_rebirth = False
            
            # 指标完成初始化
            self.indicator_inited = True

    def on_tick(self, tick: TickData):
        if not self.trading:
            return
        
        # 判断Rebirth
        if not self.hour_up_rebirth and self.hour_up and tick.last_price < self.hour_up:
            self.hour_up_rebirth = True

        if not self.hour_down_rebirth and self.hour_down and tick.last_price > self.hour_down:
            self.hour_down_rebirth = True
        
        if self.target_pos:
            if self.direction == Direction.LONG and self.hour_up and tick.last_price <= self.hour_up * 0.99:
                # 多头平仓
                trade_price = tick.last_price * 0.995
                self.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(self.target_pos))
                self.target_pos = 0

            if self.direction == Direction.SHORT and self.hour_down and tick.last_price >= self.hour_down * 1.01:
                # 空头平仓
                trade_price = tick.last_price * 1.005
                self.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(self.target_pos))
                self.target_pos = 0
        
        elif self.tradable and self.indicator_inited and not self.bar_lack and not self.pos:
            if self.direction == Direction.LONG and self.hour_up and self.hour_up_rebirth and tick.last_price >= self.hour_up:
                # 多头开仓
                self.hour_up_confirm = True
                trade_value = self.portfolio.portfolio_value
                trade_price = tick.last_price * 1.005
                trade_volume = trade_value / tick.last_price
                self.send_order(Direction.LONG, Offset.OPEN, trade_price, trade_volume)
                self.target_pos = trade_volume

            if self.direction == Direction.SHORT and self.hour_down and self.hour_down_rebirth and tick.last_price <= self.hour_down:
                # 空头开仓
                self.hour_down_confirm = True
                self.hour_up_confirm = True
                trade_value = self.portfolio.portfolio_value
                trade_price = tick.last_price * 0.995
                trade_volume = trade_value / tick.last_price
                self.send_order(Direction.SHORT, Offset.OPEN, trade_price, trade_volume)
                self.target_pos = trade_volume * -1

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
        
        # 平仓订单数量处理
        if offset != Offset.OPEN:
            volume = min(volume, abs(self.pos))
        
        # 发出订单
        super().send_order(direction, offset, price, volume)

    def on_trade(self, trade):
        super().on_trade(trade)

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
