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
from .base import EVENT_BAR_UPDATED

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

    # 同步列表
    syncs = [
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

        self.bar_loading = False                                                                            # 正在加载bar数据
        self.bar_lack = True                                                                                # bar缺失
        self.bar = None                                                                                     # 当前最新bar
        self.am = ArrayManager(60)                                                                          # K线容器
        self.bar_generator = BarGenerator(on_bar=None, window=5, on_window_bar=self.on_window_bar)          # bar生成工具

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
        # 数据库加载bar数据
        mc = MongoClient()
        db = mc[MINUTE_DB_NAME]
        collection = db[self.vt_symbol]
        data_from = datetime.now().replace(second=0, microsecond=0) - timedelta(hours=6)
        data_to = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)
        flt = {"datetime": {"$gte": data_from, "$lte": data_to}}
        cursor = collection.find(flt).sort('datetime')

        next_bar_dt = None
        for d in cursor:
            bar = BarData(gateway_name = '', symbol = '', exchange = Exchange.NONE, datetime = None, endDatetime = None)
            bar.__dict__ = d

            if next_bar_dt and bar.datetime != next_bar_dt:
                # bar数据缺失
                self.bar_lack = True
                self.bar_loading = False

                msg = f"Bar数据确实\n合约 {self.vt_symbol}\n时间 {next_bar_dt}"
                self.send_ding_talk(msg)
                return
            
            next_bar_dt = bar.datetime + timedelta(minutes=1)
            self.on_bar(bar)

        if next_bar_dt - timedelta(minutes=1) != data_to:
            # bar数据缺失
            self.bar_lack = True
            self.bar_loading = False
            return

        # 更新状态（bar数据加载完毕）
        self.bar_lack = False
        self.bar_loading = False

        # 计算指标
        self.calculate_indicator()

    def on_bar(self, bar):
        self.bar_generator.update_bar(bar)

    def on_window_bar(self, bar):
        self.am.update_bar(bar)
        self.bar = bar

    def on_bar_updated(self, event):
        pass

    def calculate_indicator(self):
        pass

    def on_tick(self, tick: TickData):
        if not self.trading:
            return
        pass

    def send_order(self, direction, offset, price, volume):
        # 撤回历史订单
        self.cancel_all()

        # 精度处理
        contract = self.cta_engine.main_engine.get_contract(self.vt_symbol)
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)
        if not price or not volume:
            return
        
        # 当前虚拟持仓
        if direction == Direction.LONG:
            self.virtual_pos += volume

        else:
            self.virtual_pos -= volume

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
