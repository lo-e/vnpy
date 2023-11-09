# encoding: UTF-8

"""
跟单策略
"""

from vnpy.trader.constant import Direction, Offset
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.utility import ArrayManager
from vnpy.app.cta_strategy.base import *
from datetime import datetime, timedelta
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import round_to, floor_to, ceil_to
import numpy as np
from threading import Thread
from utilities.BarGenerator import BarGenerator
from App.marting.martingPortfolio import BAR_DOWNLOAD_GENERATE_COMPLETE
from vnpy.event import Event
from copy import copy
from vnpy.trader.event import EVENT_MAINENGINE_POSITION_UPDATED

class CopytradeStrategy(CtaTemplate):
    """ 跟单交易策略 """

    className = "CopytradeStrategy"
    author = "loe"

    # 参数列表，保存了参数的名称
    parameters = [
        "strategy_name",
        "vt_symbol",
    ]

    # 变量列表，保存了变量的名称
    variables = [
    ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncs = [
    ]

    def __init__(self, ctaEngine, setting):
        # 合约仓位字典
        self.symbol_pos_dict = {}

        # 完成setting.json参数的配置
        super(CopytradeStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

    def on_init(self):
        self.cta_engine.event_engine.register(EVENT_MAINENGINE_POSITION_UPDATED, self.on_mainengine_position_updated)
        pass

    def on_start(self):
        pass

    def on_timer(self):
        pass

    def on_mainengine_position_updated(self, event):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        for vt_positionid, position in oms_engine.positions.items():
            print(f"{datetime.now()}\t{vt_positionid}\t{position.symbol}\t{position.exchange.value}\t{position.exchange_user}\t{position.direction.value}\t{position.volume}\t{position.price}")
        print(f"\n")

    def on_tick(self, tick):
        if not self.trading:
            return

        # 去除时区，避免不必要的麻烦
        tick.datetime = tick.datetime.replace(tzinfo=None)

    def on_trade(self, trade):
        """成交推送"""
        pass
    
    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.strategy_name}\t{content}"
        self.cta_engine.main_engine.send_ding_talk(content)

    def send_email(self, content):
        # 邮件发送通知
        self.cta_engine.send_email(msg=content, subject=f"{self.strategy_name}")
