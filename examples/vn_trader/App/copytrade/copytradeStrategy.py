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
from vnpy.trader.object import BarData, TickData, PositionData
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
import numpy as np
from threading import Thread
from utilities.BarGenerator import BarGenerator
from App.marting.martingPortfolio import BAR_DOWNLOAD_GENERATE_COMPLETE
from vnpy.event import Event
from copy import copy
from vnpy.trader.event import EVENT_MAINENGINE_POSITION_UPDATED
import os
from pathlib import Path
from vnpy.trader.constant import Exchange

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

        # 跟单设置
        self.copy_setting = {}

        # 完成setting.json参数的配置
        super(CopytradeStrategy, self).__init__(
            cta_engine=ctaEngine, strategy_name="", vt_symbol="", setting=setting
        )

    def on_init(self):
        # 订阅交易所仓位更新
        self.cta_engine.event_engine.register(EVENT_MAINENGINE_POSITION_UPDATED, self.on_mainengine_position_updated)

        # 导入跟单设置
        dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
        file_path = dir_path.joinpath("setting.json")
        self.copy_setting = load_json_path(file_path)

    def on_start(self):
        pass

    def on_timer(self):
        pass

    def on_mainengine_position_updated(self, event):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        for vt_positionid, position in oms_engine.positions.items():
            exchange: Exchange = position.exchange
            exchange_user: str = position.exchange_user
            target_setting = self.copy_setting.get(exchange.value, {}).get(exchange_user, {})
            print(f"{datetime.now()}\t{vt_positionid}\t{position.volume}\t{position.price}")
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
