# encoding: UTF-8

from datetime import datetime, timedelta
from copy import copy
import time
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue, Empty
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
from vnpy.trader.object import SubscribeRequest
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from vnpy.trader.constant import Direction, Offset
from .supportResistanceStrategy import SupportResistanceStrategy

class SupportResistancePortfolio(object):
    """ 压力支撑策略组合管理 """
    parameters = ["name",
                  "portfolioValue"]

    variables = [
        "inited",
        "started"
    ]

    syncs = [
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.inited = False
        self.started = False
        self.strategy_symbols = set()
        self.strategy_status_check_ts = {}

        # 设置参数
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    def on_init(self):
        pass

    def on_start(self):
        Thread(target=self.check_strategy_status).start()

    def on_stop(self):
        pass

    def on_timer(self):
        pass

    def check_strategy_status(self):
        while True:
            try:
                for name in self.cta_engine.strategies.copy().keys():
                    strategy: SupportResistanceStrategy = self.cta_engine.strategies[name]
                    strategy_check_ts = self.strategy_status_check_ts.get(strategy.strategy_name, 0)
                    if time.time() >= strategy_check_ts + 10:
                        self.strategy_status_check_ts[strategy.strategy_name] = time.time()

                        # 检查仓位
                        if strategy.tick and strategy.target_pos != strategy.pos:
                            if strategy.direction == Direction.LONG:
                                if strategy.target_pos < 0 or strategy.pos < 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy.target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = strategy.target_pos - strategy.pos
                                # if gap > 0 and not strategy.insufficient_value:
                                #     # 多头开仓
                                #     trade_price = strategy.tick.last_price * 1.005
                                #     strategy.send_order(Direction.LONG, Offset.OPEN, trade_price, abs(gap))
                                
                                # elif gap < 0:
                                #     # 多头平仓
                                #     trade_price = strategy.tick.last_price * 0.995
                                #     strategy.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                                if gap < 0:
                                    # 多头平仓
                                    trade_price = strategy.tick.last_price * 0.995
                                    strategy.send_order(Direction.SHORT, Offset.CLOSE, trade_price, abs(gap))

                            if strategy.direction == Direction.SHORT:
                                if strategy.target_pos > 0 or strategy.pos > 0:
                                    msg = f"仓位异常\n\n合约 {strategy.vt_symbol}\n方向 {strategy.direction.value}\n目标 {strategy.target_pos}\n当前 {strategy.pos}"
                                    self.send_ding_talk(msg)

                                gap = abs(strategy.target_pos) - abs(strategy.pos)
                                # if gap > 0 and not strategy.insufficient_value:
                                #     # 空头开仓
                                #     trade_price = strategy.tick.last_price * 0.995
                                #     strategy.send_order(Direction.SHORT, Offset.OPEN, trade_price, abs(gap))
                                
                                # elif gap < 0:
                                #     # 空头平仓
                                #     trade_price = strategy.tick.last_price * 1.005
                                #     strategy.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                                if gap < 0:
                                    # 空头平仓
                                    trade_price = strategy.tick.last_price * 1.005
                                    strategy.send_order(Direction.LONG, Offset.CLOSE, trade_price, abs(gap))

                        # 检查关闭策略
                        if not strategy.completed and strategy.target_pos == strategy.pos and strategy.entry:
                            strategy.on_complete()

            except Exception as e:
                # msg = f"核查策略目标仓位出错\n\n{e}"
                # self.send_ding_talk(msg)
                # break
                pass

    
    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)