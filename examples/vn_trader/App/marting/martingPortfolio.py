# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset
import re
from datetime import datetime
from copy import copy
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from time import time
from threading import Thread
from utilities.BarGenerator import MultiThreadsMinuteBarProcessor
from vnpy.trader.constant import Interval
from vnpy.event import Event
import os
import json
from pathlib import Path

BAR_DOWNLOAD_GENERATE_COMPLETE = "eDataComplete"


class MartingPortfolio(object):
    """马丁组合"""

    # 参数
    name = ""
    portfolioValue = 0  # 组合市值

    # 变量
    today = None

    paramList = ["name", "portfolioValue"]
    varList = [
        "today",
        "all_inited",
        "is_downloading",
        "downloading_cost",
        "downloading_wait",
        "is_generating",
        "generating_cost",
        "trending_top",
    ]
    syncList = ["today", "trending_top"]

    def __init__(self, engine, setting):
        self.engine = engine
        self.on_update_today()

        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()  # 数据下载引擎
        self.downloading_trigger = False  # 开启下载线程
        self.is_downloading = False  # 是否正在下载
        self.downloading_wait = 10000  # 数据下载等待时间（秒）
        self.downloading_cost = 0  # 下载更新一次花费的时间
        self.downloading_time = 0  # 下载开始的时间戳

        # window_bar合成相关
        self.bar_generate_engine = MultiThreadsMinuteBarProcessor(
            symbol_list=[],
            window=5,
            interval=Interval.MINUTE,
            start_date="2020-1-1",
            end_date="2023-12-31",
            from_data_base=True,
        )
        self.is_generating = False  # 是否正在合成
        self.generating_cost = 0  # 合成更新一次花费的时间
        self.generating_time = 0  # 合成开始的时间戳

        # 回测相关
        self.backtesting_count_down = 10  # 通知策略回测倒计时（秒）
        self.backtesting_preparing = False  # 准备通知策略回测，倒计时的开关
        self.backtesting_saved_count_down = 60  # 保存策略回测历史倒计时（秒）
        self.backtesting_saved_preparing = False  # 准备保存策略回测历史，倒计时的开关

        # 其它
        self.strategy_symbols = []  # 策略合约列表
        self.trending_top = False  # 策略组合中是否有策略已经达到趋势追踪最高级别
        self.all_inited = False # 是否所有策略完成初始化

        # 策略回测历史
        self.strategys_backtesting_history = {}
        self.load_backtesting_history()

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

    def on_update_today(self):
        self.today = copy(self.engine.today)
        # 同步到数据库
        self.engine.savePortfolioSyncData()

    def on_timer(self):
        # 保存策略回测历史
        if self.backtesting_saved_preparing:
            self.backtesting_saved_count_down -= 1
            if self.backtesting_saved_count_down <= 0:
                self.backtesting_saved_count_down = 60
                self.backtesting_saved_preparing = False
                self.save_backtesting_history()

        # 通知策略回测
        if self.backtesting_preparing:
            self.backtesting_count_down -= 1
            if self.backtesting_count_down <= 0:
                self.backtesting_count_down = 10
                self.backtesting_preparing = False
                event = Event(BAR_DOWNLOAD_GENERATE_COMPLETE)
                self.engine.event_engine.put(event)

                # 准备保存策略回测历史
                self.backtesting_saved_preparing = True
                self.backtesting_saved_count_down = 60

        # 合成结束
        if self.bar_generate_engine.loading_complete:
            if self.is_generating:
                # 准备通知策略回测
                self.backtesting_count_down = 10
                self.backtesting_preparing = True

            self.is_generating = False
            self.generating_time = 0

        # 合成花费时间计算
        if self.is_generating:
            self.generating_cost = int(time() - self.generating_time)

        # 下载开始
        if len(self.download_engine.threads):
            if self.downloading_trigger:
                self.downloading_trigger = False
                self.is_downloading = True
                self.downloading_wait = 0
                self.downloading_time = time()

        elif not self.downloading_trigger and self.download_engine.loading_complete:
            if self.is_downloading:
                # 刚结束下载，开始生成window_bar
                self.is_generating = True
                self.generating_time = time()

                thread = Thread(target=self.generate_window_bar)
                thread.start()

            self.is_downloading = False
            self.downloading_wait += 1
            self.downloading_time = 0

        # 下载花费时间计算
        if self.is_downloading:
            self.downloading_cost = int(time() - self.downloading_time)

        # 每隔设定的时间开始下载
        if (
            self.downloading_wait >= 5 * 60
            and not self.downloading_trigger
            and not self.is_downloading
            and not self.is_generating
        ):
            self.downloading_trigger = True
            thread = Thread(target=self.download_data)
            thread.start()

        # 组合状态更新
        self.engine.put_portfolio_event()

    def download_data(self):
        contract_list = []
        for symbol in self.strategy_symbols:
            contract_list.append(symbol.split(".")[0])
        self.download_engine.download_from_bybit(
            contract_list=contract_list, from_data_base=True
        )

    def generate_window_bar(self):
        self.bar_generate_engine.symbol_list = self.strategy_symbols
        self.bar_generate_engine.start()

    def get_backtesting_history_file_path(self):
        dir = os.path.dirname(os.path.realpath(__file__))
        file_path = Path(dir)
        file_path = file_path.joinpath("backtesting_history.json")
        return file_path

    def load_backtesting_history(self):
        # 从json文件获取策略回测历史
        history_data = {}
        json_file = self.get_backtesting_history_file_path()
        if json_file.exists():
            with open(json_file, mode="r", encoding="UTF-8") as f:
                history_data = json.load(f)
        if history_data:
            self.strategys_backtesting_history = history_data

    def save_backtesting_history(self):
        # 策略历史数据回测保存到json文件中
        for _, strategy in self.engine.strategies.items():
            if strategy.backtesting_status and strategy.backtesting_to:
                strategy_backtesting_data = {
                    "backtesting_status": strategy.backtesting_status,
                    "backtesting_to": strategy.backtesting_to.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
                self.strategys_backtesting_history[
                    f"{strategy.strategy_name}"
                ] = strategy_backtesting_data

        json_file = self.get_backtesting_history_file_path()
        with open(json_file, "w", encoding="utf-8") as file:
            file.write(
                json.dumps(self.strategys_backtesting_history, ensure_ascii=False)
            )
