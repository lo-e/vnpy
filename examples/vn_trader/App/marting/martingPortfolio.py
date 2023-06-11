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
from vnpy.trader.utility import DIR_SYMBOL

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
        self.all_inited = False  # 是否所有策略完成初始化
        self.strategy_info_count_down = 10 * 60  # 每隔一段时间发送策略状态信息通知

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

        # 策略回测历史
        self.strategys_backtesting_history = {}
        self.load_backtesting_history()

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

        # 通知策略状态信息
        self.strategy_info_count_down -= 1
        if self.strategy_info_count_down <= 0:
            self.strategy_info_count_down = 60 * 60
            self.notice_strategy_info()

        # 组合状态更新
        self.engine.put_portfolio_event()

    def download_data(self):
        contract_list = []
        exchange = ""
        for symbol in self.strategy_symbols:
            contract_list.append(symbol.split(".")[0])
            if not exchange:
                exchange = symbol.split(".")[-1]

        if exchange == "BYBIT":
            self.download_engine.download_from_bybit(
                contract_list=contract_list, from_data_base=True
            )

        elif exchange == "BINANCE":
            self.download_engine.download_from_binance(
                contract_list=contract_list, from_data_base=True
            )

    def generate_window_bar(self):
        self.bar_generate_engine.symbol_list = self.strategy_symbols
        self.bar_generate_engine.start()

    def get_backtesting_history_file_path(self):
        exchange = self.name.split("_")[-1]
        dir = os.path.dirname(os.path.realpath(__file__))
        dir_path = Path(dir).joinpath(f"backtesting_history{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path.joinpath(f"{exchange}.json")
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

    def update_trending_top(self):
        self.trending_top = False
        for _, strategy in self.engine.strategies.items():
            if strategy.trending_step >= strategy.top_step:
                self.trending_top = True
                break

    def notice_strategy_info(self):
        total = 0
        error_count = 0
        fit_content = ""
        fit_count = 0
        un_fit_content = ""
        un_fit_count = 0
        ding_content = ""
        ding_count = 0
        min_datetime = None
        max_datetime = None
        for _, strategy in self.engine.strategies.items():
            # 总计
            total += 1

            # 回测数据缺失的数量
            backtesting_status = (
                strategy.backtesting_status if strategy.backtesting_status else {}
            )
            if not backtesting_status:
                error_count += 1

            # 回测趋势追踪等级
            backtesting_step = backtesting_status.get("trending_step", 0)

            # 回测截止时间
            backtesting_to = strategy.backtesting_to

            # 最小回测截止时间
            min_datetime = (
                min(min_datetime, backtesting_to) if min_datetime else backtesting_to
            )

            # 最大回测截止时间
            max_datetime = (
                max(max_datetime, backtesting_to) if max_datetime else backtesting_to
            )

            # 实盘持仓信息
            position_value = abs(strategy.pos) * strategy.position_price

            # 回测和实盘比较趋势追踪等级是否一致
            if backtesting_step or strategy.trending_step:
                if backtesting_step == strategy.trending_step:
                    content = f"\nstrategy_name:{strategy.strategy_name}\nstrategy_pos:{strategy.pos}\nstrategy_position_price:{strategy.position_price}\nstrategy_position_value:{position_value}\nstrategy_pnl:{strategy.current_pnl_rate}\nstrategy_bottom: {strategy.bottom_step}\nstrategy_top: {strategy.top_step}\n\nstrategy_step: {strategy.trending_step}\nbacktesting_step: {backtesting_step}\nbacktesting_to: {backtesting_to}"
                    fit_content += content
                    fit_content += "\n\n" + "-" * 10 + "\n\n"
                    fit_count += 1

                    if strategy.trending_step >= 3:
                        ding_content += content
                        ding_content += "\n\n" + "-" * 10 + "\n\n"
                        ding_count += 1

                else:
                    if (
                        strategy.trending_step == 0
                        and backtesting_step < strategy.bottom_step
                    ):
                        continue
                    un_fit_content += f"\nstrategy_name:{strategy.strategy_name}\nstrategy_pos:{strategy.pos}\nstrategy_position_price:{strategy.position_price}\nstrategy_position_value:{position_value}\nstrategy_pnl:{strategy.current_pnl_rate}\nstrategy_bottom: {strategy.bottom_step}\nstrategy_top: {strategy.top_step}\n\nstrategy_step: {strategy.trending_step}\nbacktesting_step: {backtesting_step}\nbacktesting_to: {backtesting_to}"
                    un_fit_content += "\n\n" + "-" * 10 + "\n\n"
                    un_fit_count += 1

        # 邮件发送通知
        fit_content = (
            f"\n策略总数：{total}\n回测周期：{min_datetime} - {max_datetime}\n" + fit_content
        )
        self.engine.send_email(msg=fit_content, subject=f"马丁策略组合状态信息【正常：{fit_count}】")

        un_fit_content = (
            f"\n策略总数：{total}\n回测周期：{min_datetime} - {max_datetime}\n" + un_fit_content
        )
        self.engine.send_email(
            msg=un_fit_content, subject=f"马丁策略组合状态信息【非正常：{un_fit_count}】"
        )

        if error_count:
            error_content = f"\n策略总数：{total}\n回测周期：{min_datetime} - {max_datetime}\n"
            self.engine.send_email(
                msg=error_content, subject=f"马丁策略组合状态信息【回测状态缺失数量：{error_count}】"
            )

        if ding_count:
            ding_content = (
                f"\n马丁策略组合状态信息【高等级追踪：{ding_count}】\n\n策略总数：{total}\n回测周期：{min_datetime} - {max_datetime}\n"
                + ding_content
            )
            self.engine.main_engine.send_ding_talk(content=ding_content)
