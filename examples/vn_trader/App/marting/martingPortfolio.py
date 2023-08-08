# encoding: UTF-8

from collections import defaultdict
from vnpy.trader.constant import Direction, Offset
import re
from datetime import datetime
from copy import copy
from App.Turtle_crypto.dataservice import TurtleCryptoDataDownloading
from time import time, sleep
from threading import Thread
from utilities.BarGenerator import MultiThreadsMinuteBarProcessor
from vnpy.trader.constant import Interval
from vnpy.event import Event
import os
import json
from pathlib import Path
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue

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
        "downloading_trigger",
        "is_downloading",
        "downloading_cost",
        "downloading_wait",
        "is_generating",
        "generating_cost",
        "strategy_backtesting",
        "strategy_backtesting_cost",
        "total_strategy_value",
    ]
    syncList = []

    def __init__(self, engine, setting):
        self.engine = engine
        self.on_update_today()

        # 数据下载相关
        self.download_engine = TurtleCryptoDataDownloading()  # 数据下载引擎
        self.downloading_trigger = False  # 开启下载线程
        self.is_downloading = False  # 是否正在下载
        self.downloading_wait = 0  # 数据下载等待时间（秒）
        self.downloading_cost = 0  # 下载更新一次花费的时间
        self.downloading_time = 0  # 下载开始的时间戳
        self.downloading_for_init = False # 是否已经为组合数据初始化下载

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

        self.backtesting_thread = Thread(target=self.run_strategy_backtesting)
        self.backtesting_thread.start()
        self.backtesting_queue = Queue()
        self.strategy_backtesting = False # 策略组合是否正在回测
        self.strategy_backtesting_cost = 0 # 策略组合回测用时
        self.strategy_backtesting_time = 0 # 策略组合回测开始时间

        # 其它
        self.strategy_symbols = []  # 策略合约列表
        self.all_inited = False  # 是否所有策略完成初始化
        self.strategy_info_count_down = 10 * 60  # 每隔一段时间发送策略状态信息通知
        self.total_strategy_value = 0 # 当前策略总持仓价值

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

        # 策略回测历史
        self.strategies_sync_data = {}
        self.strategies_sync_cross = False
        self.load_strategies_sync_data()

        # 策略组合合约杠杆
        self.strategys_symbol_leverage = {}

    def on_update_today(self):
        self.today = copy(self.engine.today)
        # 同步到数据库
        self.engine.savePortfolioSyncData()

    def on_timer(self):
        # 通知策略回测
        if self.backtesting_preparing:
            self.backtesting_count_down -= 1
            if self.backtesting_count_down <= 0:
                self.backtesting_count_down = 10
                self.backtesting_preparing = False
                event = Event(BAR_DOWNLOAD_GENERATE_COMPLETE)
                self.engine.event_engine.put(event)

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
        if not self.download_engine.loading_complete:
            if self.downloading_trigger:
                self.downloading_trigger = False
                self.is_downloading = True
                self.downloading_wait = 0
                self.downloading_time = time()

        elif not self.downloading_trigger and not len(self.download_engine.threads):
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
            self.downloading_wait >= 350
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

        # 策略组合是否正在回测
        if self.backtesting_queue.qsize():
            if not self.strategy_backtesting:
                # 计时
                self.strategy_backtesting_time = time()
            self.strategy_backtesting = True
        
        else:
            if self.strategy_backtesting and self.strategy_backtesting_time:
                # 统计用时
                self.strategy_backtesting_cost = time() - self.strategy_backtesting_time
                self.strategy_backtesting_time = 0
            self.strategy_backtesting = False

        # 更新组合持仓价值
        self.update_strategys_position_value()

        # 检查保存策略同步信息
        if self.strategies_sync_cross:
            self.strategies_sync_cross = False
            self.save_strategies_sync_data()
        
        # 组合状态更新
        self.engine.put_portfolio_event()

    def download_data(self):
        contract_list = []
        exchange = ""
        for symbol in self.strategy_symbols:
            contract_list.append(symbol.split(".")[0])
            if not exchange:
                exchange = symbol.split(".")[-1]

        if exchange == "BINANCE":
            self.download_engine.download_from_binance(
                contract_list=contract_list, days=5, from_data_base=True
            )

        elif exchange == "OKX":
            self.download_engine.download_from_okx(
                contract_list=contract_list, days=5, from_data_base=True, save_to=self.name
            )
        
        elif exchange == "BYBIT":
            self.download_engine.download_from_bybit(
                contract_list=contract_list, days=5, from_data_base=True
            )

    def download_initing(self):
        if not self.downloading_for_init:
            self.downloading_for_init = True
            self.downloading_wait = 350

    def generate_window_bar(self):
        self.bar_generate_engine.symbol_list = self.strategy_symbols
        self.bar_generate_engine.start()

    def get_strategies_sync_file_path(self):
        dir = os.path.dirname(os.path.realpath(__file__))
        dir_path = Path(dir).joinpath(f"strategies_sync_data{DIR_SYMBOL}BaiduSyncdisk{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path.joinpath(f"{self.name}.json")
        return file_path
    
    def load_strategies_sync_data(self):
        # 从json文件获取策略回测历史
        history_data = {}
        json_file = self.get_strategies_sync_file_path()
        if json_file.exists():
            with open(json_file, mode="r", encoding="UTF-8") as f:
                history_data = json.load(f)
        if history_data:
            self.strategies_sync_data = history_data

    def save_strategies_sync_data_timer(self):
        self.strategies_sync_cross = True

    def save_strategies_sync_data(self):
        # 保存策略同步信息到json文件中
        self.strategies_sync_data = {}
        for _, strategy in self.engine.strategies.items():
            sync_data = {}
            for key in strategy.syncs:
                value = strategy.__getattribute__(key)
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                sync_data[key] = value
            self.strategies_sync_data[
                f"{strategy.strategy_name}"
            ] = sync_data

        json_file = self.get_strategies_sync_file_path()
        try:
            with open(json_file, "w", encoding="utf-8") as file:
                file.write(
                    json.dumps(self.strategies_sync_data, ensure_ascii=False)
                )
        except:
            pass

    def check_open_cross(self, strategy, open_value):
        if strategy.open_waitting:
            return True
        
        self.update_strategys_position_value()
        result_value = self.total_strategy_value + open_value
        if result_value >= self.portfolioValue * 25:
            return False
        else:
            return True
        
    def update_strategys_position_value(self):
        self.total_strategy_value = 0
        for _, strategy in self.engine.strategies.items():
            value_ = strategy.position_price * abs(strategy.pos)
            self.total_strategy_value += value_

    def notice_strategy_info(self):
        total = 0
        loss_tick_symbols = set()

        trading_content = ""
        trading_count = 0

        top_content = ""
        top_count = 0

        min_bar_datetime = ""
        max_bar_datetime = ""

        for _, strategy in self.engine.strategies.items():
            # 总计
            total += 1

            # 行情缺失合约
            if not strategy.tick:
                symbol = strategy.vt_symbol.split(".")[0]
                loss_tick_symbols.add(symbol)

            # Bar截止时间
            bar_dt = strategy.bar_dt
            if bar_dt:
                min_bar_datetime = (
                    min(min_bar_datetime, bar_dt) if min_bar_datetime else bar_dt
                )

                max_bar_datetime = (
                    max(max_bar_datetime, bar_dt) if max_bar_datetime else bar_dt
                )

            # 持仓价值
            position_value = abs(strategy.pos) * strategy.position_price

            # 正在交易的策略信息
            if strategy.trending_step and not strategy.open_waitting:
                content = f"\nsignal_name:{strategy.strategy_name}\nsignal_pos:{strategy.pos}\nsignal_position_price:{strategy.position_price}\nsignal_position_value:{position_value}\nsignal_pnl:{strategy.current_pnl_rate}\nsignal_step: {strategy.trending_step}"
                trading_content += content
                trading_content += "\n\n" + "-" * 10 + "\n\n"
                trading_count += 1

                if strategy.className == "MartingStrategy" and strategy.trending_step >= strategy.top_step:
                    top_content += content
                    top_content += "\n\n" + "-" * 10 + "\n\n"
                    top_count += 1

        """ 邮件发送通知 """
        main_content = f"策略总数：{total}\n行情缺失合约：{len(loss_tick_symbols)}\n{loss_tick_symbols}\nBar截止时间：{min_bar_datetime} - {max_bar_datetime}"
        trading_content = (
            f"\n{main_content}\n" + trading_content
        )
        self.engine.send_email(msg=trading_content, subject=f"马丁策略组合状态信息：{trading_count}")

        if top_count:
            top_content = (
                f"\n{main_content}\n" + top_content
            )
            self.engine.send_email(msg=top_content, subject=f"马丁策略组合状态信息【TOP】：{top_count}")

    def run_strategy_backtesting(self):
        while True:
            try:
                strategy_name = self.backtesting_queue.get(block=True, timeout=1)
                strategy = self.engine.strategies.get(strategy_name, None)
                if strategy:
                    print(f"{strategy_name}\t开始回测")
                    start_t = time()
                    strategy.backtesting_marting()
                    if strategy.window_bar_list:
                        bar = strategy.window_bar_list[-1]
                        print(f"latest_bar_dt：{bar.datetime}\to：{bar.open_price}\th：{bar.high_price}\tl：{bar.low_price}\tc：{bar.close_price}")
                    print(f"{strategy_name}\t回测用时：{time() - start_t}s\n")
            except:
                pass

    def set_strategy_symbols(self, symbols:list):
        self.strategy_symbols = symbols
        self.load_symbol_leverage_data()

    def load_symbol_leverage_data(self):
        # 读取json文件
        exchange = self.strategy_symbols[0].split(".")[-1]
        dir = os.path.dirname(os.path.realpath(__file__))
        dir_path = Path(dir).joinpath(f"leverage{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        json_file = dir_path.joinpath(f"{exchange}.json")
        if os.path.exists(json_file):
            with open(json_file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
                if json_data:
                    self.strategys_symbol_leverage = json_data

    