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
        "trending_top",
        "total_strategy_value",
    ]
    syncList = ["today", "trending_top"]

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
        self.backtesting_saved_count_down = 60  # 保存策略回测历史倒计时（秒）
        self.backtesting_saved_preparing = False  # 准备保存策略回测历史，倒计时的开关

        self.backtesting_thread = Thread(target=self.run_strategy_backtesting)
        self.backtesting_thread.start()
        self.backtesting_queue = Queue()
        self.strategy_backtesting = False # 策略组合是否正在回测
        self.strategy_backtesting_cost = 0 # 策略组合回测用时
        self.strategy_backtesting_time = 0 # 策略组合回测开始时间

        # 其它
        self.strategy_symbols = []  # 策略合约列表
        self.trending_top = False  # 策略组合中是否有策略已经达到趋势追踪最高级别
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
        self.strategys_backtesting_history = {}
        self.load_backtesting_history()

        # 策略组合合约杠杆
        self.strategys_symbol_leverage = {}

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
                contract_list=contract_list, days=5, from_data_base=True
            )

        elif exchange == "BINANCE":
            self.download_engine.download_from_binance(
                contract_list=contract_list, days=5, from_data_base=True
            )

    def download_initing(self):
        if not self.downloading_for_init:
            self.downloading_for_init = True
            self.downloading_wait = 350

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

    def check_open_cross(self, open_value):
        self.update_strategys_position_value()
        result_value = self.total_strategy_value + open_value
        if result_value >= self.portfolioValue * 20:
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
        loss_tick_symbols_forward = set()
        loss_tick_symbols_inverse = set()
        error_count = 0
        forward_content = ""
        forward_count = 0
        fit_content = ""
        fit_count = 0
        un_fit_content = ""
        un_fit_count = 0
        highlight_content = ""
        highlight_count = 0
        b_min_datetime = ""
        b_max_datetime = ""
        s_min_datetime = ""
        s_max_datetime = ""
        # fake
        # strategy_to_dict = {}

        for _, strategy in self.engine.strategies.items():
            # 总计
            total += 1

            if not strategy.tick:
                symbol = strategy.vt_symbol.split(".")[0]
                if strategy.forward:
                    loss_tick_symbols_forward.add(symbol)
                else:
                    loss_tick_symbols_inverse.add(symbol)

            # 回测数据缺失的数量
            strategy_status = (
                copy(strategy.strategy_status) if strategy.strategy_status else {}
            )
            if not strategy_status:
                error_count += 1

            # 回测趋势追踪等级
            strategy_step = strategy_status.get("trending_step", 0)

            # 回测截止时间
            backtesting_to = strategy.backtesting_to
            strategy_to = strategy.strategy_to

            # fake
            # key = "none"

            if backtesting_to:
                # 最小回测截止时间
                b_min_datetime = (
                    min(b_min_datetime, backtesting_to) if b_min_datetime else backtesting_to
                )

                # 最大回测截止时间
                b_max_datetime = (
                    max(b_max_datetime, backtesting_to) if b_max_datetime else backtesting_to
                )

            if strategy_to:
                # fake
                # key = strategy_to

                # 最小回测截止时间
                s_min_datetime = (
                    min(s_min_datetime, strategy_to) if s_min_datetime else strategy_to
                )

                # 最大回测截止时间
                s_max_datetime = (
                    max(s_max_datetime, strategy_to) if s_max_datetime else strategy_to
                )

            # fake
            # name_list = strategy_to_dict.get(key, [])
            # name_list.append(strategy.strategy_name)
            # strategy_to_dict[key] = name_list

            # 实盘持仓信息
            position_value = abs(strategy.pos) * strategy.position_price

            # 回测和实盘比较趋势追踪等级是否一致
            if strategy_step or strategy.trending_step:
                if strategy.forward:
                    if strategy_step >= 2:
                        content = f"\nsignal_name:{strategy.strategy_name}_趋势追踪\nsignal_pos:{strategy.pos}\nsignal_position_price:{strategy.position_price}\nsignal_position_value:{position_value}\nsignal_pnl:{strategy.current_pnl_rate}\n\nstrategy_step: {strategy_step}\nstrategy_to: {strategy_to}\nstrategy_pnl：{strategy.strategy_current_pnl_rate}"
                        forward_content += content
                        forward_content += "\n\n" + "-" * 10 + "\n\n"
                        forward_count += 1

                else:
                    if strategy_step == strategy.trending_step:
                        content = f"\nsignal_name:{strategy.strategy_name}_反转\nsignal_pos:{strategy.pos}\nsignal_position_price:{strategy.position_price}\nsignal_position_value:{position_value}\nsignal_pnl:{strategy.current_pnl_rate}\nsignal_bottom: {strategy.bottom_step}\nsignal_top: {strategy.top_step}\nsignal_step: {strategy.trending_step}\n\nstrategy_step: {strategy_step}\nstrategy_to: {strategy_to}\nstrategy_pnl：{strategy.strategy_current_pnl_rate}"
                        fit_content += content
                        fit_content += "\n\n" + "-" * 10 + "\n\n"
                        fit_count += 1

                        if strategy.trending_step >= 3:
                            highlight_content += content
                            highlight_content += "\n\n" + "-" * 10 + "\n\n"
                            highlight_count += 1

                    else:
                        if (
                            strategy.trending_step == 0
                            and strategy_step < strategy.bottom_step
                        ):
                            continue
                        
                        content = f"\nsignal_name:{strategy.strategy_name}_反转\nsignal_pos:{strategy.pos}\nsignal_position_price:{strategy.position_price}\nsignal_position_value:{position_value}\nsignal_pnl:{strategy.current_pnl_rate}\nsignal_bottom: {strategy.bottom_step}\nsignal_top: {strategy.top_step}\nsignal_step: {strategy.trending_step}\n\nstrategy_step: {strategy_step}\nstrategy_to: {strategy_to}\nstrategy_pnl：{strategy.strategy_current_pnl_rate}"
                        un_fit_content += content
                        un_fit_content += "\n\n" + "-" * 10 + "\n\n"
                        un_fit_count += 1

                        if strategy.trending_step >= 3:
                            highlight_content += content
                            highlight_content += "\n\n" + "-" * 10 + "\n\n"
                            highlight_count += 1

        total_loss_tick_count = len(loss_tick_symbols_forward) + len(loss_tick_symbols_inverse)
        main_content = f"策略总数：{total}\n行情缺失合约{total_loss_tick_count}：\n（趋势追踪{len(loss_tick_symbols_forward)}）\n{loss_tick_symbols_forward}\n（反转{len(loss_tick_symbols_inverse)}）\n{loss_tick_symbols_inverse}\n回测周期b：{b_min_datetime} - {b_max_datetime}\n回测周期s：{s_min_datetime} - {s_max_datetime}"
        """ 邮件发送通知 """
        # 趋势追踪
        if forward_count:
            forward_content = (
                f"\n{main_content}\n" + forward_content
            )
            self.engine.send_email(msg=forward_content, subject=f"马丁策略组合状态信息【趋势追踪：{forward_count}】")

        # 反转正常
        fit_content = (
            f"\n{main_content}\n" + fit_content
        )
        self.engine.send_email(msg=fit_content, subject=f"马丁策略组合状态信息【正常反转：{fit_count}】")

        # 反转非正常
        un_fit_content = (
            f"\n{main_content}\n" + un_fit_content
        )
        self.engine.send_email(
            msg=un_fit_content, subject=f"马丁策略组合状态信息【非正常反转：{un_fit_count}】"
        )

        # 回测状态缺失
        if error_count:
            error_subject = f"马丁策略组合状态信息【回测状态缺失：{error_count}】"
            error_content = f"\n{main_content}\n"
            self.engine.send_email(
                msg=error_content, subject=error_subject
            )
            
            error_content = f"\n{error_subject}\n{error_content}"
            self.engine.main_engine.send_ding_talk(content=error_content)

        # 高等级反转
        if highlight_count:
            highlight_subject = f"马丁策略组合状态信息【高等级反转：{highlight_count}】"
            highlight_content = (
                f"\n{main_content}\n"
                + highlight_content
            )
            self.engine.send_email(
                msg=highlight_content, subject=highlight_subject
            )

            highlight_content = f"\n{highlight_subject}\n{highlight_content}"
            self.engine.main_engine.send_ding_talk(content=highlight_content)
        
        # fake
        # to_content = ""
        # for to_, name_list in strategy_to_dict.items():
        #     to_content += f"\n\n{to_}\n总数：{len(name_list)}\n{name_list}"
        # self.engine.send_email(
        #     msg=to_content, subject="回测时间详情"
        # )

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
        with open(json_file, "r", encoding="utf-8") as f:
            json_data = json.load(f)
            if json_data:
                self.strategys_symbol_leverage = json_data

    