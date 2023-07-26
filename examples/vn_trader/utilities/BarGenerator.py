# -- coding: utf-8 --

from typing import Callable, Optional
from vnpy.trader.object import BarData, TickData
from vnpy.trader.constant import Exchange, Interval
from pymongo import MongoClient, ASCENDING, DESCENDING
from vnpy.app.cta_strategy.base import (
    MINUTE_DB_NAME,
    HOUR_DB_NAME,
    MinuteDataBaseName,
    HourDataBaseName,
)
import re
from datetime import datetime, timedelta
from time import sleep
from threading import Thread
from copy import copy

# 将上一级目录添加到模块搜索路径中
import sys

sys.path.append("..")
from App.Turtle_crypto.dataservice.BybitDataService import (
    bybit_get_symbol_list,
    BybitSymbolType,
)

from App.Turtle_crypto.dataservice.BinanceDataService import binance_get_symbol_list
from App.Turtle_crypto.dataservice.OKXDataService import okx_get_symbol_list, OKXType


class BarGenerator:
    """
    For:
    1. generating 1 minute bar data from tick data
    2. generating x minute bar/x hour bar data from 1 minute data

    Notice:
    1. for x minute bar, x must be able to divide 60: 2, 3, 5, 6, 10, 15, 20, 30
    2. for x hour bar, x can be any number
    """

    def __init__(
        self,
        on_bar: Callable = None,
        window: int = 0,
        on_window_bar: Callable = None,
        interval: Interval = Interval.MINUTE,
    ):
        """Constructor"""
        self.bar: BarData = None
        self.on_bar: Callable = on_bar
        self.bar_start = False

        self.interval: Interval = interval

        self.hour_bar: BarData = None

        self.window: int = window
        self.window_bar: BarData = None
        self.on_window_bar: Callable = on_window_bar
        self.window_start = False

        self.last_tick: TickData = None

    def update_tick(self, tick: TickData) -> None:
        """
        Update new tick data into generator.
        """
        tick = copy(tick)
        tick.datetime = tick.datetime.replace(microsecond=0)
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return

        # Filter tick data with older timestamp
        if self.last_tick and tick.datetime < self.last_tick.datetime:
            return

        if not self.bar:
            new_minute = True

        elif (self.bar.datetime.minute != tick.datetime.minute) or (
            self.bar.datetime.hour != tick.datetime.hour
        ):
            if self.bar_start:
                self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)
                self.on_bar(self.bar)
                new_minute = True

            else:
                # 初始周期
                self.bar_start = True
                new_minute = True

        if new_minute:
            self.bar = BarData(
                symbol=tick.symbol,
                exchange=tick.exchange,
                interval=Interval.MINUTE,
                datetime=tick.datetime,
                gateway_name=tick.gateway_name,
                open_price=tick.last_price,
                high_price=tick.last_price,
                low_price=tick.last_price,
                close_price=tick.last_price,
                open_interest=tick.open_interest,
            )
        else:
            self.bar.high_price = max(self.bar.high_price, tick.last_price)
            if tick.high_price > self.last_tick.high_price:
                self.bar.high_price = max(self.bar.high_price, tick.high_price)

            self.bar.low_price = min(self.bar.low_price, tick.last_price)
            if tick.low_price < self.last_tick.low_price:
                self.bar.low_price = min(self.bar.low_price, tick.low_price)

            self.bar.close_price = tick.last_price
            self.bar.open_interest = tick.open_interest
            self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)

            turnover_change = tick.turnover - self.last_tick.turnover
            self.bar.turnover += max(turnover_change, 0)

        self.last_tick = tick

    def update_bar(self, bar: BarData) -> None:
        """
        Update 1 minute bar into generator
        """
        if self.interval == Interval.MINUTE:
            self.update_bar_minute_window(bar)
        else:
            self.update_bar_hour_window(bar)

    def update_bar_minute_window(self, bar: BarData) -> None:
        """"""
        # If not inited, create window bar object
        if not self.window_bar:
            dt = bar.datetime.replace(second=0, microsecond=0)
            self.window_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
            )
        # Otherwise, update high/low price into window bar
        else:
            self.window_bar.high_price = max(self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(self.window_bar.low_price, bar.low_price)

        # Update close price/volume/turnover into window bar
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += bar.volume
        self.window_bar.turnover += bar.turnover
        self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        if not (bar.datetime.minute + 1) % self.window:
            if self.window_start:
                self.on_window_bar(self.window_bar)
                self.window_bar = None

            else:
                # 初始周期
                self.window_start = True
                self.window_bar = None

    def update_bar_hour_window(self, bar: BarData) -> None:
        """"""
        # If not inited, create window bar object
        if not self.hour_bar:
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest,
            )
            return

        finished_bar = None

        # If minute is 59, update minute bar into window bar and push
        if bar.datetime.minute == 59:
            self.hour_bar.high_price = max(self.hour_bar.high_price, bar.high_price)
            self.hour_bar.low_price = min(self.hour_bar.low_price, bar.low_price)

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

            finished_bar = self.hour_bar
            self.hour_bar = None

        # If minute bar of new hour, then push existing window bar
        elif bar.datetime.hour != self.hour_bar.datetime.hour:
            finished_bar = self.hour_bar

            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)
            self.hour_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest,
            )
        # Otherwise only update minute bar
        else:
            self.hour_bar.high_price = max(self.hour_bar.high_price, bar.high_price)
            self.hour_bar.low_price = min(self.hour_bar.low_price, bar.low_price)

            self.hour_bar.close_price = bar.close_price
            self.hour_bar.volume += bar.volume
            self.hour_bar.turnover += bar.turnover
            self.hour_bar.open_interest = bar.open_interest

        # Push finished window bar
        if finished_bar:
            self.on_hour_bar(finished_bar)

    def on_hour_bar(self, bar: BarData) -> None:
        """"""
        if self.window == 1:
            self.on_window_bar(bar)
        else:
            if not self.window_bar:
                self.window_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=bar.datetime,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price,
                )
            else:
                self.window_bar.high_price = max(
                    self.window_bar.high_price, bar.high_price
                )
                self.window_bar.low_price = min(
                    self.window_bar.low_price, bar.low_price
                )

            self.window_bar.close_price = bar.close_price
            self.window_bar.volume += bar.volume
            self.window_bar.turnover += bar.turnover
            self.window_bar.open_interest = bar.open_interest

            if not (bar.datetime.hour + 1) % self.window:
                if self.window_start:
                    self.on_window_bar(self.window_bar)
                    self.window_bar = None
                else:
                    # 初始周期
                    self.window_start = True
                    self.window_bar = None


class MinuteBarProcessor:
    def __init__(
        self,
        symbol: str = "",
        window: int = 0,
        interval: Interval = Interval.MINUTE,
        start_date: str = "",
        end_date: str = "",
        from_data_base: bool = False,
    ):
        self.symbol = symbol
        self.window = window
        self.interval = interval

        if start_date:
            self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            self.start_date = None

        if end_date:
            self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            self.end_date = None

        # minute_bar数据库
        client = MongoClient("localhost", 27017)
        minute_bar_db = client[MINUTE_DB_NAME]
        self.minute_bar_collection = minute_bar_db[self.symbol]

        # window_bar数据库
        if self.interval == Interval.MINUTE:
            window_bar_db = client[MinuteDataBaseName(self.window)]
        else:
            window_bar_db = client[HourDataBaseName(self.window)]
        self.window_bar_collection = window_bar_db[self.symbol]
        self.window_bar_collection.create_index("datetime")

        # 根据数据库最新数据决定起始时间
        if from_data_base:
            start_data = self.window_bar_collection.find_one(
                sort=[("datetime", ASCENDING)]
            )
            db_start_dt = start_data["datetime"] if start_data else None
            end_data = self.window_bar_collection.find_one(
                sort=[("datetime", DESCENDING)]
            )
            db_end_dt = end_data["datetime"] if end_data else None
            print(f"{self.symbol}数据库起止时间\t{db_start_dt}\t{db_end_dt}")
            if db_end_dt:
                self.start_date = db_end_dt - timedelta(minutes=self.window*10)

        self.bar_generator = BarGenerator(
            window=self.window, on_window_bar=self.on_window_bar, interval=self.interval
        )

    def on_window_bar(self, bar: BarData):
        self.window_bar_collection.update_many(
            {"datetime": bar.datetime}, {"$set": bar.__dict__}, upsert=True
        )

    def start(self):
        print(f"{self.symbol}开始")
        start_dt = None
        end_dt = None
        minute_bar = None

        flt = {}
        flt_data = {}
        if self.start_date:
            flt_data["$gte"] = self.start_date
        if self.end_date:
            flt_data["$lte"] = self.end_date

        if flt_data:
            flt["datetime"] = flt_data
        cursor = self.minute_bar_collection.find(flt).sort("datetime", ASCENDING)
        for d in cursor:
            minute_bar = BarData(
                gateway_name="", symbol="", exchange=None, datetime=None
            )
            minute_bar.__dict__ = d
            if not start_dt:
                start_dt = minute_bar.datetime
            self.bar_generator.update_bar(minute_bar)

        if minute_bar:
            end_dt = minute_bar.datetime

        interval_ = re.sub("\d", "", self.interval.value)
        print(
            f"{self.symbol}\n1m -> {self.window}{interval_}\n{start_dt} -> {end_dt}\n\n"
        )


class MultiThreadsMinuteBarProcessor:
    def __init__(
        self,
        symbol_list: list = [],
        window: int = 0,
        interval: Interval = Interval.MINUTE,
        start_date: str = "",
        end_date: str = "",
        from_data_base: bool = False,
    ):
        self.symbol_list = symbol_list
        self.window = window
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.from_data_base = from_data_base
        self.threads = []
        self.loading_complete = True

    def remove_thread(self, thread):
        if thread in self.threads:
            self.threads.remove(thread)

    def start(self):
        # 多线程获取数据
        self.loading_complete = False
        for symbol in self.symbol_list:
            while len(self.threads) >= 10:
                sleep(2)
            thread = ProcessorThread(
                engine=self,
                symbol=symbol,
                window=self.window,
                interval=self.interval,
                start_date=self.start_date,
                end_date=self.end_date,
                from_data_base=self.from_data_base,
            )
            self.threads.append(thread)
            thread.start()
        
        # 所有合约装载完成
        self.loading_complete = True


class ProcessorThread(object):
    def __init__(
        self,
        engine,
        symbol: str = "",
        window: int = 0,
        interval: Interval = Interval.MINUTE,
        start_date: str = "",
        end_date: str = "",
        from_data_base: bool = False,
    ):
        self.engine = engine
        self.symbol = symbol
        self.window = window
        self.interval = interval
        self.start_date = start_date
        self.end_date = end_date
        self.from_data_base = from_data_base

        self.thread = Thread(target=self.run)
        self.active = False

    def run(self):
        processor = MinuteBarProcessor(
            symbol=self.symbol,
            window=self.window,
            interval=self.interval,
            start_date=self.start_date,
            end_date=self.end_date,
            from_data_base=self.from_data_base,
        )
        processor.start()

        # 终止线程
        self.close()

    def start(self) -> None:
        if self.active:
            return

        self.active = True
        self.thread.start()

    def close(self) -> None:
        if not self.active:
            return

        self.active = False
        self.engine.remove_thread(self)


def get_full_symbol(symbol_list):
    full_symbol_list = []
    for symbol in symbol_list:
        full_symbol = f"{symbol}.{Exchange.BYBIT.value}"
        full_symbol_list.append(full_symbol)
    return full_symbol_list


if __name__ == "__main__":
    exchange = input('选择交易所【Binance：1 OKX：2 Bybit：3】')
    if exchange == "2":
        exchange = "OKX"
        symbol_list = okx_get_symbol_list(type=OKXType.USDT)
    
    elif exchange == "3":
        exchange = "BYBIT"
        symbol_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)

    else:
        exchange = "BINANCE"
        symbol_list = binance_get_symbol_list()
    
    print("\n")
    for symbol in symbol_list:
        print(symbol)
    print(f"\n交易所：{exchange}\n合约总数：{len(symbol_list)}")
    sleep(2)

    # fake
    # start_ = 40
    # end_ = 60
    # print(f"\n本次下载起止合约：{symbol_list[start_]} -> {symbol_list[end_-1]}")
    # symbol_list = symbol_list[start_:end_]
    # print(symbol_list)
    # print(f"总计：{len(symbol_list)}\n")
    # sleep(2)

    symbol_list = [f"{symbol}.{exchange}" for symbol in symbol_list]
    processor = MultiThreadsMinuteBarProcessor(
        symbol_list=symbol_list,
        window=5,
        interval=Interval.MINUTE,
        start_date="2020-1-1",
        end_date="2023-12-31",
        from_data_base=True,
    )
    processor.start()
