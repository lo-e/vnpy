# encoding: UTF-8

"""
立即下载数据到数据库中，用于手动执行更新操作。
"""

from .BybitDataService import (
    bybit_get_bar_data,
    bybit_get_symbol_list,
    BybitSymbolType,
    bybit_get_first_bar_datetime,
)
from .OKXDataService import okx_get_bar_data, okx_get_first_bar_datetime
from .BinanceDataService import (
    binance_get_bar_data,
    BinanceType,
    binance_get_first_bar_datetime,
)
from .CSVsToLocal import (
    CSVsBybitBarLocalEngine,
    CSVsOKXBarLocalEngine,
    CSVsBinanceBarLocalEngine,
)
from .BarToLocal import BarLocalEngine
from datetime import datetime, timedelta
import shutil
import os
from vnpy.trader.constant import Interval
from time import time, sleep
from threading import Thread
from vnpy.app.cta_strategy.base import MINUTE_DB_NAME
from pymongo import MongoClient, ASCENDING, DESCENDING
from enum import Enum
from .utility import get_csv_path


class ExchangeType(Enum):
    BINANCE = "BINANCE"
    OKX = "OKX"
    BYBIT = "BYBIT"
    NONE = "NONE"


class TurtleCryptoDataDownloading(object):
    def __init__(self):
        self.threads = []
        self.loading_complete = True
        pass

    def remove_thread(self, thread):
        if thread in self.threads:
            self.threads.remove(thread)

    def download_from_bybit(
        self,
        contract_list,
        days=1,
        to_date: datetime = None,
        from_data_base: bool = False,
        api_check: bool = False,
    ):
        if not to_date:
            to_date = datetime.now() + timedelta(days=2)

        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 多线程获取数据
        self.loading_complete = False
        start_time = time()
        for contract in contract_list:
            while len(self.threads) >= 10:
                sleep(2)
            thread = DownloadThread(
                self,
                exchange=ExchangeType.BYBIT,
                contract=contract,
                interval="1",
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=api_check,
            )
            self.threads.append(thread)
            thread.start()
        end_time = time()

        # 避免下载时间过短影响逻辑判断
        if end_time - start_time < 3:
            sleep(2)
        self.loading_complete = True

    def download_from_okx(
        self,
        contract_list,
        days=1,
        to_date: datetime = None,
        from_data_base: bool = False,
        api_check: bool = False,
    ):
        if not to_date:
            to_date = datetime.now() + timedelta(days=2)

        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 多线程获取数据
        self.loading_complete = False
        start_time = time()
        for contract in contract_list:
            while len(self.threads) >= 10:
                sleep(2)
            thread = DownloadThread(
                self,
                exchange=ExchangeType.OKX,
                contract=contract,
                interval="1m",
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=api_check,
            )
            self.threads.append(thread)
            thread.start()
        end_time = time()

        # 避免下载时间过短影响逻辑判断
        if end_time - start_time < 3:
            sleep(2)
        self.loading_complete = True

    def download_from_binance(
        self,
        contract_list,
        days=1,
        to_date: datetime = None,
        from_data_base: bool = False,
        api_check: bool = False,
    ):
        if not to_date:
            to_date = datetime.now() + timedelta(days=2)

        # 先删除原有文件夹，包括其中所有内容
        csv_path = get_csv_path()
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)

        # 多线程获取数据
        self.loading_complete = False
        start_time = time()
        for contract in contract_list:
            while len(self.threads) >= 10:
                sleep(2)
            thread = DownloadThread(
                self,
                exchange=ExchangeType.BINANCE,
                contract=contract,
                interval="1m",
                days=days,
                to_date=to_date,
                from_data_base=from_data_base,
                api_check=api_check,
            )
            self.threads.append(thread)
            thread.start()
        end_time = time()

        # 避免下载时间过短影响逻辑判断
        if end_time - start_time < 3:
            sleep(2)
        self.loading_complete = True

    def generate_for_bybit(self, contract_list, days=1):
        result = True
        complete_msg = ""
        back_msg = ""
        lost_msg = ""

        if not contract_list:
            return False, complete_msg, back_msg, lost_msg

        # 1m数据合成Daily数据
        print("\n====== 1m数据合成Daily数据 ======")
        engine = BarLocalEngine()

        from_day = datetime.now() - timedelta(days=days)
        to_day = datetime.now()
        start_date = datetime.strptime(
            f"{from_day.year}-{from_day.month}-{from_day.day} 08:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        end_date = datetime.strptime(
            f"{to_day.year}-{to_day.month}-{to_day.day} 07:59:00", "%Y-%m-%d %H:%M:%S"
        )

        for contract in contract_list:
            symbol = f"{contract}.BYBIT"
            re, c_msg, b_msg, l_msg = engine.Crypto_1Min_Daily(
                symbol=symbol, start_date=start_date, end_date=end_date
            )
            if not re:
                result = False
            complete_msg += c_msg + "\n\n"
            back_msg += b_msg + "\n\n"
            lost_msg += l_msg + "\n\n"

        return result, complete_msg, back_msg, lost_msg

    def generate_8h_for_bybit(self, contract_list, days=1):
        result = True
        complete_msg = ""
        back_msg = ""
        lost_msg = ""

        if not contract_list:
            return False, complete_msg, back_msg, lost_msg

        # 1m数据合成Daily数据
        print("\n====== 1m数据合成Daily数据 ======")
        engine = BarLocalEngine()

        from_day = datetime.now() - timedelta(days=days)
        to_day = datetime.now()
        start_date = datetime.strptime(
            f"{from_day.year}-{from_day.month}-{from_day.day} 08:00:00",
            "%Y-%m-%d %H:%M:%S",
        )
        end_date = datetime.strptime(
            f"{to_day.year}-{to_day.month}-{to_day.day} 07:59:00", "%Y-%m-%d %H:%M:%S"
        )

        for contract in contract_list:
            symbol = f"{contract}.BYBIT"
            re, c_msg, b_msg, l_msg = engine.Crypto_1Min_8H(
                symbol=symbol, start_date=start_date, end_date=end_date
            )
            if not re:
                result = False
            complete_msg += c_msg + "\n\n"
            back_msg += b_msg + "\n\n"
            lost_msg += l_msg + "\n\n"

        return result, complete_msg, back_msg, lost_msg


class DownloadThread(object):
    def __init__(
        self,
        engine,
        exchange: ExchangeType,
        contract,
        interval,
        days=1,
        to_date: datetime = datetime.now() + timedelta(days=2),
        from_data_base: bool = False,
        api_check: bool = False,
    ):
        self.engine = engine
        self.exchange = exchange
        self.contract = contract
        self.interval = interval
        self.days = days
        self.to_date = to_date
        self.from_data_base = from_data_base
        self.api_check = api_check

        self.thread = Thread(target=self.run)
        self.active = False

    def run(self):
        if (
            self.exchange != ExchangeType.BINANCE
            and self.exchange != ExchangeType.OKX
            and self.exchange != ExchangeType.BYBIT
        ):
            exit(f"交易所类型错误")

        #"""
        # 获取bar数据
        print(f"====== {self.contract}开始下载 ======")
        from_time = datetime.now() - timedelta(days=self.days)
        from_time = datetime(from_time.year, from_time.month, from_time.day)

        # 接口获取合约起始时间
        first_bar_dt = None
        if self.api_check:
            request_needed = True
            while request_needed:
                try:
                    if self.exchange == ExchangeType.BINANCE:
                        first_bar_dt = binance_get_first_bar_datetime(
                            symbol=self.contract,
                            interval=self.interval,
                            symbol_type=BinanceType.USDT,
                            start_time=datetime.strftime(
                                from_time, "%Y-%m-%d %H:%M:%S"
                            ),
                        )

                    elif self.exchange == ExchangeType.OKX:
                        first_bar_dt = okx_get_first_bar_datetime(
                            symbol=self.contract,
                            from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"),
                        )

                    elif self.exchange == ExchangeType.BYBIT:
                        first_bar_dt = bybit_get_first_bar_datetime(
                            symbol=self.contract,
                            interval=self.interval,
                            from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"),
                        )

                    request_needed = False
                except:
                    sleep(2)

        if self.from_data_base:
            client = MongoClient("localhost", 27017)
            db = client[MINUTE_DB_NAME]
            symbol = self.contract
            if self.exchange == ExchangeType.BINANCE:
                symbol = self.contract + ".BINANCE"

            elif self.exchange == ExchangeType.OKX:
                symbol = self.contract + ".OKX"

            elif self.exchange == ExchangeType.BYBIT:
                symbol = self.contract + ".BYBIT"
            collection = db[symbol]

            if first_bar_dt:
                flt = {"datetime": {"$gte": from_time}}
                cursor = collection.find(flt).sort("datetime", ASCENDING)
                dt_list = []
                if cursor:
                    for bar in list(cursor):
                        dt_list.append(bar["datetime"])
                if dt_list:
                    db_start_dt = dt_list[0]
                    db_end_dt = dt_list[-1]
                    print(f"{self.contract}数据库起止时间\t{db_start_dt}\t{db_end_dt}\t")

                    if db_start_dt <= first_bar_dt:
                        virtual_dt_list = []
                        i = first_bar_dt
                        while i <= db_end_dt:
                            virtual_dt_list.append(i)
                            i += timedelta(minutes=1)
                        sub = set(virtual_dt_list).difference(set(dt_list))
                        if sub:
                            # 数据库数据缺失
                            loss_dt = sorted(list(sub))[0]
                            from_time = loss_dt - timedelta(minutes=10)
                            print(
                                f"!!!!!! {self.contract}数据库数据缺失【from：{loss_dt}】 !!!!!!"
                            )
                        else:
                            # 数据库数据完整
                            from_time = db_end_dt - timedelta(minutes=10)

            else:
                start_data = collection.find_one(sort=[("datetime", ASCENDING)])
                db_start_dt = start_data["datetime"] if start_data else None
                end_data = collection.find_one(sort=[("datetime", DESCENDING)])
                db_end_dt = end_data["datetime"] if end_data else None

                print(f"{self.contract}数据库起止时间\t{db_start_dt}\t{db_end_dt}")
                if db_end_dt:
                    from_time = db_end_dt - timedelta(minutes=10)

        to_time = datetime(self.to_date.year, self.to_date.month, self.to_date.day)
        while from_time:
            if from_time >= to_time:
                break

            print(f"下载数据：{from_time}\t{self.contract}")
            download_failed = False
            try:
                if self.exchange == ExchangeType.BINANCE:
                    from_time = binance_get_bar_data(
                        symbol=self.contract,
                        interval=self.interval,
                        symbol_type=BinanceType.USDT,
                        start_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"),
                        end_time=datetime.strftime(to_time, "%Y-%m-%d %H:%M:%S"),
                    )

                elif self.exchange == ExchangeType.OKX:
                    from_time = okx_get_bar_data(
                        symbol=self.contract,
                        interval=self.interval,
                        from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"),
                    )

                elif self.exchange == ExchangeType.BYBIT:
                    from_time = bybit_get_bar_data(
                        symbol=self.contract,
                        interval=self.interval,
                        from_time=datetime.strftime(from_time, "%Y-%m-%d %H:%M:%S"),
                    )

                else:
                    print(f"交易所类型错误")
                    break
            except Exception:
                download_failed = True
                print("****** 下载中断 ******")

            if download_failed:
                sleep(2)

            elif from_time:
                from_time = from_time + timedelta(minutes=1)
        #"""
        
        # 1m数据入数据库
        print("\n====== 1m数据入数据库 ======")
        if self.exchange == ExchangeType.BINANCE:
            engine = CSVsBinanceBarLocalEngine(duration="1m", contract=self.contract)
            engine.startWork()

        elif self.exchange == ExchangeType.OKX:
            engine = CSVsOKXBarLocalEngine(duration="1m", contract=self.contract)
            engine.startWork()
            pass

        elif self.exchange == ExchangeType.BYBIT:
            engine = CSVsBybitBarLocalEngine(duration="1", contract=self.contract)
            engine.startWork()
        
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
