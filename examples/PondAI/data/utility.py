import pymongo
from time import time
import csv
from datetime import datetime
from vnpy.trader.object import BarData
from enum import Enum
from vnpy.app.cta_strategy.base import (
    HOUR_DB_NAME
)

class CSVsBarLocalEngine(object):
    def __init__(self):
        super(CSVsBarLocalEngine, self).__init__()
        self.client = pymongo.MongoClient("localhost", 27017)

    def csv_to_mongodb(self, file_name:str, db_name:str):
        count = 0
        startTime = time()
        bar_db = self.client[db_name]
        print(f"====== {file_name} -> {db_name} ======")
        # 读取文件
        with open(file_name, "r") as f:
            reader = csv.DictReader(f)
            # 开始导入数据
            for row in reader:
                # 数据库collection
                vt_symbol = row["symbol"]
                collection = bar_db[vt_symbol]
                collection.create_index("datetime")

                # 确定日期
                dt_str = row["datetime"]
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

                # 创建BarData对象
                bar = BarData(
                    gateway_name="",
                    symbol=vt_symbol,
                    exchange=None,
                    datetime=dt
                )
                bar.open_price = float(row["open"])
                bar.high_price = float(row["high"])
                bar.low_price = float(row["low"])
                bar.close_price = float(row["close"])

                # 保存bar到数据库
                collection.update_many(
                    {"datetime": bar.datetime},
                    {"$set": bar.__dict__},
                    upsert=True,
                )
    
                # 打印进程
                count += 1
                if count % 100 == 0:
                    sub = time() - startTime
                    print("用时：", sub, "s")
                    print("数据量：", count, "\n")

if __name__ == "__main__":
    engine = CSVsBarLocalEngine()
    engine.csv_to_mongodb(file_name="eth_price.csv", db_name=HOUR_DB_NAME)