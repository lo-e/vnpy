from datetime import datetime, timedelta
from pymongo import MongoClient
from vnpy.app.cta_strategy.base import (
    DAILY_DB_NAME,
    MINUTE_DB_NAME,
    HOUR_DB_NAME,
    MinuteDataBaseName,
    HourDataBaseName,
)
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    Status,
    Product,
    OptionType,
    OrderType,
)
from vnpy.trader.object import BarData

SYMBOL_LIST = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GALAUSDT", "AVAXUSDT", "XRPUSDT"]
EXCHANGE = "BYBIT"


def get_full_symbol():
    full_symbol_list = []
    for symbo in SYMBOL_LIST:
        full_symbol = symbo + f".{EXCHANGE}"
        full_symbol_list.append(full_symbol)
    return full_symbol_list


class MultiSymbol(object):
    """
    1、设置标的列表
    2、设置周期
    3、获取数据
    4、计算指标
    5、做出交易决策
    """

    # 初始化
    def __init__(
        self,
        start: datetime = datetime.now(),
        end: datetime = datetime.now(),
        interval: Interval = Interval.MINUTE,
        window: int = 1,
    ):
        # 设置标的、起始时间
        self.full_symbol_list = get_full_symbol()
        self.start = start
        self.end = end

        # 根据周期设置确定数据库
        mc = MongoClient()
        db_name = self.get_db_name(interval=interval, window=window)
        self.dataBase = mc[db_name]

        # 查询处理后的数据保存对象
        self.datetime_barlist_dic = {}
        self.datetime_price_change_dic = {}

    # 获取数据库名称
    def get_db_name(self, interval: Interval, window: int):
        if interval == Interval.MINUTE:
            db_name = MinuteDataBaseName(window)

        elif interval == Interval.HOUR:
            db_name = HourDataBaseName(window)

        else:
            db_name = DAILY_DB_NAME

        return db_name

    # 导入数据
    def load_data(self):
        for symbol in self.full_symbol_list:
            flt = {"datetime": {"$gte": self.start, "$lte": self.end}}

            collection = self.dataBase[symbol]
            cursor = collection.find(flt).sort("datetime")
            bar_list = []
            for d in cursor:
                bar = BarData(gateway_name="", symbol="", exchange="", datetime=None)
                bar.__dict__ = d
                bar_list = self.datetime_barlist_dic.get(bar.datetime, [])
                bar_list.append(bar)
                self.datetime_barlist_dic[bar.datetime] = bar_list

    # 数据处理
    def process_data(self):
        self.datetime_price_change_dic = {}
        for the_datetime, barlist in self.datetime_barlist_dic.items():
            long_list = []
            short_list = []
            for bar in barlist:
                price_change = (
                    (bar.close_price - bar.open_price) / bar.open_price
                ) * 100
                data = {"symbol": bar.symbol, "bar": bar, "change": price_change}
                if price_change >= 0:
                    long_list.append(data)
                elif price_change < 0:
                    short_list.append(data)
            self.datetime_price_change_dic[the_datetime] = {
                "long": long_list,
                "short": short_list,
            }

    def generate_result(self):
        for the_datetime, price_chage_data in self.datetime_price_change_dic.items():
            long_list = price_chage_data["long"]
            short_list = price_chage_data["short"]
            print(
                f"{the_datetime}\t上涨：{len(long_list)}\t下跌：{len(short_list)}\t总计：{len(long_list) + len(short_list)}"
            )


if __name__ == "__main__":
    engine = MultiSymbol(
        start=datetime.now() - timedelta(days=2),
        end=datetime.now() -timedelta(days=1),
        interval=Interval.MINUTE,
        window=5,
    )
    engine.load_data()
    engine.process_data()
    engine.generate_result()
    print(len(engine.datetime_price_change_dic))
