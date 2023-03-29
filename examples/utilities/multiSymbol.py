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
from vnpy.trader.utility import round_to
from collections import OrderedDict

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
        self.datetime_bar_dic = OrderedDict()
        self.datetime_direction_dic = OrderedDict()
        self.datetime_result_dict = OrderedDict()

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
            for d in cursor:
                bar = BarData(gateway_name="", symbol="", exchange="", datetime=None)
                bar.__dict__ = d
                bar_dic = self.datetime_bar_dic.get(bar.datetime, {})
                bar_dic[bar.symbol] = bar
                self.datetime_bar_dic[bar.datetime] = bar_dic
        
        # 测试数据
        # for the_datetime, bar_dic in self.datetime_bar_dic.items():
        #     print(f'\n{the_datetime}')
        #     for symbol, bar_data in bar_dic.items():
        #         print(f'{symbol}\t{bar_data.open_price}\t{bar_data.high_price}\t{bar_data.low_price}\t{bar_data.close_price}')

    # 数据处理
    def process_data(self):
        self.datetime_direction_dic = {}
        for the_datetime, bar_dic in self.datetime_bar_dic.items():
            long_dic = {}
            short_dic = {}
            for symbol, bar in bar_dic.items():
                price_change = (
                    (bar.close_price - bar.open_price) / bar.open_price
                ) * 100
                price_change = round_to(price_change, 0.001)
                if price_change >= 0:
                    long_dic[symbol] = {"bar": bar, "change": price_change}
                elif price_change < 0:
                    short_dic[symbol] = {"bar": bar, "change": price_change}
            self.datetime_direction_dic[the_datetime] = {
                "long": long_dic,
                "short": short_dic,
            }

        # 测试数据
        # for the_datetime, direction_dic in self.datetime_direction_dic.items():
        #     print(f'\n{the_datetime}')

        #     print(f'------ 上涨 ------')
        #     long_data = direction_dic['long']
        #     for symbol, data in long_data.items():
        #         bar = data['bar']
        #         change = data['change']
        #         print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%')

        #     print(f'------ 下跌 ------')
        #     short_data = direction_dic['short']
        #     for symbol, data in short_data.items():
        #         bar = data['bar']
        #         change = data['change']
        #         print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%')



    # 生成交易信号
    def generate_result(self):
        for the_datetime, direction_data in self.datetime_direction_dic.items():
            long_dic = direction_data["long"]
            short_dic = direction_data["short"]

            # 大盘多空趋势
            market_direction = Direction.NET
            if len(long_dic) >= (len(long_dic) + len(short_dic)) * 0.8:
                market_direction = Direction.LONG

            elif len(short_dic) >= (len(long_dic) + len(short_dic)) * 0.8:
                market_direction = Direction.SHORT

            # 上涨的平均幅度
            long_changes = []
            for _, long_data in long_dic.items():
                long_changes.append(long_data["change"])
            long_change_everage = (
                sum(long_changes) / len(long_changes) if len(long_changes) else 0
            )

            # 下跌的平均幅度
            short_changes = []
            for _, short_data in short_dic.items():
                short_changes.append(short_data["change"])
            short_change_everage = (
                sum(short_changes) / len(short_changes) if len(short_changes) else 0
            )

            # 根据各标的相对大盘的价格走势筛选出有多空趋势的标的
            long_result_dic = {}
            short_result_dic = {}
            if market_direction == Direction.LONG:
                # 大盘上涨时
                # 【上涨幅度过大】的标的判断有上涨趋势
                # 【上涨幅度过小】或者【逆市场下跌】的标的判断有下跌趋势
                for symbol, long_data in long_dic.items():
                    change = long_data["change"]
                    if change > long_change_everage * 2:
                        long_result_dic[symbol] = {
                            "change": change,
                            "market": long_change_everage,
                        }

                    if change <= long_change_everage * 0.5:
                        short_result_dic[symbol] = {
                            "change": change,
                            "market": long_change_everage,
                        }

                for symbol, short_data in short_dic.items():
                    change = short_data["change"]
                    short_result_dic[symbol] = {
                        "change": change,
                        "market": long_change_everage,
                    }

            elif market_direction == Direction.SHORT:
                # 大盘下跌时
                # 【下跌幅度过大】的标的判断有下跌趋势
                # 【下跌幅度过小】或者【逆市场上涨】的标的判断有上涨趋势
                for symbol, short_data in short_dic.items():
                    change = short_data["change"]
                    if change < short_change_everage * 2:
                        short_result_dic[symbol] = {
                            "change": change,
                            "market": short_change_everage,
                        }

                    if change >= short_change_everage * 0.5:
                        long_result_dic[symbol] = {
                            "change": change,
                            "market": short_change_everage,
                        }

                for symbol, long_data in long_dic.items():
                    change = long_data["change"]
                    long_result_dic[symbol] = {
                        "change": change,
                        "market": short_change_everage,
                    }

            self.datetime_result_dict[the_datetime] = {
                "long": long_result_dic,
                "short": short_result_dic,
            }
            
        # 测试数据
        for the_datetime, result_dic in self.datetime_result_dict.items():
            direction_dic = self.datetime_direction_dic[the_datetime]
            direction_long = direction_dic['long']
            direction_short = direction_dic['short']
            print(f'\n{the_datetime}')
            print(f'上涨数量：{len(direction_long)}\t下跌数量：{len(direction_short)}')
            print(f'------ 原始数据 ------')
            bar_dic = self.datetime_bar_dic[the_datetime]
            for symbol, bar_data in bar_dic.items():
                change_data = direction_long.get(symbol, {})
                if not change_data:
                    change_data = direction_short.get(symbol, {})
                if not change_data:
                    exit('检查数据！')
                change = change_data['change']
                print(f'{symbol}\t{bar_data.open_price}\t{bar_data.close_price}\t{change}%')

            print(f'------ LONG ------')
            long_result_dic = result_dic['long']
            for symbol, result_data in long_result_dic.items():
                change = result_data['change']
                market = result_data['market']
                bar = self.datetime_bar_dic[the_datetime][symbol]
                print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%')

            print(f'------ SHORT ------')
            short_result_dic = result_dic['short']
            for symbol, result_data in short_result_dic.items():
                change = result_data['change']
                market = result_data['market']
                bar = self.datetime_bar_dic[the_datetime][symbol]
                print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%')


    # 回测
    def backtesting(self):
        last_result_data = None
        for the_datetime, result_data in self.datetime_result_dict.items():
            long_result_dic = result_data["long"]
            short_result_dic = result_data["short"]
            print(f"{the_datetime}")
            print("做多：")
            for symbol in long_result_dic.keys():
                print(f"{symbol}")

            print("------------")
            print("做空：")
            for symbol in short_result_dic.keys():
                print(f"{symbol}")
            print("\n\n")

            if not last_result_data:
                last_result_data = result_data
                continue

            current_price_change = self.datetime_direction_dic[the_datetime]


if __name__ == "__main__":
    engine = MultiSymbol(
        start=datetime.now() - timedelta(days=20),
        end=datetime.now() - timedelta(days=1),
        interval=Interval.HOUR,
        window=1,
    )
    engine.load_data()
    engine.process_data()
    engine.generate_result()
    # engine.backtesting()
    print(len(engine.datetime_direction_dic))
