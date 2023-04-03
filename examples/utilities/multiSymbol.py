from datetime import date, datetime, timedelta
from lzma import FILTER_DELTA
from pymongo import MongoClient, ASCENDING
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
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 将上一级目录添加到模块搜索路径中
import sys

sys.path.append("..")
from vn_trader.App.Turtle_crypto.dataservice.BybitDataService import (
    bybit_get_symbol_list,
    BybitSymbolType,
)


def get_full_symbol():
    full_symbol_list = []

    symbol_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GALAUSDT", "AVAXUSDT", "XRPUSDT"]
    symbol_list = bybit_get_symbol_list(type=BybitSymbolType.USDT)
    for symbol in symbol_list:
        full_symbol = f"{symbol}.{Exchange.BYBIT.value}"
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
        stop_line: int = 200,
        maker_trade: bool = False,
    ):
        # 设置标的、起始时间
        self.full_symbol_list = get_full_symbol()
        self.start = start
        self.end = end

        # 设置严格止损线%
        self.stop_line = stop_line

        # 设置是否限定maker成交
        self.maker_trade = maker_trade

        # 根据周期设置确定数据库
        self.mc = MongoClient()

        # 查询处理后的数据保存对象
        self.datetime_bar_dic = OrderedDict()
        self.minute5_bar_dic = OrderedDict()
        self.filter_datetime_bar_dic = OrderedDict()
        self.datetime_direction_dic = OrderedDict()
        self.datetime_result_dict = OrderedDict()
        self.pnl_dict = OrderedDict()
        self.exceed_pnl_dic = OrderedDict()
        self.trade_count = 0
        self.total_pnl = 0

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
        self.datetime_bar_dic = self.load_db_data(interval=Interval.HOUR, window=1)
        self.minute5_bar_dic = self.load_db_data(interval=Interval.MINUTE, window=5)

    def load_db_data(self, interval:Interval, window:int):
        temporary_datetime_bar_dic = {}
        result_datetime_bar_dic = {}
        for symbol in self.full_symbol_list:
            flt = {"datetime": {"$gte": self.start, "$lte": self.end}}

            db_name = self.get_db_name(interval=interval, window=window)
            dataBase = self.mc[db_name]
            collection = dataBase[symbol]
            cursor = collection.find(flt).sort("datetime", ASCENDING)
            for d in cursor:
                bar = BarData(gateway_name="", symbol="", exchange="", datetime=None)
                bar.__dict__ = d
                bar_dic = temporary_datetime_bar_dic.get(bar.datetime, {})
                bar_dic[bar.symbol] = bar
                temporary_datetime_bar_dic[bar.datetime] = bar_dic

        # 日期排序
        datetime_list = sorted(list(temporary_datetime_bar_dic.keys()))
        for dt in datetime_list:
            result_datetime_bar_dic[dt] = temporary_datetime_bar_dic[dt]

        # 测试数据
        # for the_datetime, bar_dic in self.datetime_bar_dic.items():
        #     print(f'\n{the_datetime}')
        #     for symbol, bar_data in bar_dic.items():
        #         print(f'{symbol}\t{bar_data.open_price}\t{bar_data.high_price}\t{bar_data.low_price}\t{bar_data.close_price}')
    
        return result_datetime_bar_dic

    # 指定某个属性给数据排序并筛选
    def filter_data(self):
        self.filter_datetime_bar_dic = {}
        for _, bar_dic in self.datetime_bar_dic.items():
            df_data_list = []
            for _, bar in bar_dic.items():
                # 过滤掉稳定币
                if bar.symbol == "USDCUSDT.BYBIT":
                    continue

                # volume按USDT计算
                bar.volume = bar.volume * bar.close_price
                df_data_list.append(bar.__dict__)

            # 选择交易量靠前的标的
            datetime_df = pd.DataFrame(df_data_list)
            datetime_df = datetime_df.sort_values(by="volume", ascending=False)
            head = int(len(datetime_df) / 3)
            datetime_df = datetime_df.head(head)
            for _, row in datetime_df.iterrows():
                filter_bar = BarData(
                    gateway_name="", symbol="", exchange="", datetime=None
                )
                filter_bar.__dict__ = dict(row)
                filter_bar_dic = self.filter_datetime_bar_dic.get(
                    filter_bar.datetime, {}
                )
                filter_bar_dic[filter_bar.symbol] = filter_bar
                self.filter_datetime_bar_dic[filter_bar.datetime] = filter_bar_dic

    # 数据处理
    def process_data(self):
        # 排序并筛选合适的行情数据
        self.filter_data()

        self.datetime_direction_dic = {}
        for the_datetime, bar_dic in self.filter_datetime_bar_dic.items():
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

            market_average_change = 0
            # 市场多时，平均幅度为上涨的平均幅度
            if market_direction == Direction.LONG:
                long_changes = []
                for _, long_data in long_dic.items():
                    long_changes.append(long_data["change"])
                market_average_change = (
                    sum(long_changes) / len(long_changes) if len(long_changes) else 0
                )

            # 市场空时，平均幅度为下跌的平均幅度
            if market_direction == Direction.SHORT:
                short_changes = []
                for _, short_data in short_dic.items():
                    short_changes.append(short_data["change"])
                market_average_change = (
                    sum(short_changes) / len(short_changes) if len(short_changes) else 0
                )

            long_result_dic = {}
            short_result_dic = {}

            # 根据各标的相对大盘的价格走势筛选出有多空趋势的标的
            if market_direction == Direction.LONG:
                # 大盘上涨时
                # 【上涨幅度过大】的标的判断有上涨趋势
                # 【上涨幅度过小】或者【逆市场下跌】的标的判断有下跌趋势
                for symbol, long_data in long_dic.items():
                    change = long_data["change"]
                    if change > market_average_change * 2:
                        short_result_dic[symbol] = {
                            "change": change,
                            "market": market_average_change,
                        }

                    if change <= market_average_change * 0.5:
                        short_result_dic[symbol] = {
                            "change": change,
                            "market": market_average_change,
                        }

                for symbol, short_data in short_dic.items():
                    change = short_data["change"]
                    long_result_dic[symbol] = {
                        "change": change,
                        "market": market_average_change,
                    }

            elif market_direction == Direction.SHORT:
                # 大盘下跌时
                # 【下跌幅度过大】的标的判断有下跌趋势
                # 【下跌幅度过小】或者【逆市场上涨】的标的判断有上涨趋势
                for symbol, short_data in short_dic.items():
                    change = short_data["change"]
                    if change < market_average_change * 2:
                        long_result_dic[symbol] = {
                            "change": change,
                            "market": market_average_change,
                        }

                    if change >= market_average_change * 0.5:
                        long_result_dic[symbol] = {
                            "change": change,
                            "market": market_average_change,
                        }

                for symbol, long_data in long_dic.items():
                    change = long_data["change"]
                    short_result_dic[symbol] = {
                        "change": change,
                        "market": market_average_change,
                    }

            self.datetime_result_dict[the_datetime] = {
                "long": long_result_dic,
                "short": short_result_dic,
            }

        # 测试数据
        # for the_datetime, result_dic in self.datetime_result_dict.items():
        #     direction_dic = self.datetime_direction_dic[the_datetime]
        #     direction_long = direction_dic['long']
        #     direction_short = direction_dic['short']
        #     print(f'\n{the_datetime}')
        #     print(f'上涨数量：{len(direction_long)}\t下跌数量：{len(direction_short)}')
        #     print(f'------ 原始数据 ------')
        #     bar_dic = self.datetime_bar_dic[the_datetime]
        #     for symbol, bar_data in bar_dic.items():
        #         change_data = direction_long.get(symbol, {})
        #         if not change_data:
        #             change_data = direction_short.get(symbol, {})
        #         if not change_data:
        #             exit('检查数据！')
        #         change = change_data['change']
        #         print(f'{symbol}\t{bar_data.open_price}\t{bar_data.close_price}\t{change}%')

        #     print(f'------ LONG ------')
        #     long_result_dic = result_dic['long']
        #     for symbol, result_data in long_result_dic.items():
        #         change = result_data['change']
        #         market = result_data['market']
        #         bar = self.datetime_bar_dic[the_datetime][symbol]
        #         print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%')

        #     print(f'------ SHORT ------')
        #     short_result_dic = result_dic['short']
        #     for symbol, result_data in short_result_dic.items():
        #         change = result_data['change']
        #         market = result_data['market']
        #         bar = self.datetime_bar_dic[the_datetime][symbol]
        #         print(f'{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%')

    # 回测
    def backtesting(self):
        self.trade_count = 0
        self.total_pnl = 0
        last_datetime = None
        last_traded = False

        for the_datetime, _ in self.datetime_result_dict.items():
            if not last_datetime:
                last_datetime = the_datetime
                continue

            last_result_data = self.datetime_result_dict[last_datetime]
            last_long = last_result_data["long"]
            last_short = last_result_data["short"]
            if not len(last_long) or not len(last_short):
                if last_traded:
                    last_traded = False
                else:
                    last_datetime = the_datetime
                    continue
            else:
                self.trade_count += 1
                last_traded = True

            # =============================================================================

            # 前原始数据
            print(f"\n= {last_datetime} =")
            direction_dic = self.datetime_direction_dic[last_datetime]
            direction_long = direction_dic["long"]
            direction_short = direction_dic["short"]
            print(f"上涨数量：{len(direction_long)}\t下跌数量：{len(direction_short)}")
            print(f"- 原始数据 -")
            bar_dic = self.filter_datetime_bar_dic[last_datetime]
            for symbol, bar_data in bar_dic.items():
                change_data = direction_long.get(symbol, {})
                if not change_data:
                    change_data = direction_short.get(symbol, {})
                if not change_data:
                    exit("检查数据！")
                change = change_data["change"]
                # print(
                #     f"{symbol}\t{bar_data.open_price}\t{bar_data.close_price}\t{change}%"
                # )

            # 前交易信号
            print(f"- LONG -")
            for symbol, last_long_result_data in last_long.items():
                change = last_long_result_data["change"]
                market = last_long_result_data["market"]
                bar = self.datetime_bar_dic[last_datetime][symbol]
                print(
                    f"{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%"
                )

            print(f"- SHORT -")
            for symbol, last_short_result_data in last_short.items():
                change = last_short_result_data["change"]
                market = last_short_result_data["market"]
                bar = self.datetime_bar_dic[last_datetime][symbol]
                print(
                    f"{symbol}\t{bar.open_price}\t{bar.close_price}\t{change}%\t市场平均：{market}%"
                )

            # =============================================================================

            print(f"\n= {the_datetime} =")
            print(f"\n盈亏")
            long_pnl = 0
            short_pnl = 0

            # 做多
            print(f"- LONG -")
            for symbol in last_long.keys():
                bar_data = self.datetime_bar_dic[the_datetime][symbol]
                if not bar_data:
                    exit("检查数据！")

                maker_success = (
                    True if bar_data.low_price < bar_data.open_price else False
                )

                close_price_change = (
                    (bar_data.close_price - bar_data.open_price) / bar_data.open_price
                ) * 100
                worst_price_change = (
                    (bar_data.low_price - bar_data.open_price) / bar_data.open_price
                ) * 100

                price_change = close_price_change
                if worst_price_change <= -self.stop_line:
                    # 触及止损
                    price_change = -self.stop_line

                if (self.maker_trade and maker_success) or not self.maker_trade:
                    long_pnl += price_change
                print(
                    f"{symbol}\t{bar_data.open_price}\t{bar_data.high_price}\t{bar_data.low_price}\t{bar_data.close_price}\t{price_change}%\t{maker_success}"
                )

            long_pnl = long_pnl / len(last_long) if len(last_long) else 0
            long_pnl = round_to(long_pnl, 0.001)
            print(f"** {long_pnl} **")

            # 做空
            print(f"\n- SHORT -")
            for symbol in last_short.keys():
                bar_data = self.datetime_bar_dic[the_datetime][symbol]
                if not bar_data:
                    exit("检查数据！")

                maker_success = (
                    True if bar_data.high_price > bar_data.open_price else False
                )

                close_price_change = (
                    (bar_data.close_price - bar_data.open_price) / bar_data.open_price
                ) * 100
                worst_price_change = (
                    (bar_data.high_price - bar_data.open_price) / bar_data.open_price
                ) * 100

                price_change = close_price_change
                if worst_price_change >= self.stop_line:
                    # 触及止损
                    price_change = self.stop_line

                if (self.maker_trade and maker_success) or not self.maker_trade:
                    short_pnl -= price_change
                print(
                    f"{symbol}\t{bar_data.open_price}\t{bar_data.high_price}\t{bar_data.low_price}\t{bar_data.close_price}\t{price_change}%\t{maker_success}"
                )

            short_pnl = short_pnl / len(last_short) if len(last_short) else 0
            short_pnl = round_to(short_pnl, 0.001)
            print(f"** {short_pnl} **")
            print(
                f"\n========================================================================"
            )

            if last_traded:
                # 周期盈亏
                cycle_pnl = (long_pnl + short_pnl) / 2
                if abs(cycle_pnl) >= 2:
                    self.exceed_pnl_dic[the_datetime.strftime("%Y-%m-%d %H:%M")] = {
                        "long": long_pnl,
                        "short": short_pnl,
                        "total": cycle_pnl,
                    }

                # 盈亏统计
                self.total_pnl += cycle_pnl

                # 用于绘制收益曲线
                self.pnl_dict[the_datetime.strftime("%Y-%m-%d %H:%M")] = self.total_pnl

            # 时间更新
            last_datetime = the_datetime


if __name__ == "__main__":
    engine = MultiSymbol(
        start=datetime.now() - timedelta(days=100),
        end=datetime.now() - timedelta(days=2),
        stop_line=2,
        maker_trade=False,
    )
    engine.load_data()
    engine.process_data()
    engine.generate_result()
    engine.backtesting()

    # 概述
    datetime_list = list(engine.datetime_bar_dic.keys())
    print(f"{datetime_list[0]} - {datetime_list[-1]}")
    print(
        f"\n总周期数：{len(engine.datetime_bar_dic)}\n交易的次数：{engine.trade_count}\nMaker手续费：{engine.trade_count*0.02}\n总盈亏：{engine.total_pnl}"
    )
    print(f"\n-- 周期盈亏幅度提示 --")
    for dt, pnl_data in engine.exceed_pnl_dic.items():
        long_ = pnl_data["long"]
        short_ = pnl_data["short"]
        total_ = pnl_data["total"]
        print(f"{dt}\t多：{long_}\t空：{short_}\t总：{total_}")

    # 绘制收益曲线
    x = list(engine.pnl_dict.keys())
    y = list(engine.pnl_dict.values())
    plt.figure(figsize=(20, 10), dpi=100)
    plt.plot(x, y)
    plt.xticks(x[:: int(len(x) / 5)])
    plt.show()
