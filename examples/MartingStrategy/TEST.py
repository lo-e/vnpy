import os
from vnpy.trader.utility import DIR_SYMBOL
import csv
import pandas as pd
from datetime import datetime


# 计算每个加仓阶段的亏损状态
def calculate_phase_loss(phase_count: int, increase_type: int = 1):
    phase_value_list = [
        500.0,
        1500.0,
        3500.0,
        7500.0,
        15500.0,
        31500.0,
        63500.0,
        127500.0,
        255500.0,
        511500.0,
        1023500.0,
        2047500.0,
    ]
    if increase_type == 0:
        increase_rate_list = [0.01 * 2 * i for i in range(len(phase_value_list))]

    elif increase_type == 1:
        increase_rate_list = [0.01 * 2**i for i in range(len(phase_value_list))]
    else:
        exit("检查代码！")
    init_price = 100

    value_before = phase_value_list[0]
    price_before = init_price
    phase_list = range(phase_count)[1:]
    print(
        f"\n第{1}阶段\n持仓价值：{value_before}\n持仓数量：{value_before/price_before}\n持仓均价：{price_before}"
    )
    for i in phase_list:
        value_after = phase_value_list[i]
        increase_rate = increase_rate_list[i]
        increase_rate = min(increase_rate, 0.32)
        trade_price = price_before * (1 + increase_rate)

        trade_volume = (value_after - value_before) / trade_price
        price_after = value_after / ((value_before / price_before) + trade_volume)

        loss_before = ((trade_price / price_before) - 1) * 100
        loss_before = round(loss_before, 2)
        loss_before = f"{loss_before}%"

        loss_after = ((trade_price / price_after) - 1) * 100
        loss_after = round(loss_after, 2)
        loss_after = f"{loss_after}%"

        print(
            f"\n第{i+1}阶段\n持仓价值：{value_after}\n持仓数量：{value_after/price_after}\n持仓均价：{price_after}\n成交价格：{trade_price}\n加仓前亏损：{loss_before}\n加仓后亏损：{loss_after}"
        )

        value_before = value_after
        price_before = price_after

    symbol_price_changed = ((trade_price / init_price) - 1) * 100
    symbol_price_changed = round(symbol_price_changed, 2)
    symbol_price_changed = f"{symbol_price_changed}%"

    position_price_changed = ((price_before / init_price) - 1) * 100
    position_price_changed = round(position_price_changed, 2)
    position_price_changed = f"{position_price_changed}%"
    print(
        f"\n\n============\n总共经历{phase_count}个阶段加仓\n初始持仓价值：{phase_value_list[0]}\t初始持仓价格：{init_price}\n最后持仓价值：{value_before}\t最后持仓价格：{price_before}\n持仓价格变化：{position_price_changed}\n合约价格变化：{symbol_price_changed}"
    )


# 分析trending_continuous下的趋势追踪结果
def analyse_trending_continuous(by_month: bool = False, target_dir: str = ""):
    # 趋势追踪程度
    continuous_open_dict = {}
    continuous_open_symbol_dict = {}
    continuous_symbol_open_dict = {}
    month_open_symbol_dict = {}
    month_symbol_open_dict = {}

    path = os.path.abspath(__file__)
    file_name = path.split(DIR_SYMBOL)[-1]
    main_dir_path = path.rstrip(file_name) + f"trending_continuous{DIR_SYMBOL}"
    for root, _, files in os.walk(main_dir_path):
        if DIR_SYMBOL in root:
            dir_name = root.split(DIR_SYMBOL)[-1]
            if target_dir and dir_name and dir_name != target_dir:
                continue

            start_end = dir_name.split("_")
            if start_end and len(start_end) == 2:
                start = start_end[0]
                end = start_end[1]
                print(f"\n====== 起止日期：{start} - {end} ======")
                
            else:
                continue

        for theFile in files:
            # 排除不合法文件
            if theFile.startswith("."):
                continue

            if ".csv" in theFile:
                # 读取文件
                filePath = root + DIR_SYMBOL + theFile
                with open(filePath, "r") as f:
                    continuous_open = 0

                    # 开始导入数据
                    reader = csv.DictReader(f)
                    for row in reader:
                        dt = row["datetime"]
                        signal = row["signal"]
                        symbol = signal[0 : len(signal) - 2]
                        # 分割行
                        if not dt:
                            continue

                        trending = row["trending"]
                        if trending == f"加仓":
                            continuous_open += 1
                        else:
                            # 记录趋势追踪程度
                            continuous_key = str(continuous_open)
                            count = continuous_open_dict.get(continuous_key, 0)
                            count += 1
                            continuous_open_dict[continuous_key] = count

                            # 趋势追踪的信号统计1
                            signal_dict = continuous_open_symbol_dict.get(
                                continuous_key, {}
                            )
                            signal_list = signal_dict.get(symbol, [])
                            signal_list.append(row)
                            signal_dict[symbol] = signal_list
                            continuous_open_symbol_dict[continuous_key] = signal_dict

                            # 趋势追踪的信号统计2
                            open_dict = continuous_symbol_open_dict.get(symbol, {})
                            open_list = open_dict.get(continuous_key, [])
                            open_list.append(row)
                            open_dict[continuous_key] = open_list
                            continuous_symbol_open_dict[symbol] = open_dict

                            # 月份趋势追踪的信号统计1
                            month_str = datetime.strptime(
                                dt, "%Y-%m-%d %H:%M:%S"
                            ).strftime("%Y-%m")
                            month_dict = month_open_symbol_dict.get(month_str, {})
                            signal_dict = month_dict.get(continuous_key, {})
                            signal_list = signal_dict.get(symbol, [])
                            signal_list.append(row)
                            signal_dict[symbol] = signal_list
                            month_dict[continuous_key] = signal_dict
                            month_open_symbol_dict[month_str] = month_dict

                            # 月份趋势追踪的信号统计2
                            month_dict = month_symbol_open_dict.get(month_str, {})
                            open_dict = month_dict.get(symbol, {})
                            open_list = open_dict.get(continuous_key, [])
                            open_list.append(row)
                            open_dict[continuous_key] = open_list
                            month_dict[symbol] = open_dict
                            month_symbol_open_dict[month_str] = month_dict

                            # 连续加仓重置
                            continuous_open = 0

    # 连续趋势追踪程度统计
    print(f"\n****** 连续趋势追踪程度统计 ******")
    continuous_keys = list(continuous_open_dict.keys())
    continuous_keys = sorted(continuous_keys)
    for continuous_key in continuous_keys:
        count = continuous_open_dict[continuous_key]
        print(f"{continuous_key}\t{count}")

    if by_month:
        month_keys = list(month_open_symbol_dict.keys())
        month_keys = sorted(month_keys)
        for month in month_keys:
            print(f"\n====== 按月统计：{month} ======")

            # 连续趋势追踪信号统计
            open_symbol_dict = month_open_symbol_dict[month]
            output_open_symbol_result(open_symbol_dict)

            # 信号连续趋势追踪统计
            symbol_open_dict = month_symbol_open_dict[month]
            output_symbol_open_result(symbol_open_dict)

    else:
        # 连续趋势追踪信号统计
        output_open_symbol_result(continuous_open_symbol_dict)

        # 信号连续趋势追踪统计
        output_symbol_open_result(continuous_symbol_open_dict)


def output_open_symbol_result(open_symbol_dict: dict):
    continuous_keys = list(open_symbol_dict.keys())
    continuous_keys = sorted(continuous_keys)
    for continuous_key in continuous_keys:
        print(f"\n****** 连续趋势追踪{continuous_key}信号统计 ******")
        symbol_dict = open_symbol_dict[continuous_key]
        # 进行次数倒叙排序
        symbol_times_list = []
        for symbol, symbol_data_list in symbol_dict.items():
            symbol_times_list.append({"symbol": symbol, "times": len(symbol_data_list)})
        df = pd.DataFrame(symbol_times_list)
        df = df.sort_values("times", ascending=False)
        for _, row in df.iterrows():
            symbol = row["symbol"]
            symbol_data_list = symbol_dict[symbol]
            print(f"{symbol}有{len(symbol_data_list)}次记录")
            # for symbol_data in symbol_data_list:
            #     dt = symbol_data["datetime"]
            #     signal = symbol_data["signal"]
            #     position_price = symbol_data["position_price"]
            #     position_value = symbol_data["position_value"]
            #     max_loss_value = symbol_data["max_loss_value"]
            #     max_loss_rate = symbol_data["max_loss_rate"]
            #     trending = symbol_data["trending"]
            #     print(
            #         f"{dt}\t{signal}\t{position_price}\t{position_value}\t{max_loss_value}\t{max_loss_rate}\t{trending}"
            #     )


def output_symbol_open_result(symbol_open_dict: dict):
    all_symbol_set = set()
    setting_file_path = "setting.csv"
    with open(setting_file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_symbol_set.add(row["symbol"])

    max_2_symbol_set = all_symbol_set - set(list(symbol_open_dict.keys()))
    max_3_symbol_set = set()
    max_4_symbol_set = set()
    for symbol, open_dict in symbol_open_dict.items():
        print(f"\n{symbol}的连续趋势追踪记录")
        max_3 = True
        max_4 = True
        for continuous_key, data_list in open_dict.items():
            if int(continuous_key) > 3:
                max_3 = False

            if int(continuous_key) > 4:
                max_4 = False

            print(f"追踪{continuous_key}\t{len(data_list)}")

        if max_3:
            max_3_symbol_set.add(symbol)

        elif max_4:
            max_4_symbol_set.add(symbol)

    print(f"\n最大连续趋势追踪2的合约总数：{len(max_2_symbol_set)}")
    for symbol in max_2_symbol_set:
        print(symbol)

    print(f"\n最大连续趋势追踪3的合约总数：{len(max_3_symbol_set)}")
    for symbol in max_3_symbol_set:
        print(symbol)

    print(f"\n最大连续趋势追踪4的合约总数：{len(max_4_symbol_set)}")
    for symbol in max_4_symbol_set:
        print(symbol)


if __name__ == "__main__":
    # 计算每个加仓阶段的亏损状态
    # calculate_phase_loss(phase_count=6, increase_type=1)

    # 分析trending_continuous下的趋势追踪结果
    analyse_trending_continuous(by_month=True, target_dir="2022-01-01_2023-05-27")
