import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
import shutil
from queue import Empty, Queue
from threading import Thread
import time
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import DIR_SYMBOL

# 统计交易盈亏
def statistics_pnl():
    main_dir_path = f"data{DIR_SYMBOL}trade_logs"
    dt_trades_data = {}
    for root, _, files in os.walk(main_dir_path):
        for file in files:
            if "GAINERS" in file:
                direction = Direction.LONG
            
            else:
                direction = Direction.SHORT
            symbol = file.split("_")[2]

            file_path = f"{root}{DIR_SYMBOL}{file}"
            df = pd.read_csv(file_path)
            logs = df["LOG"].to_list()

            open_date_time = ""
            slot = 0
            for log in logs:
                elements = log.split(" ")
                offset = elements[2]

                if offset == "OPEN":
                    # 开仓，获取合约槽位数
                    offset = Offset.OPEN
                    open_date_time = f"{elements[0]} {elements[1]}"
                    slot = int(elements[3])
                
                else:
                    offset = Offset.CLOSE
                    close_date_time = f"{elements[0]} {elements[1]}"
                    pnl_rate = float(elements[3].split("%")[0])
                    if not slot:
                        # raise("slot数据缺失！")a
                        print(f"{symbol} {close_date_time} slot数据缺失！")
                        continue

                    data = {"symbol": symbol,
                            "direction": direction,
                            "open_date_time": open_date_time,
                            "close_date_time": close_date_time,
                            "slot": slot,
                            "pnl_rate": pnl_rate}
                    
                    trades_data = dt_trades_data.get(close_date_time, [])
                    trades_data.append(data)
                    dt_trades_data[close_date_time] = trades_data

                    open_date_time = ""
                    slot = 0

    # 按交易时间排序
    sorted_dt_trades_data = dict(sorted(dt_trades_data.items(), key=lambda x: x[0]))

    # 统计盈亏
    dt_pnl_data = {}
    for dt, trades_data in sorted_dt_trades_data.items():
        dt_pnl = 0
        for data in trades_data:
            slog = data["slot"]
            pnl_rate = data["pnl_rate"]
            dt_pnl += pnl_rate / slog

        dt_pnl_data[dt] = round(dt_pnl, 2)
    
    total_pnl = 0
    for dt, dt_pnl in dt_pnl_data.items():
        total_pnl += dt_pnl
        print(f"{dt}\t{dt_pnl}\t{total_pnl}")

    print(f"总计盈亏：{total_pnl}")

    # df = pd.DataFrame(results)
    # df = df.sort_values(by="close_time")
    # for i, row in df.iterrows():
    #     mint = row["mint"]

if __name__ == "__main__":
    statistics_pnl()