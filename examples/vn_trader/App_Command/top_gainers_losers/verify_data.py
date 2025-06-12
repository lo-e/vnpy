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
            open_tick_time = ""
            slot = 0
            for log in logs:
                elements = log.split(" ")
                offset = elements[4]

                if offset == "OPEN":
                    # 开仓，获取合约槽位数
                    offset = Offset.OPEN
                    open_date_time = f"{elements[0]} {elements[1]}"
                    open_tick_time = f"{elements[2]} {elements[3]}"
                    open_ts = datetime.strptime(open_date_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    open_tick_ts = datetime.strptime(open_tick_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    open_delay = abs(open_ts - open_tick_ts)
                    if open_delay >= 10:
                        print(f"{symbol}\t{direction.value}\tOPEN_DELAY\t{open_delay}\t{open_date_time}\t{open_tick_time}")
                    slot = int(elements[5])
                
                else:
                    offset = Offset.CLOSE
                    close_date_time = f"{elements[0]} {elements[1]}"
                    close_tick_time = f"{elements[2]} {elements[3]}"
                    close_ts = datetime.strptime(close_date_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    close_tick_ts = datetime.strptime(close_tick_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    close_delay = abs(close_ts - close_tick_ts)
                    if close_delay >= 10:
                        print(f"{symbol}\t{direction.value}\tCLOSE_DELAY\t{close_delay}\t{close_date_time}\t{close_tick_time}")

                    pnl_rate = float(elements[5].split("%")[0])
                    if not slot:
                        # raise("slot数据缺失！")a
                        print(f"{symbol} {close_date_time} slot数据缺失！")
                        continue

                    data = {"symbol": symbol,
                            "direction": direction,
                            "open_date_time": open_date_time,
                            "open_tick_time": open_tick_time,
                            "close_date_time": close_date_time,
                            "close_tick_time": close_tick_time,
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
    total_pnl = 0
    for dt, trades_data in sorted_dt_trades_data.items():
        # 仓位盈亏、开仓时间
        dt_pnl = 0
        open_ts = 0
        direction = ""
        for data in trades_data:
            direction_ = data["direction"].value
            direction = f"{direction}{direction_}" if direction_ not in direction else direction
            open_date_time = data["open_date_time"]
            ts = datetime.strptime(open_date_time, "%Y-%m-%d %H:%M:%S").timestamp()
            open_ts = min(open_ts, ts) if open_ts else ts

            slot = data["slot"]
            pnl_rate = data["pnl_rate"]
            dt_pnl += pnl_rate / slot

            # if open_date_time == "2025-06-12 15:25:04":
            #     symbol = data["symbol"]
            #     print(f"{symbol}\t{direction_}\t{open_date_time}")

        # 持仓时间
        close_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
        position_time = close_ts - open_ts
        position_minute = int(position_time / 60)
        position_second = int(position_time - position_minute * 60)

        # 累计盈亏
        total_pnl += dt_pnl

        opent_dt = datetime.fromtimestamp(open_ts).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{opent_dt} - {dt}\t{direction}\t{position_minute}m {position_second}s\t{len(trades_data)}\t{slot}\t{dt_pnl:.3f}\t{total_pnl:.3f}")

    print(f"总计盈亏：{total_pnl}")

if __name__ == "__main__":
    statistics_pnl()