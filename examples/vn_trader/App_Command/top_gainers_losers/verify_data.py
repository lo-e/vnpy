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
def statistics_pnl(for_eth: bool = False):
    main_dir_path = f"data{DIR_SYMBOL}trade_logs"
    dt_trades_data = {}
    for root, _, files in os.walk(main_dir_path):
        for file in files:
            if "GAINERS" in file:
                direction = Direction.LONG
            
            else:
                direction = Direction.SHORT
            symbol = file.split("_")[2]
            if for_eth and symbol != "ETHETH":
                continue

            if not for_eth and symbol == "ETHETH":
                continue

            file_path = f"{root}{DIR_SYMBOL}{file}"
            df = pd.read_csv(file_path)
            logs = df["LOG"].to_list()

            open_date_time = ""
            open_tick_time = ""
            cross = False
            last_close_date_time = ""
            last_profit = False
            conmtinuous_over_loss_count = 0
            over_loss_datetime = ""
            for log in logs:
                elements = log.split(" ")
                offset = elements[4]

                if offset == "OPEN":
                    # 开仓
                    offset = Offset.OPEN
                    open_date_time_ = f"{elements[0]} {elements[1]}"
                    open_ts = datetime.strptime(open_date_time_, "%Y-%m-%d %H:%M:%S").timestamp()

                    open_tick_time_ = f"{elements[2]} {elements[3]}"
                    open_tick_ts = datetime.strptime(open_tick_time_, "%Y-%m-%d %H:%M:%S").timestamp()
                    
                    open_delay = abs(open_ts - open_tick_ts)
                    if open_delay >= 10:
                        print(f"{symbol}\t{direction.value}\tOPEN_DELAY\t{open_delay}\t{open_date_time_}\t{open_tick_time_}")

                    if not open_date_time:
                        open_date_time = open_date_time_

                    if not open_tick_time:
                        open_tick_time = open_tick_time_
                
                elif offset == "CROSS":
                    cross = True
                    
                elif offset == "OPEN_COUNT":
                    offset = Offset.CLOSE
                    close_date_time = f"{elements[0]} {elements[1]}"
                    close_tick_time = f"{elements[2]} {elements[3]}"
                    close_ts = datetime.strptime(close_date_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    close_tick_ts = datetime.strptime(close_tick_time, "%Y-%m-%d %H:%M:%S").timestamp()
                    close_delay = abs(close_ts - close_tick_ts)
                    if close_delay >= 10:
                        print(f"{symbol}\t{direction.value}\tCLOSE_DELAY\t{close_delay}\t{close_date_time}\t{close_tick_time}")

                    open_count = int(elements[5])
                    stop_count = int(elements[7])
                    stop_rate = float(elements[9].split("%")[0])
                    pnl_rate = float(elements[11].split("%")[0])
                    
                    entry_drawdown = False
                    entry_drawdown = elements[13]
                    entry_drawdown = True if entry_drawdown == "True" else False
                    trending_ts = float(elements[17])

                    data = {"symbol": symbol,
                            "direction": direction,
                            "open_date_time": open_date_time,
                            "open_tick_time": open_tick_time,
                            "close_date_time": close_date_time,
                            "close_tick_time": close_tick_time,
                            "last_close_date_time": last_close_date_time,
                            "last_profit": last_profit,
                            "cross": cross,
                            "open_count":open_count,
                            "stop_count":stop_count,
                            "stop_rate":stop_rate,
                            "pnl_rate": pnl_rate,
                            "entry_drawdown": entry_drawdown,
                            "trending_ts": trending_ts,
                            "over_loss_datetime": over_loss_datetime}
                    
                    trades_data = dt_trades_data.get(close_date_time, [])
                    trades_data.append(data)
                    dt_trades_data[close_date_time] = trades_data

                    open_date_time = ""
                    open_tick_time = ""
                    cross = False
                    last_close_date_time = close_date_time

                    # 上次盈利
                    if pnl_rate > 0 and stop_count <= 0:
                        last_profit = True
                    
                    else:
                        last_profit = False

                    # 连续大亏，暂停下一次交易
                    if stop_count >= 2:
                        conmtinuous_over_loss_count += 1
                    
                    else:
                        conmtinuous_over_loss_count = 0
                    
                    if conmtinuous_over_loss_count >= 2:
                        over_loss_datetime = close_date_time

                    else:
                        over_loss_datetime = ""

    # 按交易时间排序
    sorted_dt_trades_data = dict(sorted(dt_trades_data.items(), key=lambda x: x[0]))

    # 统计盈亏
    total_pnl = 0
    stop_pnl_error_count = 0
    pnl_count = 0
    for dt, trades_data in sorted_dt_trades_data.items():
        # 仓位盈亏、开仓时间
        for data in trades_data:
            symbol = data["symbol"]
            direction = data["direction"].value
            open_date_time = data["open_date_time"]
            open_ts = datetime.strptime(open_date_time, "%Y-%m-%d %H:%M:%S").timestamp() if open_date_time else 0
            cross = data["cross"]
            open_count = data["open_count"]
            stop_count = data["stop_count"]
            stop_rate = data["stop_rate"]
            pnl_rate = data["pnl_rate"]
            entry_drawdown = data["entry_drawdown"]
            if open_count == stop_count and pnl_rate:
                raise(f"平仓盈亏异常，检查数据！")

            # 实际盈亏
            # real_pnl_rate = pnl_rate + stop_rate - open_count * 0.1
            # if stop_count >= 2 and abs(stop_rate) < 0.5:
            #     # 止损盈亏数据异常判断
            #     stop_pnl_error_count += 1
            #     real_pnl_rate -= stop_count * 0.8

            if open_count > 2:
                if stop_count <= 2:
                    real_pnl_rate = stop_rate - 0.1 * 2
                
                else:
                    real_pnl_rate = -0.9 * 2 - 0.1 * 2

            else:
                real_pnl_rate = pnl_rate + stop_rate - open_count * 0.1

            # 持仓时间
            close_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
            position_time = close_ts - open_ts if open_ts else 0
            position_minute = int(position_time / 60)
            position_second = int(position_time - position_minute * 60)

            # 择时开仓
            trending_ts = data["trending_ts"]
            trending_time = datetime.fromtimestamp(trending_ts).strftime(f"%Y-%m-%d %H:%M:%S")
            last_close_date_time = data["last_close_date_time"]
            last_close_ts = datetime.strptime(last_close_date_time, "%Y-%m-%d %H:%M:%S").timestamp() if last_close_date_time else 0
            if (open_ts - last_close_ts > 4 * 60 * 60) and (open_ts - trending_ts > 2 * 60 * 60):
            # if (open_ts - trending_ts > 2 * 60 * 60):
                real_pnl_rate = 0
            
            over_loss_flag = False
            if data["over_loss_datetime"]:
                over_loss_flag = True
                real_pnl_rate = 0

            # 累计盈亏
            total_pnl += real_pnl_rate
            if real_pnl_rate:
                pnl_count += 1

            msg = f"{trending_time}\t{open_date_time} - {dt}\t{direction}\t{position_minute}m {position_second}s\tcross {cross}\tentry_drawdown {entry_drawdown}\topen {open_count}\tstop {stop_count}\tstop_pnl {stop_rate:.3f}\tpnl {pnl_rate:.3f}\t{real_pnl_rate:.3f}\t{total_pnl:.3f}\t{symbol}"
            if over_loss_flag:
                msg = f"{msg}\t*"
                
            print(msg)

    print(f"止损盈亏异常数：{stop_pnl_error_count}")
    print(f"盈亏交易数：{pnl_count}")
    print(f"总计盈亏：{total_pnl}")

if __name__ == "__main__":
    statistics_pnl(for_eth=False)