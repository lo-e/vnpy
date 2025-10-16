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
from enum import Enum
import re

class BacktestingMode(Enum):
    TRENDING_24H = "24H趋势"
    TRENDING_24H_QUICK = "24H快速趋势"
    TRENDING_1H = "1H趋势"

class Backtesting(object):
    def __init__(self):
        self.mode = BacktestingMode.TRENDING_24H
        self.trending_tokens_24h = {}
        self.rise_onboard_data_24h = {}
        self.fall_onboard_data_24h = {}
        self.trending_tokens_1h = {}
        self.signal_tokens_1h = {}
        self.rise_onboard_data_1h = {}
        self.fall_onboard_data_1h = {}
        self.history_rise_symbol_trending_ts = {}
        self.history_fall_symbol_trending_ts = {}
        self.signal_count = 0
        self.trending_rise_24h = False
        self.trending_fall_24h = False

    def start(self, mode: BacktestingMode):
        self.mode = mode
        if "24H" in self.mode.value:
            self.load_recent_24h_trending_data()
        
        elif "1H" in self.mode.value:
            self.load_recent_1h_trending_data()

    def load_recent_24h_trending_data(self):
        # 24小时趋势数据
        print(f"加载24H历史趋势数据..")
        self.trending_tokens_24h = {}
        hour_time = datetime.strptime(f"2025-10-15 00:00:00", f"%Y-%m-%d %H:%M:%S")
        # hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=3)
        while hour_time < datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}24h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        rise_list = []
                        fall_list = []

                        rise_file_path = f"{root}{DIR_SYMBOL}{file}"
                        fall_file_path = rise_file_path.replace("rank_rise", "rank_fall")
                        if os.path.exists(fall_file_path):
                            df_rise = pd.read_csv(rise_file_path)
                            for _, row in df_rise.iterrows():
                                rise_list.append(dict(row))

                            df_fall = pd.read_csv(fall_file_path)
                            for _, row in df_fall.iterrows():
                                fall_list.append(dict(row))

                            # 生成信号
                            if self.mode == BacktestingMode.TRENDING_24H:
                                self.on_trending_data_24h((rise_list, fall_list))

                            elif self.mode == BacktestingMode.TRENDING_24H_QUICK:
                                self.on_trending_data_24h_quick((rise_list, fall_list))
                        
                        else:
                            raise(f"文件状态异常，检查代码")
            
            hour_time += timedelta(hours=1)

        print(f"历史趋势数据加载完成！")

    def load_recent_1h_trending_data(self):
        # 1小时趋势数据
        print(f"加载1H历史趋势数据..")
        self.trending_tokens_1h = {}
        hour_time = datetime.strptime(f"2025-09-10 00:00:00", f"%Y-%m-%d %H:%M:%S")
        # hour_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=5)
        while hour_time < datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        rise_list = []
                        fall_list = []

                        rise_file_path = f"{root}{DIR_SYMBOL}{file}"
                        fall_file_path = rise_file_path.replace("rank_rise", "rank_fall")
                        if os.path.exists(fall_file_path):
                            df_rise = pd.read_csv(rise_file_path)
                            for _, row in df_rise.iterrows():
                                rise_list.append(dict(row))

                            df_fall = pd.read_csv(fall_file_path)
                            for _, row in df_fall.iterrows():
                                fall_list.append(dict(row))

                            # 生成信号
                            self.on_trending_data_1h((rise_list, fall_list))
                        
                        else:
                            raise(f"文件状态异常，检查代码")
            
            hour_time += timedelta(hours=1)
        
        print(f"历史趋势数据加载完成！")

    def on_trending_data_24h(self, data: tuple):
        rise_trending_list, fall_trending_list = data
        data_time = rise_trending_list[0]["change"]
        dt_str_24h = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
        mean_rise_change = rise_trending_list[1]["change"]
        mean_fall_change = rise_trending_list[2]["change"]
        rise_trending_list = rise_trending_list[3:]
        fall_trending_list = fall_trending_list[3:]
        
        trending_tokens = set()
        for i in range(min(len(rise_trending_list), 5)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            volume = data["volume"]
            volume_v = float(re.sub(r'[^\d.]', '', volume))
            volume_u = re.sub(r'[\d.]', '', volume)
            
            trending_tokens.add(symbol)
            if abs(change) >= 100.0:
                if symbol not in self.trending_tokens_24h:
                    trending_data = {"symbol": symbol,
                                     "direction": "LONG",
                                     "trending_ts": data_time,
                                     "trending_dt": dt_str_24h,
                                     "change": change,
                                     "volume": volume}
                            
                    self.trending_tokens_24h[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 5)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            volume = data["volume"]

            trending_tokens.add(symbol)
            if abs(change) >= 100.0:
                if symbol not in self.trending_tokens_24h:
                    trending_data = {"symbol": symbol,
                                     "direction": "SHORT",
                                     "trending_ts": data_time,
                                     "trending_dt": dt_str_24h,
                                     "change": change,
                                     "volume": volume}
                            
                    self.trending_tokens_24h[symbol] = trending_data

        for symbol in self.trending_tokens_24h.copy().keys():
            if symbol not in trending_tokens:
                trending_data = self.trending_tokens_24h[symbol]
                
                direction = trending_data["direction"]
                trending_ts = trending_data["trending_ts"]
                trending_dt = trending_data["trending_dt"]
                change = trending_data["change"]
                volume = trending_data["volume"]
                
                # 上榜时间
                trending_time = int(data_time - trending_ts)

                # 趋势信号
                self.signal_count += 1
                off_dt = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                boarding_hour = int(trending_time / 3600)
                boarding_minute = int((trending_time - (boarding_hour * 3600)) / 60)
                boarding_second = int(trending_time - boarding_hour * 3600 - boarding_minute * 60)
                msg = f"趋势停止 {symbol}\nchange：{change}\nvolume：{volume}\non：{trending_dt}\noff：{off_dt}\ntime：{boarding_hour}h {boarding_minute}m {boarding_second}s\ncount：{self.signal_count}\n"
                print(msg)

                self.trending_tokens_24h.pop(symbol)
        
        # 排序
        self.trending_tokens_24h = dict(sorted(self.trending_tokens_24h.items()))

    def on_trending_data_24h_quick(self, data: tuple):
        rise_trending_list, fall_trending_list = data
        data_time = rise_trending_list[0]["change"]
        mean_rise_change = rise_trending_list[1]["change"]
        mean_fall_change = rise_trending_list[2]["change"]
        rise_trending_list = rise_trending_list[3:]
        fall_trending_list = fall_trending_list[3:]
        is_init = True if not self.rise_onboard_data_24h else False

        rise_onboard_tokens = set()
        for i in range(len(rise_trending_list)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            rise_onboard_tokens.add(symbol)

            if symbol not in self.rise_onboard_data_24h:
                if is_init:
                    self.rise_onboard_data_24h[symbol] = 0

                else:
                    self.rise_onboard_data_24h[symbol] = data_time

        for symbol in self.rise_onboard_data_24h.copy().keys():
            if symbol not in rise_onboard_tokens:
                self.rise_onboard_data_24h.pop(symbol)

                if symbol in self.trending_tokens_24h:
                    self.trending_tokens_24h.pop(symbol)

        fall_onboard_tokens = set()
        for i in range(len(fall_trending_list)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            fall_onboard_tokens.add(symbol)

            if symbol not in self.fall_onboard_data_24h:
                if is_init:
                    self.fall_onboard_data_24h[symbol] = 0

                else:
                    self.fall_onboard_data_24h[symbol] = data_time

        for symbol in self.fall_onboard_data_24h.copy().keys():
            if symbol not in fall_onboard_tokens:
                self.fall_onboard_data_24h.pop(symbol)

                if symbol in self.trending_tokens_24h:
                    self.trending_tokens_24h.pop(symbol)
            
        for i in range(min(len(rise_trending_list), 10)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            onboard_ts = self.rise_onboard_data_24h.get(symbol, 0)
            onboard_time = datetime.fromtimestamp(onboard_ts).strftime(f"%Y-%m-%d %H:%M:%S")
            boarding_time = data_time - onboard_ts
            history_symbol_trending_ts = self.history_rise_symbol_trending_ts.get(symbol, 0)
            self.history_rise_symbol_trending_ts[symbol] = data_time

            if boarding_time <= 10 * 60 and data_time - history_symbol_trending_ts >= 24 * 60 * 60:
                rank_1h = 0
                trending_list_1h = self.load_1h_trending_data(to_ts=data_time, direction="rise")
                if trending_list_1h:
                    data_time_1h = trending_list_1h[0]["change"]
                    if data_time - data_time_1h <= 10 * 60:
                        trending_list_1h = trending_list_1h[3:]
                        symbols_1h = []
                        for data_1h in trending_list_1h:
                            symbols_1h.append(data_1h["symbol"])
                        if symbol in symbols_1h:
                            rank_1h = symbols_1h.index(symbol) + 1

                if 1 <= rank_1h <= 3 and symbol not in self.trending_tokens_24h:
                    trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"direction": "LONG",
                                        "mean_rise": mean_rise_change,
                                        "mean_fall": mean_fall_change,
                                        "change": change,
                                        "rank_1h": rank_1h,
                                        "onboard_ts": onboard_ts,
                                        "onboard_time": onboard_time,
                                        "trending_ts": data_time,
                                        "trending_time": trending_time}
                    self.trending_tokens_24h[symbol] = trending_data

                    self.signal_count += 1
                    boarding_hour = int(boarding_time / 3600)
                    boarding_minute = int((boarding_time - (boarding_hour * 3600)) / 60)
                    boarding_second = int(boarding_time - boarding_hour * 3600 - boarding_minute * 60)
                    msg = f"趋势启动 {symbol}\nmean_rise：{mean_rise_change}\nmean_fall：{mean_fall_change}\nchange：{change}\nrank_1h：{rank_1h}\ntrending：{trending_time}\ntime：{boarding_hour}h {boarding_minute}m {boarding_second}s\ncount：{self.signal_count}\n"
                    print(msg)

        for i in range(min(len(fall_trending_list), 10)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            onboard_ts = self.fall_onboard_data_24h.get(symbol, 0)
            onboard_time = datetime.fromtimestamp(onboard_ts).strftime(f"%Y-%m-%d %H:%M:%S")
            boarding_time = data_time - onboard_ts
            istory_symbol_trending_ts = self.history_fall_symbol_trending_ts.get(symbol, 0)
            self.history_fall_symbol_trending_ts[symbol] = data_time

            if boarding_time <= 10 * 60 and data_time - history_symbol_trending_ts >= 24 * 60 * 60:
                rank_1h = 0
                trending_list_1h = self.load_1h_trending_data(to_ts=data_time, direction="fall")
                if trending_list_1h:
                    data_time_1h = trending_list_1h[0]["change"]
                    if data_time - data_time_1h <= 10 * 60:
                        trending_list_1h = trending_list_1h[3:]
                        symbols_1h = []
                        for data_1h in trending_list_1h:
                            symbols_1h.append(data_1h["symbol"])
                        if symbol in symbols_1h:
                            rank_1h = symbols_1h.index(symbol) + 1

                if 1 <= rank_1h <= 3 and symbol not in self.trending_tokens_24h:
                    trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
                    trending_data = {"direction": "SHORT",
                                        "mean_rise": mean_rise_change,
                                        "mean_fall": mean_fall_change,
                                        "change": change,
                                        "rank_1h": rank_1h,
                                        "onboard_ts": onboard_ts,
                                        "onboard_time": onboard_time,
                                        "trending_ts": data_time,
                                        "trending_time": trending_time}
                            
                    self.trending_tokens_24h[symbol] = trending_data

                    self.signal_count += 1
                    boarding_hour = int(boarding_time / 3600)
                    boarding_minute = int((boarding_time - (boarding_hour * 3600)) / 60)
                    boarding_second = int(boarding_time - boarding_hour * 3600 - boarding_minute * 60)
                    msg = f"趋势启动 {symbol}\nmean_rise：{mean_rise_change}\nmean_fall：{mean_fall_change}\nchange：{change}\nrank_1h：{rank_1h}\ntrending：{trending_time}\ntime：{boarding_hour}h {boarding_minute}m {boarding_second}s\ncount：{self.signal_count}\n"
                    print(msg)

    def on_trending_data_1h(self, data: tuple):
        rise_trending_list, fall_trending_list = data
        data_time = rise_trending_list[0]["change"]
        # mean_rise_change = rise_trending_list[1]["change"]
        # mean_fall_change = rise_trending_list[2]["change"]
        mean_rise_change = pd.DataFrame(rise_trending_list[3:8])["change"].mean()
        mean_fall_change = pd.DataFrame(fall_trending_list[3:8])["change"].mean()

        rise_trending_list = rise_trending_list[3:]
        fall_trending_list = fall_trending_list[3:]

        # 上榜时间
        is_init = True if not self.rise_onboard_data_1h else False
        rise_onboard_tokens = set()
        for i in range(len(rise_trending_list)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            rise_onboard_tokens.add(symbol)

            if symbol not in self.rise_onboard_data_1h:
                if is_init:
                    self.rise_onboard_data_1h[symbol] = 0

                else:
                    self.rise_onboard_data_1h[symbol] = data_time

        for symbol in self.rise_onboard_data_1h.copy().keys():
            if symbol not in rise_onboard_tokens:
                self.rise_onboard_data_1h.pop(symbol)

        fall_onboard_tokens = set()
        for i in range(len(fall_trending_list)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            fall_onboard_tokens.add(symbol)

            if symbol not in self.fall_onboard_data_1h:
                if is_init:
                    self.fall_onboard_data_1h[symbol] = 0

                else:
                    self.fall_onboard_data_1h[symbol] = data_time

        for symbol in self.fall_onboard_data_1h.copy().keys():
            if symbol not in fall_onboard_tokens:
                self.fall_onboard_data_1h.pop(symbol)
        
        # 1H趋势
        trending_tokens = set()
        for i in range(min(len(rise_trending_list), 5)):
            data = rise_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            trending_tokens.add(symbol)

            trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
            trending_data = {
                "direction": "LONG",
                "mean_rise": mean_rise_change,
                "mean_fall": mean_fall_change,
                "change": change,
                "trending_1h_rank": i + 1,
                "trending_1h_ts": data_time,
                "trending_1h_time": trending_time,
                "trending_1h_start": data_time,
                "onboard_ts": self.rise_onboard_data_1h.get(symbol, 0)
                }
            
            history_trending_data = self.trending_tokens_1h.get(symbol, {})
            if history_trending_data:
                trending_data["trending_1h_start"] = history_trending_data["trending_1h_start"]
                    
            self.trending_tokens_1h[symbol] = trending_data

        for i in range(min(len(fall_trending_list), 5)):
            data = fall_trending_list[i]
            symbol = data["symbol"]
            change = data["change"]
            trending_tokens.add(symbol)

            trending_time = datetime.fromtimestamp(data_time).strftime(f"%Y-%m-%d %H:%M:%S")
            trending_data = {
                "direction": "SHORT",
                "mean_rise": mean_rise_change,
                "mean_fall": mean_fall_change,
                "change": change,
                "trending_1h_rank": i + 1,
                "trending_1h_ts": data_time,
                "trending_1h_time": trending_time,
                "trending_1h_start": data_time,
                "onboard_ts": self.fall_onboard_data_1h.get(symbol, 0)
                }
            
            history_trending_data = self.trending_tokens_1h.get(symbol, {})
            if history_trending_data:
                trending_data["trending_1h_start"] = history_trending_data["trending_1h_start"]
                    
            self.trending_tokens_1h[symbol] = trending_data

        # 信号判断
        for symbol, trending_data in self.trending_tokens_1h.copy().items():
            if symbol not in trending_tokens:
                self.trending_tokens_1h.pop(symbol)
            
            else:
                direction = trending_data["direction"]
                mean_rise = trending_data["mean_rise"]
                mean_fall = trending_data["mean_fall"]
                change = trending_data["change"]
                trending_1h_rank = trending_data["trending_1h_rank"]
                trending_1h_ts = trending_data["trending_1h_ts"]
                trending_1h_time = trending_data["trending_1h_time"]
                onboard_ts = trending_data["onboard_ts"]
                onboard_time = datetime.fromtimestamp(onboard_ts).strftime(f"%Y-%m-%d %H:%M:%S")

                if abs(change) >= 5 and data_time <= onboard_ts + 600000 * 60:
                    # 查询24h排行
                    rank_24h = 0
                    # reverse_direction = ""
                    # if change >= 0:
                    #     direction = "rise"
                    #     reverse_direction = "fall"
                    
                    # else:
                    #     direction = "fall"
                    #     reverse_direction = "rise"
                    
                    trending_top_mean_change = 0
                    # trending_list_24h = self.load_24h_trending_data(to_ts=trending_1h_ts, direction=direction)
                    # if trending_list_24h:
                    #     trending_top_mean_change = pd.DataFrame(trending_list_24h[3:8])["change"].mean()
                    
                    reverse_top_mean_change = 0
                    # reverse_list_24h = self.load_24h_trending_data(to_ts=trending_1h_ts, direction=reverse_direction)
                    # if reverse_list_24h:
                    #     reverse_top_mean_change = pd.DataFrame(reverse_list_24h[3:8])["change"].mean()

                    over_trending = False
                    # if abs(trending_top_mean_change) >= abs(reverse_top_mean_change) * 3:
                    #     over_trending = True

                    if not over_trending:
                        signal_ts = self.signal_tokens_1h.get(symbol, 0)
                        self.signal_tokens_1h[symbol] = data_time

                        if data_time >= signal_ts + 6 * 60 * 60:
                            self.signal_count += 1
                            msg = f"1H趋势启动 {symbol}\nchange：{change}\ntrending_1h_rank：{trending_1h_rank}\ntrending_1h_time：{trending_1h_time}\nrank_24h：{rank_24h}\ntrending_24h_top：{trending_top_mean_change}\nreverse_24h_top：{reverse_top_mean_change}\ncount：{self.signal_count}\n"
                            print(msg)

        # 排序
        self.trending_tokens_1h = dict(sorted(self.trending_tokens_1h.items()))

    def load_1h_trending_data(self, to_ts: float, direction: str):
        result = []
        start_hour_time = (datetime.fromtimestamp(to_ts) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        hour_time = start_hour_time
        while hour_time <= start_hour_time + timedelta(hours=2):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            rank_direction = f"rank_{direction}"
            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}{rank_direction}{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        t = file.split(".")[0].replace("_", ":")
                        dt = f"{date} {t}"
                        file_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
                        if file_ts < to_ts:
                            # file_path = f"{root}{DIR_SYMBOL}{file}"
                            # df = pd.read_csv(file_path)
                            # result = []
                            # for _, row in df.iterrows():
                            #     result.append(dict(row))
                            pass
                        
                        else:
                            file_path = f"{root}{DIR_SYMBOL}{file}"
                            df = pd.read_csv(file_path)
                            result = []
                            for _, row in df.iterrows():
                                result.append(dict(row))

                            return result
                            

            hour_time += timedelta(hours=1)
        return result

    def search_1h_trending_data(self, from_ts: float, direction: str, symbol: str, top: int):
        result = []
        start_hour_time = (datetime.fromtimestamp(from_ts) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        hour_time = start_hour_time
        while hour_time <= datetime.now():
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            rank_direction = f"rank_{direction}"
            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}{rank_direction}{DIR_SYMBOL}1h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        t = file.split(".")[0].replace("_", ":")
                        dt = f"{date} {t}"
                        file_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
                        if file_ts > from_ts and result:
                            rank_1h = 0
                            symbols_1h = []
                            trending_list = result[3:]
                            for data_1h in trending_list:
                                symbols_1h.append(data_1h["symbol"])

                            if symbol in symbols_1h:
                                rank_1h = symbols_1h.index(symbol) + 1

                            if 1 <= rank_1h <= top:
                                ts = result[0]["change"]
                                return ts, rank_1h
                        
                        file_path = f"{root}{DIR_SYMBOL}{file}"
                        df = pd.read_csv(file_path)
                        result = []
                        for _, row in df.iterrows():
                            result.append(dict(row))

            hour_time += timedelta(hours=1)
        return 0, 0
    
    def load_24h_trending_data(self, to_ts: float, direction: str):
        result = []
        start_hour_time = (datetime.fromtimestamp(to_ts) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        hour_time = start_hour_time
        while hour_time <= start_hour_time + timedelta(hours=2):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            rank_direction = f"rank_{direction}"
            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}{rank_direction}{DIR_SYMBOL}24h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        t = file.split(".")[0].replace("_", ":")
                        dt = f"{date} {t}"
                        file_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
                        if file_ts < to_ts:
                            file_path = f"{root}{DIR_SYMBOL}{file}"
                            df = pd.read_csv(file_path)
                            result = []
                            for _, row in df.iterrows():
                                result.append(dict(row))
                        
                        else:
                            # file_path = f"{root}{DIR_SYMBOL}{file}"
                            # df = pd.read_csv(file_path)
                            # result = []
                            # for _, row in df.iterrows():
                            #     result.append(dict(row))

                            return result
                            

            hour_time += timedelta(hours=1)
        return result
    
    def search_24h_trending_data(self, from_ts: float, direction: str, symbol: str, top: int, reverse: bool = False):
        result = []
        start_hour_time = (datetime.fromtimestamp(from_ts) - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        if reverse:
            end_time = datetime.now()
        
        else:
            end_time = start_hour_time + timedelta(hours=25)
            
        hour_time = start_hour_time
        while hour_time <= end_time:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = hour_time.strftime(f"%Y-%m-%d")
            hour = hour_time.hour

            rank_direction = f"rank_{direction}"
            dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}{rank_direction}{DIR_SYMBOL}24h{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            if os.path.exists(dir_path):
                for root, _, files in os.walk(dir_path):
                    for file in files:
                        t = file.split(".")[0].replace("_", ":")
                        dt = f"{date} {t}"
                        file_ts = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").timestamp()
                        if file_ts > from_ts and result:
                            rank_24h = 0
                            symbols_24h = []
                            trending_list = result[3:]
                            for data_1h in trending_list:
                                symbols_24h.append(data_1h["symbol"])

                            if symbol in symbols_24h:
                                rank_24h = symbols_24h.index(symbol) + 1

                            if 1 <= rank_24h <= top and not reverse:
                                ts = result[0]["change"]
                                return ts, rank_24h
                            
                            if rank_24h > top and reverse:
                                ts = result[0]["change"]
                                return ts, rank_24h
                        
                        file_path = f"{root}{DIR_SYMBOL}{file}"
                        df = pd.read_csv(file_path)
                        result = []
                        for _, row in df.iterrows():
                            result.append(dict(row))

            hour_time += timedelta(hours=1)
        return 0, 0

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
                    
                    # 近期趋势数据
                    trending_change_recent = 0
                    trending_recent_index = elements.index("TRENDING_RECENT") if "TRENDING_RECENT" in elements else -1
                    if trending_recent_index >= 0:
                        trending_change_recent = float(elements[trending_recent_index + 1])

                    # 1H趋势数据
                    trending_change_1h = 0
                    trending_ts_1h = 0
                    trending_1h_index = elements.index("TRENDING_1H") if "TRENDING_1H" in elements else -1
                    if trending_1h_index >= 0:
                        value_1 = elements[trending_1h_index + 1]
                        if float(value_1) < 100:
                            trending_change_1h = float(value_1)
                            value_2 = elements[trending_1h_index + 2]
                            if len(value_2):
                                trending_ts_1h = float(value_2)
                        
                        else:
                            trending_ts_1h = float(value_1)
                    trending_change_1h = 0 if not trending_change_1h else trending_change_1h

                    # 24H趋势数据
                    trending_ts_24h = 0
                    trending_24h_index = elements.index("TRENDING_24H") if "TRENDING_24H" in elements else -1
                    if trending_24h_index < 0:
                        trending_24h_index = elements.index("TRENDING") if "TRENDING" in elements else -1
                    if trending_24h_index >= 0:
                        trending_ts_24h = float(elements[trending_24h_index + 1])

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
                            "trending_change_recent": round(trending_change_recent, 2),
                            "trending_change_1h": round(trending_change_1h, 2),
                            "trending_ts_1h": trending_ts_1h,
                            "trending_ts_24h": trending_ts_24h,
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
    symbol_trending_count_data = {}
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
            trending_change_recent = data["trending_change_recent"]

            trending_change_1h = data["trending_change_1h"]
            trending_ts_1h = data["trending_ts_1h"]
            trending_time_1h = datetime.fromtimestamp(trending_ts_1h).strftime(f"%Y-%m-%d %H:%M:%S")

            trending_ts_24h = data["trending_ts_24h"]
            trending_time_24h = datetime.fromtimestamp(trending_ts_24h).strftime(f"%Y-%m-%d %H:%M:%S")

            last_close_date_time = data["last_close_date_time"]
            last_close_ts = datetime.strptime(last_close_date_time, "%Y-%m-%d %H:%M:%S").timestamp() if last_close_date_time else 0

            # if (open_ts - last_close_ts > 4 * 60 * 60) and (open_ts - trending_ts_24h > 2 * 60 * 60):
            #     real_pnl_rate = 0
            
            # 趋势前五开仓限制
            symbol_trending = f"{symbol}_{direction}_{int(trending_ts_24h)}"
            symbol_trending_count = symbol_trending_count_data.get(symbol_trending, 0)
            if symbol_trending_count >= 5:
                real_pnl_rate = 0
            symbol_trending_count += 1
            symbol_trending_count_data[symbol_trending] = symbol_trending_count
            
            # 连续大幅亏损后停止开仓
            over_loss_flag = False
            if data["over_loss_datetime"]:
                over_loss_flag = True
                real_pnl_rate = 0

            # 累计盈亏
            total_pnl += real_pnl_rate
            if real_pnl_rate:
                pnl_count += 1

            msg = f"{trending_time_1h}\t{trending_change_1h}\t{trending_change_recent}\t{trending_time_24h}\t{open_date_time} - {dt}\t{direction}\t{position_minute}m {position_second}s\tcross {cross}\tentry_drawdown {entry_drawdown}\topen {open_count}\tstop {stop_count}\tstop_pnl {stop_rate:.3f}\tpnl {pnl_rate:.3f}\t{real_pnl_rate:.3f}\t{total_pnl:.3f}\t{symbol}"
            if over_loss_flag:
                msg = f"{msg}\t*"
            
            if pnl_rate >= 5.0:
                msg = f"{msg}\n"
                
            print(msg)

    print(f"止损盈亏异常数：{stop_pnl_error_count}")
    print(f"盈亏交易数：{pnl_count}")
    print(f"总计盈亏：{total_pnl}")

if __name__ == "__main__":
    backtesting = Backtesting()
    backtesting.start(BacktestingMode.TRENDING_24H)
    # backtesting.start(BacktestingMode.TRENDING_24H_QUICK)
    # backtesting.start(BacktestingMode.TRENDING_1H)