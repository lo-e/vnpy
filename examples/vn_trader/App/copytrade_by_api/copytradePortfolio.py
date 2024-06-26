# encoding: UTF-8

from datetime import datetime
from copy import copy
from time import time, sleep
from threading import Thread
from vnpy.trader.utility import DIR_SYMBOL
from queue import Queue, Empty
from vnpy.trader.utility import round_to, floor_to, ceil_to, load_json_path
from vnpy.trader.object import SubscribeRequest

class CopytradePortfolio(object):
    """ 跟单交易组合 """
    paramList = ["name"]

    varList = [
        "inited",
        "starting",
        "trader_name_position_dict",
        "trader_name_position_updated_time",
        "trader_position_inited",
        "trader_from_time_dict",
        "trader_pnl_dict",
    ]

    syncList = [
        "trader_from_time_dict"
    ]

    def __init__(self, engine, setting):
        self.cta_engine = engine
        self.name = ""
        self.inited = False
        self.starting = False

        self.symbols = setting.get("symbols", []) # 默认订阅的品种
        self.copy_setting = setting.get("copy_setting", {}) # 带单交易员设置参数

        self.trader_position_dict = {} # 带单交易员带单数据
        self.trader_name_position_dict = {} # 带单交易员带单数据
        self.trader_name_position_updated_time = {} # 带单交易员带单更新时间
        self.trader_position_inited = False # 带单交易员带单数据初始化
        self.trader_from_time_dict = {} # 带单交易员有效带单起始时间
        self.trader_pnl_dict = {} # 带单交易员持仓盈亏

        # 设置参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]

    def on_init(self):
        # 开启新线程获取交易员的当前带单
        for trader_code, setting in self.copy_setting.items():
            start = setting.get("start", False)
            if start:
                type = setting.get("type", "")
                if type == "copy":
                    t = Thread(target=self.fetch_copy_trade_data, args=(trader_code,))
                    t.start()
                
                elif type == "public":
                    t = Thread(target=self.fetch_public_trade_data, args=(trader_code,))
                    t.start()

        # 开启新线程统计当前带单交易员带单盈亏
        t = Thread(target=self.calculate_pnl)
        t.start()

    def on_start(self):
        pass

    def on_timer(self):
        # 检查所有带单交易员带单数据是否成功获取
        inited = True
        for trader in self.copy_setting.keys():
            if trader not in self.trader_position_dict:
                inited = False
        self.trader_position_inited = inited

        # 投资组合事件推送
        self.cta_engine.put_portfolio_event()

    def fetch_copy_trade_data(self, trader):
        setting = self.copy_setting.get(trader, {})
        trader_name = setting.get("trader", "")
        error_notice_time = 0
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            try:
                gateway = self.cta_engine.main_engine.get_default_gateway("OKX")
                if gateway:
                    # 查询交易员排行榜
                    # rank_data = gateway.rest_api.query_copytrader_rank()

                    # 查询交易员当前带单
                    """
                    从小有个百万梦 '540D011FDACCB47A'
                    墙头草 'D5E7A8430A35CA84'
                    Alvinnn111 '9B28742D954561AE'
                    fat_bear '5EAE0133C50F4261'
                    比特智说币 '8B9619BF3BDE40E7'
                    """
                    trader_position_data, message = gateway.rest_api.query_copytrade(trader)
                    if isinstance(trader_position_data, list) and (not message):
                        symbol_pos_dict_copy = {}
                        for d in trader_position_data:
                            # 带单起始时间判断
                            open_time = d["openTime"]
                            open_time = datetime.fromtimestamp(int(open_time) / 1000)
                            valid_from_time = self.trader_from_time_dict.get(trader, None)
                            if valid_from_time and open_time < valid_from_time:
                                continue
                            
                            # 带单合约判断
                            symbol = d["instId"]
                            pure_symbol = symbol.split("-")[0]
                            symbol_list = setting.get("symbol_list", [])
                            if symbol_list and pure_symbol not in symbol_list:
                                continue

                            contract = self.cta_engine.main_engine.get_contract(f"{symbol}.OKX")
                            if contract:
                                # 订单数量
                                subPos = float(d["subPos"])
                                posSide = d["posSide"]
                                pos = abs(subPos)
                                pos = pos * contract.contract_value
                                if (posSide == "short") or (posSide == "net" and subPos < 0):
                                    pos = pos * -1

                                # 订单成交价格
                                price = float(d["openAvgPx"])

                                # 计算仓位均价
                                absolute_pos_data = symbol_pos_dict_copy.get(symbol, {})
                                long_data = absolute_pos_data.get("long", {})
                                long_volume = long_data.get("volume", 0)
                                long_price = long_data.get("price", 0)
                                long_value = abs(long_volume * long_price)

                                short_data = absolute_pos_data.get("short", {})
                                short_volume = short_data.get("volume", 0)
                                short_price = short_data.get("price", 0)
                                short_value = abs(short_volume * short_price)

                                if pos > 0:
                                    long_volume += abs(pos)
                                    long_value += abs(price * pos)
                                    long_price = long_value / abs(long_volume)

                                else:
                                    short_volume += abs(pos)
                                    short_value += abs(price * pos)
                                    short_price = short_value / abs(short_volume)
                                
                                # 精度处理
                                long_volume = round_to(long_volume, contract.min_volume)
                                long_price = round_to(long_price, contract.pricetick)
                                short_volume = round_to(short_volume, contract.min_volume)
                                short_price = round_to(short_price, contract.pricetick)

                                # 统计带单员多空持仓数量、均价
                                pos_data = {}
                                if long_volume:
                                    pos_data["long"] = {"volume":long_volume, "price":long_price}

                                if short_volume:
                                    pos_data["short"] = {"volume":short_volume, "price":short_price}

                                symbol_pos_dict_copy[symbol] = pos_data

                                # print(f"{symbol}\t{posSide}\t{pos}")

                        # 带单交易员带单数据更新
                        if (trader not in self.trader_position_dict) or self.trader_position_dict[trader] != symbol_pos_dict_copy:
                            self.trader_position_dict[trader] = symbol_pos_dict_copy
                            self.trader_name_position_dict[trader_name] = symbol_pos_dict_copy

                            # 策略响应
                            for strategy in self.cta_engine.strategies.values():
                                strategy.on_copy_trader()

                            # 订阅带单合约行情
                            for symbol in symbol_pos_dict_copy.keys():
                                vt_symbol = f"{symbol}.OKX"
                                tick = oms_engine.ticks.get(vt_symbol, None)
                                contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                                if not tick and contract:
                                    # 订阅合约行情
                                    req = SubscribeRequest(
                                        symbol=contract.symbol, exchange=contract.exchange
                                    )
                                    self.cta_engine.main_engine.subscribe(req, contract.gateway_name)

                            # 打印更新内容 
                            print(f"\n--------------------\n{datetime.now()}\n带单员【{trader_name}】带单更新：\n{symbol_pos_dict_copy}\n\n{trader_position_data}\n--------------------\n")
                        self.trader_name_position_updated_time[trader_name] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                        
                        # print(f"{datetime.now()}\t带单员：{trader_name}\t开单数量：{len(trader_position_data)}\t最新持仓：{symbol_pos_dict_copy}\n")
                    
                    else:
                        error_notice_gap = int(time()) - error_notice_time
                        if error_notice_gap >= 60*5:
                            error_notice_time = int(time())
                            msg = f"！获取（{trader_name}）带单数据异常！\n{trader_position_data}\n\n{message}"
                            self.send_ding_talk(msg)

            except Exception as e:
                error_notice_gap = int(time()) - error_notice_time
                if error_notice_gap >= 60*5:
                    error_notice_time = int(time())
                    msg = f"！获取（{trader_name}）带单报错！\n{e}"
                    self.send_ding_talk(msg)

            sleep(0.01)

    def fetch_public_trade_data(self, trader):
        setting = self.copy_setting.get(trader, {})
        trader_name = setting.get("trader", "")
        error_notice_time = 0
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            try:
                gateway = self.cta_engine.main_engine.get_default_gateway("OKX")
                if gateway:
                    # 查询交易员当前带单
                    """
                    bit_lang_lang 563E3A78CDBAFB4E
                    """

                    trader_position_data, message = gateway.rest_api.query_publictrade(trader)
                    if isinstance(trader_position_data, list) and (not message):
                        long_lever = 0
                        short_lever = 0
                        symbol_pos_dict_copy = {}
                        for direction_position_data in trader_position_data:
                            position_data_list = direction_position_data["posData"]
                            for d in position_data_list:
                                # 建仓起始时间判断
                                open_time = d["cTime"]
                                open_time = datetime.fromtimestamp(int(open_time) / 1000)
                                valid_from_time = self.trader_from_time_dict.get(trader, None)
                                if valid_from_time and open_time < valid_from_time:
                                    continue
                                
                                # 带单合约判断
                                symbol = d["instId"]
                                pure_symbol = symbol.split("-")[0]
                                symbol_list = setting.get("symbol_list", [])
                                if symbol_list and pure_symbol not in symbol_list:
                                    continue

                                contract = self.cta_engine.main_engine.get_contract(f"{symbol}.OKX")
                                if contract:
                                    # 仓位价格
                                    price = float(d["avgPx"])

                                    # 仓位大小
                                    copy_assets = setting["copy_assets"]
                                    pos_space = float(d["posSpace"])
                                    pos_value = copy_assets * pos_space
                                    pos = pos_value / price

                                    posSide = d["posSide"]
                                    if posSide == "long":
                                        long_lever += round(pos_space, 2)

                                    else:
                                        short_lever += round(pos_space, 2)
                                        pos = pos * -1

                                    # 计算仓位均价
                                    absolute_pos_data = symbol_pos_dict_copy.get(symbol, {})
                                    long_data = absolute_pos_data.get("long", {})
                                    long_volume = long_data.get("volume", 0)
                                    long_price = long_data.get("price", 0)
                                    long_value = abs(long_volume * long_price)

                                    short_data = absolute_pos_data.get("short", {})
                                    short_volume = short_data.get("volume", 0)
                                    short_price = short_data.get("price", 0)
                                    short_value = abs(short_volume * short_price)

                                    if pos > 0:
                                        long_volume += abs(pos)
                                        long_value += abs(price * pos)
                                        long_price = long_value / abs(long_volume)

                                    else:
                                        short_volume += abs(pos)
                                        short_value += abs(price * pos)
                                        short_price = short_value / abs(short_volume)
                                    
                                    # 精度处理
                                    long_volume = round_to(long_volume, contract.min_volume)
                                    long_price = round_to(long_price, contract.pricetick)
                                    short_volume = round_to(short_volume, contract.min_volume)
                                    short_price = round_to(short_price, contract.pricetick)

                                    # 统计带单员多空持仓数量、均价
                                    pos_data = {}
                                    if long_volume:
                                        pos_data["long"] = {"volume":long_volume, "price":long_price}

                                    if short_volume:
                                        pos_data["short"] = {"volume":short_volume, "price":short_price}

                                    symbol_pos_dict_copy[symbol] = pos_data

                                    # print(f"{symbol}\t{posSide}\t{pos}")

                        # 带单交易员带单数据更新
                        if (trader not in self.trader_position_dict) or self.trader_position_dict[trader] != symbol_pos_dict_copy:
                            msg = ""
                            last_position_data = self.trader_position_dict.get(trader, {})
                            symbol_list = set(last_position_data.keys()).union(symbol_pos_dict_copy.keys())
                            for symbol in symbol_list:
                                pos_data = last_position_data.get(symbol, {})
                                last_long_data = pos_data.get("long", {})
                                last_long_price = last_long_data.get("price", 0)
                                last_short_data = pos_data.get("short", {})
                                last_short_price = last_short_data.get("price", 0)

                                current_pos_data = symbol_pos_dict_copy.get(symbol, {})
                                current_long_data = current_pos_data.get("long", {})
                                current_long_price = current_long_data.get("price", 0)
                                current_short_data = current_pos_data.get("short", {})
                                current_short_price = current_short_data.get("price", 0)

                                if last_long_price != current_long_price:
                                    msg += f"\n{symbol}\nlong {last_long_price} - {current_long_price}"
                                
                                if last_short_price != current_short_price:
                                    msg += f"\n{symbol}\nshort {last_short_price} - {current_short_price}"
                                
                            if msg:
                                msg = f"\nlong_lever {long_lever}\nshort_lever {short_lever}\n{msg}"
                                self.send_ding_talk(msg)

                                # 策略响应
                                for strategy in self.cta_engine.strategies.values():
                                    strategy.on_copy_trader()

                            self.trader_position_dict[trader] = symbol_pos_dict_copy
                            self.trader_name_position_dict[trader_name] = symbol_pos_dict_copy

                            # 订阅带单合约行情
                            for symbol in symbol_pos_dict_copy.keys():
                                vt_symbol = f"{symbol}.OKX"
                                tick = oms_engine.ticks.get(vt_symbol, None)
                                contract = self.cta_engine.main_engine.get_contract(vt_symbol)
                                if not tick and contract:
                                    # 订阅合约行情
                                    req = SubscribeRequest(
                                        symbol=contract.symbol, exchange=contract.exchange
                                    )
                                    self.cta_engine.main_engine.subscribe(req, contract.gateway_name)
                        
                            # 打印更新内容 
                            print(f"\n--------------------\n{datetime.now()}\n交易员【{trader_name}】交易更新：\n{symbol_pos_dict_copy}\n\n{trader_position_data}\n--------------------\n")
                        self.trader_name_position_updated_time[trader_name] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                        
                        # print(f"{datetime.now()}\t带单员：{trader_name}\t开单数量：{len(trader_position_data)}\t最新持仓：{symbol_pos_dict_copy}\n")
                    
                    else:
                        error_notice_gap = int(time()) - error_notice_time
                        if error_notice_gap >= 60*5:
                            error_notice_time = int(time())
                            msg = f"！获取（{trader_name}）交易数据异常！\n{trader_position_data}\n\n{message}"
                            self.send_ding_talk(msg)

            except Exception as e:
                error_notice_gap = int(time()) - error_notice_time
                if error_notice_gap >= 60*5:
                    error_notice_time = int(time())
                    msg = f"！获取（{trader_name}）交易数据报错！\n{e}"
                    self.send_ding_talk(msg)

            sleep(0.01)

    def calculate_pnl(self):
        oms_engine = self.cta_engine.main_engine.engines["oms"]
        while True:
            try:
                # 计算带单员带单盈亏
                trader_pnl_dict = {}
                for trader, symbol_pos_dict in self.trader_position_dict.items():
                    trader_pnl = 0
                    for symbol, pos_data in symbol_pos_dict.items():
                        pure_symbol = symbol.split("-")[0]
                        tick = oms_engine.ticks.get(f"{symbol}.OKX", None)
                        if tick:
                            long_data = pos_data.get("long", {})
                            long_volume = abs(long_data.get("volume", 0))
                            long_price = long_data.get("price", 0)
                            if long_volume and long_price and tick.last_price:
                                long_pnl = long_volume * (tick.last_price - long_price)
                                # 墙头草跟单山寨币仓位加倍，盈亏加倍计算
                                if (trader == "D5E7A8430A35CA84") and (pure_symbol not in ["BTC", "ETH", "XRP"]):
                                    long_pnl *= 2
                                trader_pnl += long_pnl

                            short_data = pos_data.get("short", {})
                            short_volume = abs(short_data.get("volume", 0))
                            short_price = short_data.get("price", 0)
                            if short_volume and short_price and tick.last_price:
                                short_pnl = short_volume * (short_price - tick.last_price)
                                # 墙头草跟单山寨币仓位加倍，盈亏加倍计算
                                if (trader == "D5E7A8430A35CA84") and (pure_symbol not in ["BTC", "ETH", "XRP"]):
                                    short_pnl *= 2
                                trader_pnl += short_pnl
                    trader_pnl_dict[trader] = trader_pnl
                
                for trader, trader_pnl in trader_pnl_dict.items():
                    trader_setting = self.copy_setting.get(trader, {})
                    trader_name = trader_setting.get("trader", "")
                    copy_assets = trader_setting.get("copy_assets", 0)
                    stop_loss = trader_setting.get("stop_loss", -1)

                    trader_pnl = round(trader_pnl, 2)
                    if copy_assets:
                        trader_pnl_rate = trader_pnl / copy_assets
                        if trader_pnl_rate <= stop_loss:
                            # 设置带单交易员有效带单起始时间（相当于止损平仓）
                            self.trader_from_time_dict[trader] = datetime.now().replace(microsecond=0)

                            # 发送通知提醒
                            msg = f"\n{trader_name} 当前亏损：{trader_pnl_rate}\n最大亏损限制：{stop_loss}\n已强制清仓"
                            self.send_ding_talk(msg)

                        trader_pnl_rate = f"{round(trader_pnl_rate * 100, 2)}%"
                        self.trader_pnl_dict[trader_name] = [trader_pnl, trader_pnl_rate]
                
            except Exception as e:
                pass
            sleep(1)

    def send_ding_talk(self, content):
        # 推送钉钉消息
        content = f"{self.name}\n{content}"
        self.cta_engine.main_engine.send_ding_talk(content)