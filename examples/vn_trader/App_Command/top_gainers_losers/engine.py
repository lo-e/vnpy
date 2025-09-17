import importlib
from operator import sub
import os
import traceback
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue, Empty
from copy import copy
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    OrderRequest,
    SubscribeRequest,
    SubscribeLotsRequest,
    HistoryRequest,
    LogData,
    TickData,
    BarData,
    ContractData,
    TradeData
)
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_TIMER,
)
from vnpy.trader.constant import (
    Direction,
    OrderType,
    Interval,
    Exchange,
    Offset,
    Status,
)
from vnpy.trader.utility import (
    load_json,
    load_json_path,
    save_json,
    extract_vt_symbol,
    round_to,
)
from vnpy.trader.utility import DIR_SYMBOL

from .base import APP_NAME
from vnpy.app.cta_strategy.base import (
    EVENT_CTA_LOG,
    EngineType,
    StopOrder,
    StopOrderStatus,
    POSITION_DB_NAME,
    PORTFOLIO_DB_NAME,
)
from vnpy.app.cta_strategy.template import CtaTemplate
from vnpy.trader.converter import OffsetConverter
import re
from collections import OrderedDict
from time import sleep
from decimal import Decimal
import json
from .topGainersLosersStrategy import TopGainersLosersStrategy
from .topGainersLosersPortfolio import TopGainersLosersPortfolio
from vnpy.app.cta_strategy.base import (
    TICK_DB_NAME,
    DAILY_DB_NAME,
    MinuteDataBaseName,
)
from gateway.binance import BinanceUsdtGateway

class TopGainersLosersEngine(BaseEngine):
    engine_type = EngineType.LIVE

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        super(TopGainersLosersEngine, self).__init__(main_engine, event_engine, APP_NAME)
        self.strategies = {}
        self.strategy_sync_data = {}
        self.strategy_variable_data = {}
        self.symbol_strategy_map = defaultdict(list)
        self.orderid_strategy_map = {}
        self.strategy_orderid_map = defaultdict(set)
        self.init_thread = None
        self.init_queue = Queue()
        self.vt_tradeids = set()
        self.offset_converter = OffsetConverter(self.main_engine)
        self.portfolio: TopGainersLosersPortfolio = None
        self.gateway_delay = False
        self.setting_update_queue = Queue()

    def init_engine(self):
        # 获取setting
        dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
        file_path = dir_path.joinpath("setting.json")
        setting = load_json_path(file_path)

        # 导入投资组合
        portfolio_setting = setting.get("portfolio", None)
        self.portfolio = TopGainersLosersPortfolio(self, portfolio_setting)
        self.load_portfolio_syncData()
        
        # 导入策略
        signal_list = setting.get("signal", [])
        for signal_setting in signal_list:
            self.add_strategy(signal_setting)

        self.register_event()
        self.write_log(f"趋势涨跌策略引擎初始化成功\t策略数：{len(self.strategies)}")

    def close(self):
        self.stop_all_strategies()

    def register_event(self):
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event: Event):
        # 投资组合推送
        if self.portfolio.inited:
            self.portfolio.on_timer()

        # 策略推送
        for strategy in self.strategies.copy().values():
            if strategy.inited:
                self.call_strategy_func(strategy, strategy.on_timer)

        # 获取交易所连接延迟
        gateway: BinanceUsdtGateway = self.main_engine.get_default_gateway("BINANCE")
        if gateway:
            if gateway.rest_api.time_offset >= 3000:
                self.gateway_delay = True
            
            else:
                self.gateway_delay = False

        # 处理setting.json更新
        try:
            type, data = self.setting_update_queue.get(block=True, timeout=1)
            if type == "new":
                self.new_strategy_setting(data)

            elif type == "remove":
                self.remove_strategy_setting(data)

        except Empty:
            pass

        except Exception as e:
            pass

    def process_tick_event(self, event: Event):
        tick = event.data

        if self.portfolio.started:
            self.portfolio.tick_queue.put(tick)
        
        # strategies = self.symbol_strategy_map[tick.vt_symbol]
        # if not strategies:
        #     return
        
        # for strategy in strategies:
        #     if strategy.inited:
        #         self.call_strategy_func(strategy, strategy.on_tick, tick)

    def process_order_event(self, event: Event):
        order = event.data
        self.offset_converter.update_order(order)
        strategy = self.orderid_strategy_map.get(order.vt_orderid, None)
        if not strategy:
            return

        # 清除非活跃状态的订单
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if order.vt_orderid in vt_orderids and not order.is_active():
            vt_orderids.remove(order.vt_orderid)

        # 策略响应
        self.call_strategy_func(strategy, strategy.on_order, order)

    def process_trade_event(self, event: Event):
        trade: TradeData = event.data
        if trade.vt_tradeid in self.vt_tradeids:
            return
        
        self.vt_tradeids.add(trade.vt_tradeid)
        self.offset_converter.update_trade(trade)
        strategy = self.orderid_strategy_map.get(trade.vt_orderid, None)

        # 止损单触发非本地订单，特殊处理
        if not strategy:
            for strategy_ in self.strategies.values():
                if strategy_.vt_symbol == trade.vt_symbol:
                    strategy = strategy_
                    break

        if not strategy:
            return
        
        contract = self.main_engine.get_contract(trade.vt_symbol)
        if not contract:
            return

        # 策略持仓统计
        if trade.direction == Direction.LONG:
            strategy.pos = float(Decimal(str(strategy.pos)) + Decimal(str(trade.volume)))

        else:
            strategy.pos = float(Decimal(str(strategy.pos)) - Decimal(str(trade.volume)))
        strategy.pos = round_to(strategy.pos, contract.min_volume)

        # 策略响应
        self.call_strategy_func(strategy, strategy.on_trade, trade)

    def process_position_event(self, event: Event):
        position = event.data
        self.offset_converter.update_position(position)

    def send_server_order(
        self,
        strategy: CtaTemplate,
        contract: ContractData,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        type: OrderType,
        lock: bool,
        stop_loss_price: float = 0
    ):
        # 创建订单
        original_req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            offset=offset,
            type=type,
            price=price,
            volume=volume,
            stop_loss_price=stop_loss_price
        )
        req_list = self.offset_converter.convert_order_request(original_req, lock)

        # 发送订单
        vt_orderids = []
        for req in req_list:
            vt_orderid = self.main_engine.send_account_order(req, contract.gateway_name, strategy.exchange_user)
            vt_orderids.append(vt_orderid)
            self.offset_converter.update_order_request(req, vt_orderid)

            # 保存orderid、strategy映射关系
            self.orderid_strategy_map[vt_orderid] = strategy
            self.strategy_orderid_map[strategy.strategy_name].add(vt_orderid)

        return vt_orderids

    def cancel_server_order(self, strategy: CtaTemplate, vt_orderid: str):
        # 取消订单
        order = self.main_engine.get_order(vt_orderid)
        if not order:
            self.write_log(f"撤单失败，找不到委托{vt_orderid}", strategy)
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_account_order(req, order.gateway_name, strategy.exchange_user)

    def send_order(
        self,
        strategy: CtaTemplate,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool,
        lock: bool,
        market: bool = False,
        stop_loss_price: float = 0
    ):
        contract = self.main_engine.get_contract(strategy.vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{strategy.vt_symbol}", strategy)
            return ""

        # 精度处理
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        # 发送订单
        type = OrderType.LIMIT
        
        if market:
            type = OrderType.MARKET

        if stop:
            type = OrderType.STOP

        return self.send_server_order(
            strategy, contract, direction, offset, price, volume, type, lock, stop_loss_price
        )

    def send_symbol_order(
        self,
        strategy: CtaTemplate,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        stop: bool,
        lock: bool,
        market: bool = False,
        stop_loss_price: float = 0
    ):
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到合约：{vt_symbol}", strategy)
            return ""

        # 精度处理
        price = round_to(price, contract.pricetick)
        volume = round_to(volume, contract.min_volume)

        # 发送订单
        type = OrderType.LIMIT
        if market:
            type = OrderType.MARKET

        return self.send_server_order(
            strategy, contract, direction, offset, price, volume, type, lock, stop_loss_price
        )

    def cancel_order(self, strategy: CtaTemplate, vt_orderid: str):
        # 取消策略的指定订单
        self.cancel_server_order(strategy, vt_orderid)

    def cancel_all(self, strategy: CtaTemplate):
        # 取消策略的所有订单
        vt_orderids = self.strategy_orderid_map[strategy.strategy_name]
        if not vt_orderids:
            return

        for vt_orderid in copy(vt_orderids):
            self.cancel_order(strategy, vt_orderid)

    def get_engine_type(self):
        # 引擎类型（实盘或者回测）
        return self.engine_type

    def call_strategy_func(
        self, strategy: CtaTemplate, func: Callable, params: Any = None
    ):
        # 响应策略方法
        try:
            if params:
                func(params)

            else:
                func()

        except Exception:
            strategy.trading = False
            strategy.inited = False

            msg = f"触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg, strategy)

    def init_strategy(self, strategy_name: str):
        # 启动独立线程初始化策略
        self.init_queue.put(strategy_name)
        if not self.init_thread:
            self.init_thread = Thread(target=self._init_strategy)
            self.init_thread.start()

    def _init_strategy(self):
        # 初始化策略
        while not self.init_queue.empty():
            strategy_name = self.init_queue.get()
            self.initing_strategy(strategy_name)

        self.init_thread = None

    def initing_strategy(self, strategy_name: str):
        strategy = self.strategies[strategy_name]
        if strategy.inited:
            self.write_log(f"{strategy_name}已经完成初始化，禁止重复操作")
            return

        # 响应策略初始化方法
        self.call_strategy_func(strategy, strategy.on_init)

        # 策略状态更新（初始化完成）
        strategy.inited = True

    def start_strategy(self, strategy_name: str):
        # 启动策略
        strategy = self.strategies[strategy_name]
        if not strategy.inited:
            self.write_log(f"策略{strategy.strategy_name}启动失败，请先初始化")
            return

        if strategy.trading:
            self.write_log(f"{strategy_name}已经启动，请勿重复操作")
            return

        # 响应策略启动方法
        self.call_strategy_func(strategy, strategy.on_start)

        # 策略状态更新（已启动）
        strategy.trading = True

    def subscribe(self, vt_symbols: list):
        # 订阅合约行情
        exchange_symbols_data = {}
        exchange_gateway_data = {}
        for vt_symbol in vt_symbols:
            contract: ContractData = self.main_engine.get_contract(vt_symbol)
            if contract:
                exchange_symbols = exchange_symbols_data.get(contract.exchange, set())
                exchange_symbols.add(contract.symbol)
                exchange_symbols_data[contract.exchange] = exchange_symbols

                exchange_gateway_data[contract.exchange] = contract.gateway_name

            else:
                self.write_log(f"行情订阅失败，找不到合约{vt_symbol}")

        for exchange, exchange_symbols in exchange_symbols_data.items():
            req = SubscribeLotsRequest(symbols=list(exchange_symbols), exchange=exchange)
            gateway_name = exchange_gateway_data[exchange]
            self.main_engine.subscribe_lots(req, gateway_name)

    def unsubscribe(self, vt_symbols: list):
        # 取消订阅合约行情
        exchange_symbols_data = {}
        exchange_gateway_data = {}
        for vt_symbol in vt_symbols:
            contract: ContractData = self.main_engine.get_contract(vt_symbol)
            if contract:
                exchange_symbols = exchange_symbols_data.get(contract.exchange, set())
                exchange_symbols.add(contract.symbol)
                exchange_symbols_data[contract.exchange] = exchange_symbols

                exchange_gateway_data[contract.exchange] = contract.gateway_name

        for exchange, exchange_symbols in exchange_symbols_data.items():
            req = SubscribeLotsRequest(symbols=list(exchange_symbols), exchange=exchange)
            gateway_name = exchange_gateway_data[exchange]
            self.main_engine.unsubscribe_lots(req, gateway_name)

    def stop_strategy(self, strategy_name: str):
        # 停止策略
        strategy = self.strategies[strategy_name]
        if not strategy.trading:
            return

        # 响应策略停止方法
        self.call_strategy_func(strategy, strategy.on_stop)

        # 策略状态更新（已停止）
        strategy.trading = False

        # 取消策略的所有订单
        self.cancel_all(strategy)

    def init_all_strategies(self):
        # 初始化所有策略
        for strategy_name in self.strategies.keys():
            # self.init_strategy(strategy_name)
            self.initing_strategy(strategy_name)
        
        # 订阅合约
        vt_symbols = set()
        for strategy_name in self.strategies.keys():
            strategy: TopGainersLosersStrategy = self.strategies[strategy_name]
            vt_symbols.add(strategy.vt_symbol)
        
        if vt_symbols:
            self.subscribe(list(vt_symbols))

    def start_all_strategies(self):
        # 启动所有策略
        for strategy_name in self.strategies.keys():
            self.start_strategy(strategy_name)

    def stop_all_strategies(self):
        # 停止所有策略
        for strategy_name in self.strategies.keys():
            self.stop_strategy(strategy_name)

    def put_strategy_event(self, strategy: CtaTemplate):
        # 保存策略同步数据到数据库
        self.save_sync_data(strategy)

    def put_portfolio_event(self):
        # 保存投资组合同步数据到数据库
        self.save_portfolio_sync_data()

    def load_portfolio_syncData(self):
        # 从数据库导入投资组合历史同步数据
        syncData = self.main_engine.dbQuery(
            PORTFOLIO_DB_NAME, self.portfolio.name, {}
        )
        if not syncData:
            return

        d = syncData[0]
        for key in self.portfolio.syncs:
            if key in d:
                self.portfolio.__setattr__(key, d[key])
    
    def save_portfolio_sync_data(self):
        # 保存策略组合同步数据到数据库
        if not self.portfolio:
            return

        data = {}
        for key in self.portfolio.syncs:
            data[key] = self.portfolio.__getattribute__(key)
        
        if data:
            self.main_engine.dbUpdate(
                PORTFOLIO_DB_NAME,
                self.portfolio.name,
                data,
                {},
                True,
                callback=self.portfolio_db_update_callback,
            )

    def portfolio_db_update_callback(self, back_data=None):
        # 保存策略组合同步数据到数据库结果回调
        try:
            if isinstance(back_data, dict):
                result = back_data.get("result", False)
                if result:
                    content = f"{self.portfolio.name}同步数据保存成功"

                else:
                    content = f"{self.portfolio.name}同步数据保存失败"
                    self.write_log(content)

            else:
                content = f"{self.portfolio.name}同步数据保存失败"
                self.write_log(content)

        except:
            content = f"{self.portfolio.name}同步数据保存失败"
            self.write_log(content)

    def write_log(self, msg: str, strategy: CtaTemplate = None):
        # 日志记录
        if strategy:
            msg = f"{strategy.strategy_name}: {msg}"
        log = LogData(msg=msg, gateway_name="CtaStrategy")
        event = Event(type=EVENT_CTA_LOG, data=log)
        self.event_engine.put(event)

        # 输出日志内容
        print(f"{log.time}\t{log.gateway_name}\t{log.msg}")

    def send_email(self, msg: str, strategy: CtaTemplate = None, subject: str = ""):
        # 发送邮件提醒
        if not subject:
            if strategy:
                subject = f"{strategy.strategy_name}"

            else:
                subject = "CTA策略引擎"

        self.main_engine.send_email(subject, msg)

    def send_dingtalk(self, msg: str, strategy: CtaTemplate = None):
        # 发送钉钉提醒
        if strategy:
            msg = f"{strategy.strategy_name}\n{msg}"

        self.main_engine.send_ding_talk(content=msg)

    def load_bar(self, vt_symbol, data_from, interval, window, callback):
        # 数据库获取数据
        if interval == Interval.DAILY:
            dbName = DAILY_DB_NAME

        elif interval == Interval.MINUTE:
            dbName = MinuteDataBaseName(window)

        else:
            dbName = TICK_DB_NAME

        d = {"datetime": {"$gte": data_from}}
        collectionName = vt_symbol.upper()
        barData = self.main_engine.dbQuery(dbName, collectionName, d, "datetime")
        result = []
        for d in barData:
            bar = BarData(
                gateway_name="",
                symbol="",
                exchange=Exchange.NONE,
                datetime=None,
                endDatetime=None,
            )
            bar.__dict__ = d

            # 检查Bar数据是否有效
            if not bar.check_valid():
                raise ("Bar数据校验不通过！！")

            result.append(bar)
        
        # 回调并返回结果
        if callback:
            callback(result)
        return result

    def add_strategy(self, setting, load_sync: bool = True):
        # 策略参数
        try:
            name = setting["strategy_name"]
            start = setting["start"]

        except Exception:
            msg = traceback.format_exc()
            self.write_log(f"载入策略出错：{msg}")
            return

        if not start:
            return

        # 防止策略重名
        if name in self.strategies:
            self.write_log(f"策略实例重名：{name}")
            return

        # 创建策略实例
        strategy = TopGainersLosersStrategy(self, setting)

        # 加载同步数据
        if load_sync:
            self.load_sync_data(strategy)

        # 添加策略
        self.strategies[name] = strategy
        strategies = self.symbol_strategy_map[strategy.vt_symbol]
        strategies.append(strategy)

    def new_strategy(self, setting):
        try:
            # 执行策略
            start = setting["start"]
            strategy_name = setting["strategy_name"]
            if start and strategy_name not in self.strategies:
                self.add_strategy(setting, load_sync=False)
                self.initing_strategy(strategy_name)
                self.start_strategy(strategy_name)

        except Exception as e:
            msg = f"趋势涨跌策略上新出错\n\n{setting}\n\n{e}"
            self.send_dingtalk(msg)

    def new_strategy_setting(self, strategy_settings: list):
        try:
            # 获取setting文件
            dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
            file_path = dir_path.joinpath("setting.json")
            setting = load_json_path(file_path)
            signal_list = setting.get("signal", [])
            signal_data = {}
            for signal_setting in signal_list:
                signal_data[signal_setting["strategy_name"]] = signal_setting

            updated = False
            for strategy_setting in strategy_settings:
                if strategy_setting["strategy_name"] not in signal_data:
                    signal_list.append(strategy_setting)
                    updated = True
                
            # 保存文件
            if updated:
                save_json(file_path, setting)

        except Exception as e:
            return False, str(e)
        
        return True, ""
        
    def remove_strategy(self, strategy_name: str):
        strategy = self.strategies.get(strategy_name, None)
        if not strategy:
            return
        
        try:
            # 停止策略
            self.stop_strategy(strategy_name)

            # 清除合约策略映射
            symbol_strategies = self.symbol_strategy_map[strategy.vt_symbol]
            if strategy in symbol_strategies:
                symbol_strategies.remove(strategy)

            # 清除订单策略映射
            for k, v in self.orderid_strategy_map.copy().items():
                if v == strategy:
                    self.orderid_strategy_map.pop(k)

            # 清除策略订单映射
            self.strategy_orderid_map[strategy.strategy_name] = set()

            # 清除历史策略
            self.strategies.pop(strategy_name)
        
        except Exception as e:
            msg = f"趋势涨跌策略移除出错\n\n{strategy_name}\n\n{e}"
            self.send_dingtalk(msg)
    
    def remove_strategy_setting(self, strategy_names: list):
        try:
            # 获取setting文件
            dir_path = Path(os.path.dirname(os.path.realpath(__file__)))
            file_path = dir_path.joinpath("setting.json")
            setting = load_json_path(file_path)
            signal_list = setting.get("signal", [])
            signal_data = {}
            for signal_setting in signal_list:
                signal_data[signal_setting["strategy_name"]] = signal_setting
            
            # 搜寻
            found = False
            for name in strategy_names:
                if name in signal_data:
                    signal_list.remove(signal_data[name])
                    found = True
            
            # 保存文件
            if found:
                save_json(file_path, setting)

        except Exception as e:
            return False, str(e)
    
        return True, ""

    def load_sync_data(self, strategy):
        # 从数据库载入策略历史同步数据
        flt = {"strategy_name": strategy.strategy_name, "vt_symbol": strategy.vt_symbol}
        colleciton_name = f"{strategy.__class__.__name__}"
        syncData = self.main_engine.dbQuery(
            POSITION_DB_NAME, colleciton_name, flt
        )
        if not syncData:
            return

        data = syncData[0]
        for key in strategy.syncs:
            if key in data:
                strategy.__setattr__(key, data[key])

        # 记录历史同步数据
        if "_id" in data:
            data.pop("_id")
        self.strategy_sync_data[strategy.strategy_name] = data

    def save_sync_data(self, strategy):
        if not strategy.inited:
            return
        
        # 保存策略同步数据到文件（数据有变化时才保存）
        flt = {"strategy_name": strategy.strategy_name, "vt_symbol": strategy.vt_symbol}
        sync_data = copy(flt)
        for key in strategy.syncs:
            sync_data[key] = strategy.__getattribute__(key)

        # history_sync_data = self.strategy_sync_data.get(strategy.strategy_name, {})
        # if history_sync_data != sync_data:
        #     # 记录历史同步数据
        #     self.strategy_sync_data[strategy.strategy_name] = sync_data

        try:
            # 保存到数据库
            colleciton_name = f"{strategy.__class__.__name__}"
            self.main_engine.dbUpdate(
                POSITION_DB_NAME,
                colleciton_name,
                sync_data,
                flt,
                True,
                callback=self.strategy_db_Update_callback,
            )
        
        except Exception as e:
            pass
                
        """
        # 保存策略变量数据到文件（数据有变化时才保存）
        variable_data = {}
        for key in strategy.variables:
            variable_data[key] = strategy.__getattribute__(key)

        history_variable_data = self.strategy_variable_data.get(strategy.strategy_name, {})
        if history_variable_data != variable_data:
            # 记录历史变量数据
            self.strategy_variable_data[strategy.strategy_name] = variable_data

            try:
                # 保存到文件
                variable_json_file = self.get_strategie_variable_file_path(strategy)
                with open(variable_json_file, "w", encoding="utf-8") as file:
                    file.write(
                        json.dumps(variable_data, ensure_ascii=False)
                    )

            except Exception as e:
                pass
        """
    
    def get_strategie_sync_file_path(self, strategy):
        # 策略同步数据保存文件路径 
        dir = os.getcwd()
        dir_path = Path(dir).joinpath(f"BaiduSyncdisk{DIR_SYMBOL}syncs{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path.joinpath(f"{strategy.strategy_name}_syncs.json")
        return file_path

    def get_strategie_variable_file_path(self, strategy):
        # 策略变量数据保存文件路径 
        dir = os.getcwd()
        dir_path = Path(dir).joinpath(f"BaiduSyncdisk{DIR_SYMBOL}variables{DIR_SYMBOL}")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_path = dir_path.joinpath(f"{strategy.strategy_name}_variables.json")
        return file_path
    
    def strategy_db_Update_callback(self, back_data=None):
        # 保存策略同步数据到数据库结果回调
        try:
            if isinstance(back_data, dict):
                result = back_data.get("result", False)
                strategy_name = back_data.get("strategy_name", "")
                if result:
                    content = f"{strategy_name}同步数据保存成功"

                else:
                    content = f"{strategy_name}同步数据保存失败！！"
                    self.write_log(content)

            else:
                content = f"策略同步数据保存失败！！"
                self.write_log(content)

        except:
            content = f"策略同步数据保存失败！！"
            self.write_log(content)

    def init_portfolio(self):
        # 初始化所有策略
        self.init_all_strategies()

        # 初始化策略组合
        if not self.portfolio.inited:
            self.portfolio.on_init()
            self.portfolio.inited = True

    def start_portfolio(self):
        # 启动所有策略
        self.start_all_strategies()

        # 启动策略组合
        if not self.portfolio.started:
            self.portfolio.on_start()
            self.portfolio.started = True

    def stopPortfolio(self):
        # 停止所有策略
        self.stop_all_strategies()

        # 停止策略组合
        if self.portfolio.started:
            self.portfolio.on_stop()
            self.portfolio.started = False

    def get_portfolio_variables(self):
        # 获取策略组合变量数据
        variables_data = OrderedDict()
        for key in self.portfolio.variables:
            variables_data[key] = self.portfolio.__getattribute__(key)

        return variables_data

    def get_portfolio_parameters(self):
        # 获取策略组合参数数据
        parameters_data = OrderedDict()
        for key in self.portfolio.parameters:
            parameters_data[key] = self.portfolio.__getattribute__(key)

        return parameters_data
    
    def get_strategy_parameters(self, strategy_name):
        # 获取策略参数数据
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            parameters_data = OrderedDict()
            for key in strategy.parameters:
                parameters_data[key] = strategy.__getattribute__(key)

            return parameters_data
        
        else:
            self.write_log(f"策略实例不存在：{strategy_name}")
            return None

    def get_strategy_variables(self, strategy_name):
        # 获取策略变量数据
        if strategy_name in self.strategies:
            strategy = self.strategies[strategy_name]
            variables_data = OrderedDict()
            for key in strategy.variables:
                variables_data[key] = strategy.__getattribute__(key)

            return variables_data
        
        else:
            self.write_log(f"策略实例不存在：{strategy_name}")
            return None

    def get_strategy_names(self):
        # 查询策略名称列表
        return self.strategies.keys()

    def get_price_tick(self, strategy):
        # 获取策略合约的最小价格变动
        contract = self.main_engine.getContract(strategy.vt_symbol)
        if contract:
            return contract.priceTick
        return 0