"""
"""

import logging
from logging import Logger
import smtplib
import os
from abc import ABC
from datetime import datetime
from email.message import EmailMessage
from queue import Empty, Queue
from threading import Thread
from typing import Any, Sequence, Type, Dict, List, Optional

from vnpy.event import Event, EventEngine
from .app import BaseApp
from .event import (
    EVENT_TICK,
    EVENT_ORDER,
    EVENT_TRADE,
    EVENT_POSITION,
    EVENT_ACCOUNT,
    EVENT_CONTRACT,
    EVENT_LOG
)
from .gateway import BaseGateway
from .object import (
    CancelRequest,
    LogData,
    OrderRequest,
    SubscribeRequest,
    HistoryRequest,
    OrderData,
    BarData,
    TickData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    Exchange
)
from .setting import SETTINGS
from .utility import get_folder_path, TRADER_DIR

""" modify by loe """
from pymongo import MongoClient, ASCENDING
from .constant import LOG_DB_NAME
from dingtalkchatbot.chatbot import DingtalkChatbot
from time import sleep
import socket
from concurrent.futures import ThreadPoolExecutor
from copy import copy

class MainEngine:
    """
    Acts as the core of VN Trader.
    """

    def __init__(self, event_engine: EventEngine = None):
        """"""
        # 记录今日日期
        self.todayDate = datetime.now().strftime('%Y%m%d')

        if event_engine:
            self.event_engine: EventEngine = event_engine
        else:
            self.event_engine = EventEngine()
        self.event_engine.start()

        self.gateways: Dict[str, BaseGateway] = {}
        self.engines: Dict[str, BaseEngine] = {}
        self.apps: Dict[str, BaseApp] = {}
        self.exchanges: List[Exchange] = []

        os.chdir(TRADER_DIR)    # Change working directory
        self.init_engines()     # Initialize function engines

        """ modify by loe """
        self.dbClient = None
        self.gateway_setting = {}
        self.thread_executor = ThreadPoolExecutor(max_workers=10)

    def add_engine(self, engine_class: Any) -> "BaseEngine":
        """
        Add function engine.
        """
        engine = engine_class(self, self.event_engine)
        self.engines[engine.engine_name] = engine
        return engine

    def add_gateway(self, gateway_class: Type[BaseGateway]) -> BaseGateway:
        """
        Add gateway.
        """
        gateway = gateway_class(self.event_engine)
        self.gateways[gateway.gateway_name] = gateway

        # Add gateway supported exchanges into engine
        for exchange in gateway.exchanges:
            if exchange not in self.exchanges:
                self.exchanges.append(exchange)

        return gateway

    def add_app(self, app_class: Type[BaseApp]) -> "BaseEngine":
        """
        Add app.
        """
        app = app_class()
        self.apps[app.app_name] = app

        engine = self.add_engine(app.engine_class)
        return engine

    def init_engines(self) -> None:
        """
        Init all engines.
        """
        self.add_engine(LogEngine)
        self.add_engine(OmsEngine)
        self.add_engine(EmailEngine)
        """ modify by loe """
        self.add_engine(DingTalkEngine)

    def write_log(self, msg: str, source: str = "") -> None:
        """
        Put log event with specific message.
        """
        log = LogData(msg=msg, gateway_name=source)
        event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

    def get_gateway(self, gateway_name: str) -> BaseGateway:
        """
        Return gateway object by name.
        """
        gateway = self.gateways.get(gateway_name, None)
        if not gateway:
            self.write_log(f"找不到底层接口：{gateway_name}")
        return gateway

    def get_engine(self, engine_name: str) -> "BaseEngine":
        """
        Return engine object by name.
        """
        engine = self.engines.get(engine_name, None)
        if not engine:
            self.write_log(f"找不到引擎：{engine_name}")
        return engine

    def get_default_setting(self, gateway_name: str) -> Optional[Dict[str, Any]]:
        """
        Get default setting dict of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.get_default_setting()
        return None

    def get_all_gateway_names(self) -> List[str]:
        """
        Get all names of gatewasy added in main engine.
        """
        return list(self.gateways.keys())

    def get_all_apps(self) -> List[BaseApp]:
        """
        Get all app objects.
        """
        return list(self.apps.values())

    def get_all_exchanges(self) -> List[Exchange]:
        """
        Get all exchanges.
        """
        return self.exchanges

    def connect(self, setting: dict, gateway_name: str) -> None:
        """
        Start connection of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        self.gateway_setting[gateway_name] = setting
        if gateway:
            gateway.connect(setting)

    """" modify by loe """
    def reconnect(self, gateway_name:str):
        gateway = self.get_gateway(gateway_name)
        setting = self.gateway_setting.get(gateway_name, None)
        if not setting:
            return False, f'gateway setting 连接参数缺失【{gateway_name}】'
        self.connect(setting, gateway_name)
        sleep(20)
        if gateway.md_api.connect_status and gateway.md_api.login_status and gateway.td_api.connect_status and gateway.td_api.login_status and gateway.td_api.auth_status:
            return True, f'gateway 重新连接成功【{gateway_name}】'
        else:
            return False, f'gateway 重新连接失败【{gateway_name}】'

    def subscribe(self, req: SubscribeRequest, gateway_name: str) -> None:
        """
        Subscribe tick data update of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.subscribe(req)

    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        """
        Send new order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_order(req)
        else:
            return ""

    def cancel_order(self, req: CancelRequest, gateway_name: str) -> None:
        """
        Send cancel order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.cancel_order(req)

    def send_orders(self, reqs: Sequence[OrderRequest], gateway_name: str) -> List[str]:
        """
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_orders(reqs)
        else:
            return ["" for req in reqs]

    def cancel_orders(self, reqs: Sequence[CancelRequest], gateway_name: str) -> None:
        """
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.cancel_orders(reqs)

    def query_history(self, req: HistoryRequest, gateway_name: str) -> Optional[List[BarData]]:
        """
        Send cancel order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.query_history(req)
        else:
            return None

    def close(self) -> None:
        """
        Make sure every gateway and app is closed properly before
        programme exit.
        """
        # Stop event engine first to prevent new timer event.
        self.event_engine.stop()

        for engine in self.engines.values():
            engine.close()

        for gateway in self.gateways.values():
            gateway.close()

    """ modify by loe """
    def dbConnect(self):
        """连接MongoDB数据库"""
        if not self.dbClient:
            try:
                self.dbClient = MongoClient('localhost', 27017, serverSelectionTimeoutMS = 600)

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                self.write_log('MongoDB连接成功')

                # 如果启动日志记录，则注册日志事件监听函数
                self.event_engine.register(EVENT_LOG, self.dbLogging)

            except:
                self.dbClient = None
                self.write_log('MongoDB连接失败！！')

    def dbQuery(self, dbName, collectionName, d, sortKey='', sortDirection=ASCENDING):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            if sortKey:
                cursor = collection.find(d).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
            else:
                cursor = collection.find(d)

            if cursor:
                return list(cursor)
            else:
                return []
        else:
            self.write_log("数据库查询失败：MongoDB")
            return []

    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert_one(d)
        else:
            self.write_log(f"数据库数据保存失败：{dbName}")

    def dbUpdate(self, dbName, collectionName, d, flt, upsert=False, callback=None):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        """ modify by loe """
        # callback会把原始数据d加上True or False的result返回
        try:
            data = {'dbName':dbName,
                    'collectionName':collectionName,
                    'flt':flt,
                    'd':d,
                    'upsert':upsert,
                    'callback':callback}
            self.thread_executor.submit(self.do_dbupdate, data)
        except:
            if callback:
                back_data = copy(d)
                back_data['result'] = False
                callback(back_data)

    def dbLogging(self, event):
        """向MongoDB中插入日志"""
        log = event.data
        d = {
            'content': log.msg,
            'gateway': log.gateway_name,
            'time': log.time
        }
        self.dbInsert(LOG_DB_NAME, self.todayDate, d)

        """ modify by loe """
        if log.gateway_name:
            print(f'{log.time}\t{log.gateway_name}\t{log.msg}')
        else:
            print(f'{log.time}\t{log.msg}')

    """ modify by loe """
    def do_dbupdate(self, data:dict):
        dbName = data.get('dbName', '')
        d = data.get('d', {})
        callback = data.get('callback', None)
        back_data = copy(d)
        try:
            collectionName = data['collectionName']
            flt = data['flt']
            upsert = data['upsert']
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.replace_one(flt, d, upsert)
            if callback:
                back_data['result'] = True
                callback(back_data)
        except:
            if callback:
                back_data['result'] = False
                callback(back_data)

class BaseEngine(ABC):
    """
    Abstract class for implementing an function engine.
    """

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
        engine_name: str,
    ):
        """"""
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.engine_name = engine_name

    def close(self):
        """"""
        pass


class LogEngine(BaseEngine):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(LogEngine, self).__init__(main_engine, event_engine, "log")

        if not SETTINGS["log.active"]:
            return

        self.level: int = SETTINGS["log.level"]

        self.logger: Logger = logging.getLogger("VN Trader")
        self.logger.setLevel(self.level)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        if SETTINGS["log.console"]:
            self.add_console_handler()

        if SETTINGS["log.file"]:
            self.add_file_handler()

        self.register_event()

    def add_null_handler(self) -> None:
        """
        Add null handler for logger.
        """
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self) -> None:
        """
        Add console output of log.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def add_file_handler(self) -> None:
        """
        Add file output of log.
        """
        today_date = datetime.now().strftime("%Y%m%d")
        filename = f"vt_{today_date}.log"
        log_path = get_folder_path("log")
        file_path = log_path.joinpath(filename)

        file_handler = logging.FileHandler(
            file_path, mode="a", encoding="utf8"
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event: Event) -> None:
        """
        Process log event.
        """
        log = event.data
        self.logger.log(log.level, log.msg)


class OmsEngine(BaseEngine):
    """
    Provides order management system function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(OmsEngine, self).__init__(main_engine, event_engine, "oms")

        self.ticks: Dict[str, TickData] = {}
        self.orders: Dict[str, OrderData] = {}
        self.trades: Dict[str, TradeData] = {}
        self.positions: Dict[str, PositionData] = {}
        self.accounts: Dict[str, AccountData] = {}
        self.contracts: Dict[str, ContractData] = {}

        self.active_orders: Dict[str, OrderData] = {}

        self.add_function()
        self.register_event()

    def add_function(self) -> None:
        """Add query function to main engine."""
        self.main_engine.get_tick = self.get_tick
        self.main_engine.get_order = self.get_order
        self.main_engine.get_trade = self.get_trade
        self.main_engine.get_position = self.get_position
        self.main_engine.get_account = self.get_account
        self.main_engine.get_contract = self.get_contract
        self.main_engine.get_all_ticks = self.get_all_ticks
        self.main_engine.get_all_orders = self.get_all_orders
        self.main_engine.get_all_trades = self.get_all_trades
        self.main_engine.get_all_positions = self.get_all_positions
        self.main_engine.get_all_accounts = self.get_all_accounts
        self.main_engine.get_all_contracts = self.get_all_contracts
        self.main_engine.get_all_active_orders = self.get_all_active_orders

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick = event.data
        self.ticks[tick.vt_symbol] = tick

    def process_order_event(self, event: Event) -> None:
        """"""
        order = event.data
        self.orders[order.vt_orderid] = order

        # If order is active, then update data in dict.
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        # Otherwise, pop inactive order from in dict
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

    def process_trade_event(self, event: Event) -> None:
        """"""
        trade = event.data
        self.trades[trade.vt_tradeid] = trade

    def process_position_event(self, event: Event) -> None:
        """"""
        position = event.data
        self.positions[position.vt_positionid] = position

    def process_account_event(self, event: Event) -> None:
        """"""
        account = event.data
        self.accounts[account.vt_accountid] = account

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract = event.data

        """ modify by loe """
        contract = self.check_update_contract(contract)

        self.contracts[contract.vt_symbol] = contract

    """ modify by loe """
    # 针对部分交易所合约tickprice做的修改
    def check_update_contract(self, contract):
        if '1TOKEN' in contract.gateway_name and ('eos' in contract.vt_symbol or 'EOS' in contract.vt_symbol):
            contract.pricetick = 0.01
        return contract

    def get_tick(self, vt_symbol: str) -> Optional[TickData]:
        """
        Get latest market tick data by vt_symbol.
        """
        return self.ticks.get(vt_symbol, None)

    def get_order(self, vt_orderid: str) -> Optional[OrderData]:
        """
        Get latest order data by vt_orderid.
        """
        return self.orders.get(vt_orderid, None)

    def get_trade(self, vt_tradeid: str) -> Optional[TradeData]:
        """
        Get trade data by vt_tradeid.
        """
        return self.trades.get(vt_tradeid, None)

    def get_position(self, vt_positionid: str) -> Optional[PositionData]:
        """
        Get latest position data by vt_positionid.
        """
        return self.positions.get(vt_positionid, None)

    def get_account(self, vt_accountid: str) -> Optional[AccountData]:
        """
        Get latest account data by vt_accountid.
        """
        return self.accounts.get(vt_accountid, None)

    def get_contract(self, vt_symbol: str) -> Optional[ContractData]:
        """
        Get contract data by vt_symbol.
        """
        return self.contracts.get(vt_symbol, None)

    def get_all_ticks(self) -> List[TickData]:
        """
        Get all tick data.
        """
        return list(self.ticks.values())

    def get_all_orders(self) -> List[OrderData]:
        """
        Get all order data.
        """
        return list(self.orders.values())

    def get_all_trades(self) -> List[TradeData]:
        """
        Get all trade data.
        """
        return list(self.trades.values())

    def get_all_positions(self) -> List[PositionData]:
        """
        Get all position data.
        """
        return list(self.positions.values())

    def get_all_accounts(self) -> List[AccountData]:
        """
        Get all account data.
        """
        return list(self.accounts.values())

    def get_all_contracts(self) -> List[ContractData]:
        """
        Get all contract data.
        """
        return list(self.contracts.values())

    def get_all_active_orders(self, vt_symbol: str = "") -> List[OrderData]:
        """
        Get all active orders by vt_symbol.

        If vt_symbol is empty, return all active orders.
        """
        if not vt_symbol:
            return list(self.active_orders.values())
        else:
            active_orders = [
                order
                for order in self.active_orders.values()
                if order.vt_symbol == vt_symbol
            ]
            return active_orders


class EmailEngine(BaseEngine):
    """
    Provides email sending function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(EmailEngine, self).__init__(main_engine, event_engine, "email")

        self.thread: Thread = Thread(target=self.run)
        self.queue: Queue = Queue()
        self.active: bool = False

        self.main_engine.send_email = self.send_email

    def send_email(self, subject: str, content: str, receiver: str = "") -> None:
        """"""
        # Start email engine when sending first email.
        if not self.active:
            self.start()

        # Use default receiver if not specified.
        if not receiver:
            receiver = SETTINGS["email.receiver"]

        msg = EmailMessage()
        msg["From"] = SETTINGS["email.sender"]
        msg["To"] = receiver
        client = socket.gethostname()
        subject = f'{subject}【{client}】'
        msg["Subject"] = subject
        """ modify by loe """
        if content:
            content = f'{datetime.now()}\n\n{content}'
        msg.set_content(content)

        self.queue.put(msg)

        """ modify by loe """
        # 同时发送钉钉消息
        self.main_engine.send_ding_talk(content=f'邮件主题\n============\n{subject}\n\n邮件内容\n============\n{content}')

    def run(self) -> None:
        """"""
        while self.active:
            try:
                msg = self.queue.get(block=True, timeout=1)

                with smtplib.SMTP_SSL(
                    SETTINGS["email.server"], SETTINGS["email.port"]
                ) as smtp:
                    smtp.login(
                        SETTINGS["email.username"], SETTINGS["email.password"]
                    )
                    smtp.send_message(msg)
            except:
                pass
            sleep(2)

    def start(self):
        """"""
        self.active = True
        self.thread.start()

    def close(self):
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()

""" modify by loe """
class DingTalkEngine(BaseEngine):
    # 发送钉钉机器人消息
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(DingTalkEngine, self).__init__(main_engine, event_engine, "ding")

        self.thread = Thread(target=self.run)
        self.queue = Queue()
        self.active = False

        self.register_event()
        self.main_engine.send_ding_talk = self.send_ding_talk

    def register_event(self):
        self.event_engine.register(EVENT_LOG, self.ding_talk)
        self.event_engine.register('eCtaLog', self.ding_talk)
        self.event_engine.register('eSpreadLog', self.ding_talk)

    def ding_talk(self, event):
        """"""
        if not isinstance(event, Event):
            return

        key_word_list = ['断开', '异常', '错误', '出错', '触发', '失败', '状态码', 'Error', 'error', 'ERROR', 'Bad', 'bad', 'BAD', 'Traceback']
        content_dic = event.data.__dict__
        msg = content_dic['msg']
        enable = False
        for key in key_word_list:
            if key in msg:
                enable = True
                break
        if enable:
            content = ''
            for key, value in content_dic.items():
                content += f'{key}：{value}\n'

            self.send_ding_talk(content=content)

    def send_ding_talk(self, content):
        # 内容添加电脑名称、时间
        client = socket.gethostname()
        full_content = f'【{client}】\n\n{datetime.now()}\n\n{content}'

        # 开启线程
        if not self.active:
            self.start()

        # 消息推入队列，做流控处理
        if self.queue.qsize() < 10:
            self.queue.put(full_content)

    def run(self):
        """"""
        while self.active:
            try:
                content = self.queue.get(block=True, timeout=1)

                # 发送消息
                webhook = 'https://oapi.dingtalk.com/robot/send?access_token=c7829ba703a3e0a28fb43f40a65f68313ec3ab43324e5bad30bd2bb660f791e4'
                ding = DingtalkChatbot(webhook)
                ding.send_text(msg=content, is_at_all=True)
            except:
                pass
            sleep(2)

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()
