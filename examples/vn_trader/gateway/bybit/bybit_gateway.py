import csv
import hashlib
import hmac
import json
from collections import defaultdict
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from time import time
from typing import Any, Callable, Dict, List
from urllib.parse import urlencode
from pytz import timezone, utc

from peewee import chunked
from requests import ConnectionError
from ..rest import Request, RestClient
from ..websocket import WebsocketClient
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway, LocalOrderManager
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    SubscribeLotsRequest,
    TickData,
    TradeData,
)
from vnpy.trader.utility import (
    extract_vt_symbol,
    get_folder_path,
    load_json,
    save_json,
)

STATUS_BYBIT2VT = {
    "Created": Status.NOTTRADED,
    "New": Status.NOTTRADED,
    "PartiallyFilled": Status.PARTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "PartiallyFilledCanceled": Status.CANCELLED,
    "Rejected": Status.REJECTED,
}

DIRECTION_VT2BYBIT = {Direction.LONG: "Buy", Direction.SHORT: "Sell"}
DIRECTION_BYBIT2VT = {v: k for k, v in DIRECTION_VT2BYBIT.items()}

OPPOSITE_DIRECTION = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}

ORDER_TYPE_VT2BYBIT = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
    OrderType.FAK: "Market",
    OrderType.FOK: "Market",
}

TIMEINFORCE_MAP = {
    OrderType.LIMIT:"GTC",
    OrderType.MARKET:"GTC",
    OrderType.FAK:"IOC",
    OrderType.FOK:"FOK",

}

ORDER_TYPE_BYBIT2VT = {v: k for k, v in TIMEINFORCE_MAP.items()}

TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

INTERVAL_VT2BYBIT = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60",
    Interval.DAILY: "D",
    Interval.WEEKLY: "W",
}

# UTC+8
CHINA_TZ: timezone = timezone("Asia/Shanghai")

CATEGORY_EXCHANGE_MAP: Dict[str, Exchange] = {"spot": Exchange.BYBITSPOT, "inverse": Exchange.BYBIT, "linear": Exchange.BYBIT, "option": Exchange.BYBIT}

REST_HOST = "https://api.bybit.com"  # 主host  https://api.bybit.com备用host https://api.bytick.com
SPOT_PUBLIC_WS_HOST = "wss://stream.bybit.com/v5/public/spot"  # 现货主网公共topic地址
USDT_PUBLIC_WS_HOST = "wss://stream.bybit.com/v5/public/linear"  # usdt,usdc合约主网公共topic地址
INVERSE_PUBLIC_WS_HOST = "wss://stream.bybit.com/v5/public/inverse"  # 反向合约主网公共topic地址
OPTION_PUBLIC_WS_HOST = "wss://stream.bybit.com/v5/public/option"  # 期权主网公共topic地址
PRIVATE_WS_HOST = "wss://stream.bybit.com/v5/private"  # 主网私有topic地址

TESTNET_REST_HOST = "https://api-testnet.bybit.com"
TESTNET_SPOT_PUBLIC_WS_HOST = "wss://stream-testnet.bybit.com/v5/public/spot"  # 现货主网公共topic地址
TESTNET_USDT_PUBLIC_WS_HOST = "wss://stream-testnet.bybit.com/v5/public/linear"  # usdt,usdc合约主网公共topic地址
TESTNET_INVERSE_PUBLIC_WS_HOST = "wss://stream-testnet.bybit.com/v5/public/inverse"  # 反向合约主网公共topic地址
TESTNET_OPTION_PUBLIC_WS_HOST = "wss://stream-testnet.bybit.com/v5/public/option"  # 期权主网公共topic地址
TESTNET_PRIVATE_WS_HOST = "wss://stream-testnet.bybit.com/v5/private"  # 主网私有topic地址

class BybitGateway(BaseGateway):
    """
    BYBIT统一账户接口
    """
    
    gateway_name = "BYBIT"
    default_setting: Dict[str, str] = {
        "账户名称":"",
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": ""
    }
    exchanges = [Exchange.BYBIT]
    
    def __init__(self, event_engine):
        super().__init__(event_engine)
        self.orders: Dict[str, OrderData] = {}
        self.rest_api = BybitRestApi(self)

        self.ws_trade_api = BybitWebsocketTradeApi(self)
        self.ws_usdt_data_api = BybitWebsocketDataApi(self)
        # self.ws_spot_data_api = BybitWebsocketDataApi(self)
        # self.ws_inverse_data_api = BybitWebsocketDataApi(self)
        # self.ws_option_data_api = BybitWebsocketDataApi(self)

        self.websocket_apis = [self.ws_usdt_data_api]
        # self.websocket_apis = [self.ws_spot_data_api, self.ws_usdt_data_api, self.ws_inverse_data_api, self.ws_option_data_api, self.ws_trade_api]

        self.account_positon_update_wait: int = 0

    def connect(self, log_account: dict):
        key = log_account["key"]
        secret = log_account["secret"]
        server = log_account["服务器"]
        proxy_host = log_account["代理地址"]
        proxy_port = log_account["代理端口"]
        
        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        if server == "REAL":
            self.ws_usdt_data_api.connect(USDT_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_spot_data_api.connect(SPOT_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_inverse_data_api.connect(INVERSE_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_option_data_api.connect(OPTION_PUBLIC_WS_HOST, proxy_host, proxy_port)

        else:
            self.ws_usdt_data_api.connect(TESTNET_USDT_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_spot_data_api.connect(TESTNET_SPOT_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_inverse_data_api.connect(TESTNET_INVERSE_PUBLIC_WS_HOST, proxy_host, proxy_port)
            # self.ws_option_data_api.connect(TESTNET_OPTION_PUBLIC_WS_HOST, proxy_host, proxy_port)

        self.ws_trade_api.connect(key, secret, server, proxy_host, proxy_port)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest):
        category = self.rest_api.get_category(req.vt_symbol)
        if category == "linear":
            self.ws_usdt_data_api.subscribe(req)

    def subscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 订阅行情 """
        vt_symbol = f"{req.symbols[0]}.{req.exchange.value}"
        category = self.rest_api.get_category(vt_symbol)
        if category == "linear":
            self.ws_usdt_data_api.subscribe_lots(req)

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """ 取消订阅 """
        category = self.rest_api.get_category(req.vt_symbol)
        if category == "linear":
            self.ws_usdt_data_api.unsubscribe(req)

    def unsubscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 取消订阅 """
        vt_symbol = f"{req.symbols[0]}.{req.exchange.value}"
        category = self.rest_api.get_category(vt_symbol)
        if category == "linear":
            self.ws_usdt_data_api.unsubscribe_lots(req)
    
    def send_order(self, req: OrderRequest):
        return self.rest_api.send_order(req)
    
    def cancel_order(self, req: CancelRequest):
        self.rest_api.cancel_order(req)

    def query_contract(self) -> None:
        """
        查询合约列表
        """
        self.rest_api.query_contract()
   
    def query_account(self) -> None:
        """
        查询账户信息
        """
        self.rest_api.query_account()

    def query_position(self) -> None:
        """
        查询持仓
        """
        self.rest_api.query_position()

    def process_timer_event(self, event):
        """
        处理定时任务
        """
        for api in self.websocket_apis:
            api.send_packet({"op": "ping"})

        self.account_positon_update_wait += 1
        if self.account_positon_update_wait >= 30:
            self.account_positon_update_wait = 0
            self.query_account()
            self.query_position()
    
    def on_order(self, order: OrderData) -> None:
        """
        收到委托单推送，BaseGateway推送数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    
    def get_order(self, orderid: str) -> OrderData:
        """
        用orderid获取委托单数据
        """
        return self.orders.get(orderid, None)
    
    def close(self):
        self.rest_api.stop()
        for api in self.websocket_apis:
            api.stop()

    def check_connected(self) -> Dict[str, Any]:
        """检查连接状态"""
        connected = True
        msg = ""

        if not self.rest_api.contract_info_ready:
            connected = False
            msg += "Rest API 合约信息未就绪"

        if not self.ws_usdt_data_api.connected:
            connected = False
            if msg:
                msg += "\n"
            msg += "Websocket API LINEAR行情连接断开"

        # if not self.ws_spot_data_api.connected:
        #     connected = False
        #     if msg:
        #         msg += "\n"
        #     msg += "Websocket API SOPT行情连接断开"

        # if not self.ws_inverse_data_api.connected:
        #     connected = False
        #     if msg:
        #         msg += "\n"
        #     msg += "Websocket API INVERSE行情连接断开"

        # if not self.ws_option_data_api.connected:
        #     connected = False
        #     if msg:
        #         msg += "\n"
        #     msg += "Websocket API OPTION行情连接断开"
        
        if not self.ws_trade_api.connected:
            connected = False
            if msg:
                msg += "\n"
            msg += "Websocket API 交易连接断开"

        res = {"gateway":self.gateway_name, "connected":connected, "msg":msg}
        return res
    
    def set_leverage(self, vt_symbol: str, target: int):
        self.rest_api.set_leverage(vt_symbol, target)
    
class BybitRestApi(RestClient):
    """
    ByBit REST API
    """
    
    def __init__(self, gateway: BybitGateway):
        super().__init__()
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.key = ""
        self.secret = b""

        # 确保生成的orderid不发生冲突
        self.order_count: int = 0
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.accounts: dict = {}
        self.positions: dict= {}
        self.contract_info_ready = False
    
    def get_server_time(self):
        """
        获取服务器时间
        """
        self.add_request(
            "GET",
            "/v3/public/time",
            callback=self.on_server_time,
        )
    
    def on_server_time(self, data: dict, request: Request):
        """
        收到服务器时间回报
        """
        server_time = generate_datetime(int(data["result"]["timeSecond"]) * 1000)
        local_time = datetime.now()
        self.gateway.write_log(f"服务器时间：{server_time}，本地时间：{local_time}")
    
    def sign(self, request: Request):
        """
        Generate ByBit signature.
        """
        if request.method == "GET":
            api_params = urlencode(request.params) if request.params else ""
        else:
            api_params = json.dumps(request.data if request.data else {})
            request.data = api_params

        #recv_window = str(30 * 1000)
        #nonce = str(generate_timestamp(-20))
        recv_window = str(5 * 1000)
        nonce = str(generate_timestamp(0))

        param_str = nonce + self.key + recv_window + api_params
        signature = hmac.new(self.secret, param_str.encode("utf-8"), hashlib.sha256).hexdigest()
        if not request.headers:
            request.headers = {"Content-Type": "application/json"}

        request.headers.update({"X-BAPI-API-KEY": self.key, "X-BAPI-SIGN": signature, "X-BAPI-TIMESTAMP": nonce, "X-BAPI-RECV-WINDOW": recv_window})
        return request
    
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret.encode()
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
            
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.gateway.write_log(f"REST API 启动成功")

        # 获取合约列表、订单、持仓、账户
        self.query_contract()
        self.query_active_order()
        self.query_position()
        self.query_account()
    
    def get_category(self, vt_symbol: str):
        """
        通过vt_symbol获取产品类型
        """
        symbol, exchange = extract_vt_symbol(vt_symbol)
        if exchange == Exchange.BYBITSPOT:
            return "spot"
        
        # SOLUSDT-11APR25 U本位交割合约，BTCUSDZ25 币本位交割合约
        if symbol.endswith(("USDT", "PERP")) or ("-" in symbol and symbol[-2:].isdigit()):
            return "linear"
        
        elif symbol.endswith(("-C", "-P")):
            return "option"
        
        else:
            return "inverse"
    
    def set_leverage(self, vt_symbol: str, target: int):
        """
        设置合约杠杆
        """
        symbol = extract_vt_symbol(vt_symbol)[0]
        category = self.get_category(vt_symbol)

        # 现货无法设置杠杆
        if category == "spot":
            return
        
        data = {"category": category,
                "symbol": symbol,
                "buyLeverage": str(target),
                "sellLeverage": str(target)}
        path = "/v5/position/set-leverage"
        self.add_request("POST",
                         path,
                         self.on_leverage,
                         data=data,
                         extra={"vt_symbol": vt_symbol})
    
    def on_leverage(self, data: dict, request: Request):
        """
        * 收到设置杠杆回调
        * reMsg:110043杠杆没有修改,0杠杆修改成功
        """
        pass
    
    def switch_isolated(self, vt_symbol: str):
        """
        设置全仓保证金模式，并设置标的杠杆
        不支持统一账户设置
        """
        symbol = extract_vt_symbol(vt_symbol)[0]
        category = self.get_category(vt_symbol)
        # 只支持统一账户反向合约，普通账户正反向合约
        if category not in ["linear", "inverse"]:
            return
        path = "/v5/position/switch-isolated"
        data = {"category": category, "symbol": symbol, "tradeMode": 0, "buyLeverage": "20", "sellLeverage": "20"}
        self.add_request("POST", path, self.on_isolated, data=data, extra={"vt_symbol": vt_symbol})
    
    def on_isolated(self, data: dict, request: Request):
        """
        收到保证金模式回调
        """
        pass
    
    def switch_mode(self, vt_symbol: str):
        """
        设置单向持仓模式
        """
        symbol = extract_vt_symbol(vt_symbol)[0]
        category = self.get_category(vt_symbol)
        # 只支持正向永续和反向永续设置持仓模式
        if category not in ["linear", "inverse"]:
            return
        path = "/v5/position/switch-mode"
        data = {"category": category, "symbol": symbol, "mode": 0}
        self.add_request("POST", path, self.on_mode, data=data, extra={"vt_symbol": vt_symbol})
    
    def on_mode(self, data: dict, request: Request):
        """
        收到持仓模式回调
        """
        pass
    
    def _new_order_id(self) -> int:
        """
        生成本地委托号
        """
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count
    
    def send_order(self, req: OrderRequest):
        """
        发送委托单
        """
        category = self.get_category(req.vt_symbol)
        orderid = f"CUSTOM-{req.symbol}-{str(self.connect_time + self._new_order_id())}"
        position_side = 0
        if (req.direction == Direction.LONG and req.offset == Offset.OPEN) or (req.direction == Direction.SHORT and req.offset != Offset.OPEN):
            position_side = 1

        elif (req.direction == Direction.SHORT and req.offset == Offset.OPEN) or (req.direction == Direction.LONG and req.offset != Offset.OPEN):
            position_side = 2
        data = {
            "category": category,
            "symbol": req.symbol,
            "price": str(req.price),
            "qty": str(req.volume),
            "side": DIRECTION_VT2BYBIT[req.direction],
            "positionIdx": position_side,
            "orderType": ORDER_TYPE_VT2BYBIT[req.type],
            "orderLinkId": orderid,
            "timeInForce": TIMEINFORCE_MAP[req.type]
        }
        order = req.create_order_data(orderid, self.gateway_name)
        order.datetime = datetime.now()

        self.add_request(
            "POST",
            "/v5/order/create",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )
        self.gateway.on_order(order)
        return order.vt_orderid
    
    def on_send_order_failed(self, status_code, request: Request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)
        data = request.response.json()
        if not data:
            return
        error_msg = data["retMsg"]
        error_code = data["retCode"]
        msg = f"发送委托失败，错误代码:{error_code},  错误信息：{error_msg}，委托单数据：{order}"
        self.gateway.write_log(msg)
    
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tracebacks, request: Request):
        """
        Callback when sending order caused exception.
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tracebacks, request)
    
    def on_send_order(self, data: dict, request: Request):
        if self.check_error("发送委托", data):
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)
            return
    
    def cancel_order(self, req: CancelRequest):
        order: OrderData = self.gateway.get_order(req.orderid)

        data = {"category": self.get_category(req.vt_symbol),
                "symbol": req.symbol}
        order_id = req.orderid
        if "CUSTOM" in order_id:
            data["orderLinkId"] = order_id
        
        else:
            data["orderId"] = order_id
        self.add_request("POST", path="/v5/order/cancel", data=data, callback=self.on_cancel_order, on_failed=self.on_cancel_failed, extra=order)
    
    def on_cancel_order(self, data: dict, request: Request):
        if self.check_error("取消委托", data):
            error_code = data["retCode"]
            # 重复撤销委托单被拒推送
            if error_code == 110001:
                order: OrderData = request.extra
                order.status = Status.REJECTED
                self.gateway.on_order(order)
            return
        
    def on_cancel_failed(self, status_code, request: Request) -> None:
        """
        收到取消委托单失败回报
        """
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"撤单失败，状态码：{status_code}，错误信息：{request.response.text}"
        self.gateway.write_log(msg)
    
    def query_contract(self):
        # params = ["linear", "inverse", "spot", "option"]
        params = ["linear"]
        for param in params:
            self.add_request("GET", "/v5/market/instruments-info", self.on_query_contract, params={"limit": 1000, "status": "Trading", "category": param})
    
    def check_error(self, name: str, data: dict):
        if data["retCode"]:
            error_code = data["retCode"]
            error_msg = data["retMsg"]
            msg = f"{name}失败，错误代码：{error_code}，信息：{error_msg}"
            self.gateway.write_log(msg)
            return True

        return False
    
    def on_query_contract(self, data: dict, request: Request):
        """
        查询合约
        """
        if self.check_error("查询合约", data):
            return
        
        category = data["result"]["category"]
        if category == "option":
            product = Product.OPTION

        elif category == "spot":
            product = Product.SPOT

        else:
            product = Product.FUTURES

        for contract_data in data["result"]["list"]:
            delivery_time = int(contract_data.get("deliveryTime", 0))  # 交割时间
            if delivery_time:
                delivery_datetime = generate_datetime(delivery_time)

                # 过滤过期合约
                if delivery_datetime <= datetime.now():
                    continue

                # 交割合约使用交割日期作为name
                name = str(delivery_datetime.date())

            else:
                name = contract_data["symbol"]

            contract = ContractData(
                symbol=contract_data["symbol"],
                exchange=CATEGORY_EXCHANGE_MAP[category],
                name=name,
                product=product,
                size=1,  # 合约杠杆
                pricetick=float(contract_data["priceFilter"].get("minPrice", contract_data["priceFilter"]["tickSize"])),
                min_volume=float(contract_data["lotSizeFilter"]["minOrderQty"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)
        
        self.contract_info_ready = True
        self.gateway.write_log(f"{category.upper()}合约信息查询成功")
    
    def query_account(self):
        """
        发送查询资金请求
        """
        self.add_request(method="GET", path="/v5/account/wallet-balance", callback=self.on_query_account, params={"accountType": "UNIFIED"})
    
    def on_query_account(self, data: dict, request: Request):
        """
        收到资金回报
        """
        if data["retCode"] == 10016:
            return
        
        if not data["result"]:
            return
        
        accountids = set()
        data = data["result"]["list"][0]
        for account_data in data["coin"]:
            accountid = account_data["coin"]
            accountids.add(accountid)
            unrealized_pnl = float(account_data["unrealisedPnl"])
            frozen = abs(unrealized_pnl) if unrealized_pnl < 0 else 0
            account = AccountData(
                accountid=accountid,
                balance=get_float_value(account_data["walletBalance"]),
                frozen=frozen,
                gateway_name=self.gateway_name,
                exchange_user=self.gateway.account_name,
            )
            
            if account.balance:
                self.accounts[accountid] = account
                self.gateway.on_account(account)

            elif accountid in self.accounts:
                account = self.accounts[accountid]
                account.balance = 0
                account.frozen = 0
                self.gateway.on_account(account)

        for accountid, account in self.accounts.items():
            if accountid not in accountids:
                account.balance = 0
                account.frozen = 0
                self.gateway.on_account(account)
    
    def query_position(self):
        """
        发送查询持仓请求
        """
        # category: linear, inverse, option
        params = {"category": "linear",
                  "settleCoin": "USDT",
                  "limit": 200}
        path = "/v5/position/list"
        self.add_request(method="GET", path=path, callback=self.on_query_position, params=params)
    
    def on_query_position(self, data: dict, request: Request):
        """
        收到持仓回报
        """
        if self.check_error("查询持仓", data):
            if data["retCode"] == 10002:
                self.gateway.write_log(f"查询持仓：服务器时间与本地时间不同步")
            return
        
        total_direction_symbols = set()
        category = data["result"]["category"]
        exchange = CATEGORY_EXCHANGE_MAP[category]
        raw_data = data["result"]["list"]
        for pos_data in raw_data:
            symbol = pos_data["symbol"]
            direction = DIRECTION_BYBIT2VT.get(pos_data["side"], None)
            direction_symbol = f"{symbol}_{direction.value}"
            if direction:
                total_direction_symbols.add(direction_symbol)
                position = PositionData(
                    symbol=symbol,
                    exchange=exchange,
                    exchange_user=self.gateway.account_name,
                    direction=direction,
                    volume=abs(float(pos_data["size"])),
                    price=float(pos_data["avgPrice"]),
                    pnl=float(pos_data["unrealisedPnl"]),
                    gateway_name=self.gateway_name,
                )

                self.positions[direction_symbol] = position
                self.gateway.on_position(position)

        for direction_symbol, position in self.positions.items():
            if direction_symbol not in total_direction_symbols:
                position.volume = 0
                position.price = 0
                position.pnl = 0
                self.gateway.on_position(position)
    
    def query_active_order(self):
        """
        查询活动委托单
        """
        # category: spot, linear, inverse, option
        params = {
            "category": "linear",
            "settleCoin": "USDT",
            "limit": 50,
        }
        path = "/v5/order/realtime"
        self.add_request("GET", path, callback=self.on_query_active_order, params=params)
    
    def on_query_active_order(self, data: dict, request: Request):
        """
        收到活动委托单回报
        """
        if self.check_error("查询活动委托单", data):
            return
        
        result = data["result"]["list"]
        if not result:
            return
        
        category = data["result"]["category"]
        exchange = CATEGORY_EXCHANGE_MAP[category]
        for order_data in result:
            orderId = order_data["orderLinkId"]
            if not orderId:
                orderId = order_data["orderId"]

            order = OrderData(
                symbol=order_data["symbol"],
                exchange=exchange,
                orderid=orderId,
                type=ORDER_TYPE_BYBIT2VT[order_data["timeInForce"]],
                direction=DIRECTION_BYBIT2VT[order_data["side"]],
                price=float(order_data["price"]),
                volume=float(order_data["qty"]),
                traded=float(order_data["cumExecQty"]),
                status=STATUS_BYBIT2VT[order_data["orderStatus"]],
                datetime=generate_datetime(int(order_data["createdTime"])),
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
class BybitWebsocketDataApi(WebsocketClient):
    def __init__(self, gateway: BybitGateway):
        super().__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        # 产品类型
        self.category: str = ""

        self.callbacks: Dict[str, Callable] = {}
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.order_book_bids = defaultdict(dict)  # 订单簿买单字典
        self.order_book_asks = defaultdict(dict)  # 订单簿卖单字典

    def connect(self, url: str, proxy_host: str, proxy_port: int):
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.category = url.split("/")[-1].upper()

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()
    
    def subscribe(self, req: SubscribeRequest):
        """
        订阅tick行情
        """
        self.subscribed[req.vt_symbol] = req

        if req.symbol not in self.ticks:
            tick = TickData(symbol=req.symbol, exchange=req.exchange, datetime=datetime.now(), name=req.symbol, gateway_name=self.gateway_name)
            self.ticks[req.symbol] = tick

        # 订阅tick_100ms数据
        self.subscribe_topic(f"tickers.{req.symbol}", self.on_tick)
        
        # 订阅成交数据
        # self.subscribe_topic(f"publicTrade.{req.symbol}", self.on_public_trade)
        
        # 订阅200挡深度数据
        # self.subscribe_topic(f"orderbook.200.{req.symbol}", self.on_depth)

    def subscribe_lots(self, req: SubscribeLotsRequest):
        """
        订阅tick行情
        """
        topics = []

        for symbol in req.symbols:
            vt_symbol = f"{symbol}.{req.exchange.value}"
            self.subscribed[vt_symbol] = SubscribeRequest(symbol=symbol, exchange=req.exchange)

            if symbol not in self.ticks:
                tick = TickData(symbol=symbol, exchange=req.exchange, datetime=datetime.now(), name=symbol, gateway_name=self.gateway_name)
                self.ticks[symbol] = tick

            # 订阅tick_100ms数据
            topics.append(f"tickers.{symbol}")

        # 发送订阅请求
        self.subscribe_topics(topics, self.on_tick)

    def unsubscribe(self, req: SubscribeRequest):
        """
        取消订阅tick行情
        """
        if req.vt_symbol in self.subscribed:
            self.subscribed.pop(req.vt_symbol)

        # 取消订阅tick_100ms数据
        self.unsubscribe_topic(f"tickers.{req.symbol}")

    def unsubscribe_lots(self, req: SubscribeLotsRequest):
        """
        取消订阅tick行情
        """
        topics = []

        for symbol in req.symbols:
            vt_symbol = f"{symbol}.{req.exchange.value}"
            if vt_symbol in self.subscribed:
                self.subscribed.pop(vt_symbol)

            # 取消订阅tick_100ms数据
            topics.append(f"tickers.{symbol}")

        # 发送订阅请求
        self.unsubscribe_topics(topics)
    
    def subscribe_topic(self, topic: str, callback: Callable[[str, dict], Any]):
        """
        订阅私有主题
        """
        self.callbacks[topic] = callback

        req = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def subscribe_topics(self, topics: list, callback: Callable[[str, dict], Any]):
        """
        订阅私有主题
        """
        for topic in topics:
            self.callbacks[topic] = callback

        req = {
            "op": "subscribe",
            "args": topics,
        }
        self.send_packet(req)
    
    def unsubscribe_topic(self, topic: str):
        """
        取消订阅私有主题
        """
        req = {
            "op": "unsubscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def unsubscribe_topics(self, topics: list):
        """
        取消订阅私有主题
        """
        req = {
            "op": "unsubscribe",
            "args": topics,
        }
        self.send_packet(req)

    def on_connected(self):
        self.gateway.write_log(f"Websocket API {self.category} 行情连接成功")
        self.connected = True

        # 重新订阅
        exchange_symbols_data = {}
        for vt_symbol in self.subscribed.keys():
            req: SubscribeRequest = self.subscribed[vt_symbol]
            exchange_symbols = exchange_symbols_data.get(req.exchange, set())
            exchange_symbols.add(req.symbol)
            exchange_symbols_data[req.exchange] = exchange_symbols
        
        for exchange, exchange_symbols in exchange_symbols_data.items():
            req: SubscribeLotsRequest = SubscribeLotsRequest(symbols=list(exchange_symbols), exchange=exchange)
            self.subscribe_lots(req)
    
    def on_disconnected(self):
        self.gateway.write_log(f"Websocket API {self.category} 行情连接断开")
        self.connected = False
    
    def on_packet(self, packet: dict):
        # 过滤心跳回报
        if packet.get("op", None) in ["ping", "pong"]:
            return
        
        if "topic" in packet:
            channel = packet["topic"]
            callback = self.callbacks[channel]
            callback(packet)

        else:
            if not packet["success"]:
                ret_msg = packet["ret_msg"]

                # 过滤重复订阅错误回报
                if "already subscribed" in ret_msg:
                    return
                
                self.gateway.write_log(f"Websocket API 出错：{ret_msg}")
    
    def on_tick(self, packet: dict):
        """
        收到tick行情回报
        """
        topic = packet["topic"]
        type_ = packet["type"]
        data = packet["data"]
        timestamp = packet["ts"]
        symbol = topic.replace("tickers.", "")
        tick = self.ticks[symbol]

        # 收到快照数据推送(订阅tick数据后只推送一次)
        if type_ == "snapshot":
            tick.high_price = float(data["highPrice24h"])
            tick.low_price = float(data["lowPrice24h"])
            tick.pre_close = float(data["prevPrice24h"])

        if "openInterest" in data:
            tick.open_interest = float(data["openInterest"])

        if "lastPrice" in data:
            tick.last_price = float(data["lastPrice"])

        if "volume24h" in data:
            tick.volume = float(data["volume24h"])

        if "turnover24h" in data:
            tick.turnover = float(data["turnover24h"])

        # snapshot和delta都推送的数据
        if "bid1Price" in data:
            tick.bid_price_1 = float(data["bid1Price"])
            tick.bid_volume_1 = float(data["bid1Size"])

        if "ask1Price" in data:
            tick.ask_price_1 = float(data["ask1Price"])
            tick.ask_volume_1 = float(data["ask1Size"])

        tick.datetime = generate_datetime(int(timestamp))
        self.gateway.on_tick(copy(tick))
    
    def on_depth(self, packet: dict):
        data = packet["data"]
        symbol = data["s"]
        tick = self.ticks[symbol]
        tick.datetime = generate_datetime(int(packet["ts"]))

        # 判断是否为全量数据推送，是则清空order book
        if packet["type"] == "snapshot":
            self.order_book_bids[symbol].clear()
            self.order_book_asks[symbol].clear()

        # 辅助函数：更新order book
        def update_order_book(order_book, data):
            for price, amount in data:
                if float(amount) == 0:
                    order_book.pop(price, None)  # 委托量为0则删除
                else:
                    order_book[price] = amount  # 更新或添加

        update_order_book(self.order_book_bids[symbol], data["b"])
        update_order_book(self.order_book_asks[symbol], data["a"])

        # 辅助函数：设置tick的价格和量
        def set_tick_attributes(tick, sorted_data, prefix):
            for index, (price, volume) in enumerate(sorted_data, start=1):
                setattr(tick, f"{prefix}_price_{index}", float(price))
                setattr(tick, f"{prefix}_volume_{index}", float(volume))

        # 排序并更新tick
        # 买单价格从高到低排序
        sort_bids = sorted(self.order_book_bids[symbol].items(), key=lambda x: float(x[0]), reverse=True)[:5]
        # 卖单价格从低到高排序
        sort_asks = sorted(self.order_book_asks[symbol].items(), key=lambda x: float(x[0]))[:5]
        set_tick_attributes(tick, sort_bids, "bid")
        set_tick_attributes(tick, sort_asks, "ask")
        # 如果有最新价格，触发tick更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def on_public_trade(self, packet: dict):
        """
        收到逐笔成交回报
        """
        data = packet["data"]
        for trade_data in data:
            symbol = trade_data["s"]
            tick = self.ticks[symbol]
            tick.last_price = float(trade_data["p"])
            tick.datetime = generate_datetime(trade_data["T"])
            self.gateway.on_tick(copy(tick))
class BybitWebsocketTradeApi(WebsocketClient):
    def __init__(self, gateway: BybitGateway):
        super().__init__()
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = b""
        self.callbacks: Dict[str, Callable] = {}
    
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ):
        
        self.key = key
        self.secret = secret.encode()
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        if self.server == "REAL":
            url = PRIVATE_WS_HOST
        else:
            url = TESTNET_PRIVATE_WS_HOST

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()
    
    def login(self):
        expires = generate_timestamp(20)
        msg = f"GET/realtime{int(expires)}"
        signature = sign(self.secret, msg.encode())

        req = {"op": "auth", "args": [self.key, expires, signature]}
        self.send_packet(req)
    
    def on_login(self):
        """
        收到登录回报
        """
        self.gateway.write_log(f"Websocket API 交易登录成功")
        self.subscribe_topic("order", self.on_order)
        #self.subscribe_topic("execution", self.on_trade)       # 全品种成交推送
        self.subscribe_topic("execution.fast", self.on_trade)       # 不支持期权
        self.subscribe_topic("position", self.on_position)
        self.subscribe_topic("wallet", self.on_account)
    
    def subscribe_topic(self, topic: str, callback: Callable[[str, dict], Any]):
        """
        Subscribe to all private topics.
        """
        self.callbacks[topic] = callback

        req = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)
    
    def on_packet(self, packet: dict):
        
        # 过滤pong回报
        if packet.get("op", None) == "pong":
            return

        if "topic" not in packet:
            # 签名成功后调用on_login
            if packet["success"] and packet["op"] == "auth":
                self.on_login()
        else:
            channel = packet["topic"]
            callback = self.callbacks[channel]
            callback(packet)
    
    def on_connected(self):
        self.gateway.write_log(f"Websocket API 交易连接成功")
        self.connected = True
        self.login()
    
    def on_disconnected(self):
        self.gateway.write_log(f"Websocket API 交易连接断开")
        self.connected = True
    
    def on_trade(self, packet):
        for trade_data in packet["data"]:
            category = trade_data["category"]
            exchange = CATEGORY_EXCHANGE_MAP[category]
            orderId = trade_data.get("orderLinkId", None)
            if not orderId:
                orderId = trade_data["orderId"]
            trade_datetime = generate_datetime(int(trade_data["execTime"]))
            trade = TradeData(
                symbol=trade_data["symbol"],
                exchange=exchange,
                orderid=orderId,
                tradeid=trade_data["execId"],
                direction=DIRECTION_BYBIT2VT[trade_data["side"]],
                price=float(trade_data["execPrice"]),
                volume=float(trade_data["execQty"]),
                datetime=trade_datetime,
                gateway_name=self.gateway_name,
            )
            order: OrderData = self.gateway.orders.get(orderId, None)
            if order:
                trade.offset = order.offset
            self.gateway.on_trade(trade)
    
    def on_order(self, packet):
        for order_data in packet["data"]:
            category = order_data["category"]
            exchange = CATEGORY_EXCHANGE_MAP[category]
            orderId = order_data["orderLinkId"]
            if not orderId:
                orderId = order_data["orderId"]
            order = OrderData(
                symbol=order_data["symbol"],
                exchange=exchange,
                orderid=orderId,
                type=ORDER_TYPE_BYBIT2VT[order_data["timeInForce"]],
                direction=DIRECTION_BYBIT2VT[order_data["side"]],
                price=float(order_data["price"]),
                volume=float(order_data["qty"]),
                traded=float(order_data["cumExecQty"]),
                status=STATUS_BYBIT2VT[order_data["orderStatus"]],
                datetime=generate_datetime(int(order_data["createdTime"])),
                gateway_name=self.gateway_name,
            )
            if order_data["reduceOnly"]:
                order.offset = Offset.CLOSE
            self.gateway.on_order(order)
    
    def on_position(self, packet):
        """
        收到持仓回报
        """
        need_query = False
        for pos_data in packet["data"]:
            category = pos_data["category"]
            exchange = CATEGORY_EXCHANGE_MAP[category]
            direction = DIRECTION_BYBIT2VT.get(pos_data["side"], None)
            if direction:
                pos = PositionData(
                    symbol=pos_data["symbol"],
                    exchange=exchange,
                    exchange_user=self.gateway.account_name,
                    direction=direction,
                    volume=abs(float(pos_data["size"])),
                    price=float(pos_data["entryPrice"]),
                    pnl=float(pos_data["unrealisedPnl"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_position(pos)
            
            else:
                # 可能有某个方向持仓清仓，需要查询确认
                need_query = True

        if need_query:
            self.gateway.query_position()

    def on_account(self, packet):
        """
        收到账户回报
        """
        data = packet["data"]
        if not data:
            return
        
        for account_data in data[0]["coin"]:
            unrealized_pnl = float(account_data["unrealisedPnl"])
            frozen = abs(unrealized_pnl) if unrealized_pnl < 0 else 0
            account = AccountData(
                accountid=account_data["coin"],
                balance=get_float_value(account_data["walletBalance"]),
                frozen=frozen,
                gateway_name=self.gateway_name,
                exchange_user=self.gateway.account_name,
            )
            
            if account.balance:
                self.gateway.on_account(account)

def get_float_value(value: str) -> float:
    """
    将字符串转换为浮点数，处理空值
    """
    return float(value) if value else 0

def generate_timestamp(expire_after: float = 30) -> int:
    """
    生成一个带有过期时间的时间戳，常用于API请求的身份验证。
    
    该函数返回一个以毫秒为单位的时间戳，表示当前时间加上指定的过期时间后的时刻。
    这个时间戳通常用于API请求中，以确保请求在一定时间内有效。
    
    参数:
        expire_after: float, 过期时间，单位为秒，默认值为30秒。
    
    返回:
        timestamp: int, 以毫秒为单位的时间戳，表示当前时间加上过期时间后的时刻。
    """
    return int(time() * 1000 + expire_after * 1000)

def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    # dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt

def sign(secret: bytes, data: bytes) -> str:
    """
    secret签名
    """
    return hmac.new(secret, data, digestmod=hashlib.sha256).hexdigest()