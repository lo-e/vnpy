import hashlib
import hmac
import sys
import time
from copy import copy
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Set

from pytz import timezone, utc
from vnpy.event.engine import EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    OrderType,
    Product,
    Status,
    Offset
)
from vnpy.trader.gateway import BaseGateway
""" modify by loe """
# 添加了FundingData
from vnpy.trader.object import (
    AccountData,
    FundingData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData
)
from ..rest import Request, RestClient
from ..websocket import WebsocketClient

from vnpy.trader.event import (
    EVENT_TIMER
)

# 中国时区
CHINA_TZ: timezone = timezone("Asia/Shanghai")

# 实盘REST API地址
REST_HOST = "https://api.bybit.com"

# 实盘Websocket API地址
INVERSE_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime"
PUBLIC_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime_public"
PRIVATE_WEBSOCKET_HOST = "wss://stream.bybit.com/realtime_private"
SPOT_PUBLIC_WEBSOCKET_HOST = "wss://stream.bybit.com/spot/public/v3"
SPOT_PRIVATE_WEBSOCKET_HOST = "wss://stream.bybit.com/spot/private/v3"

# 模拟盘REST API地址
TESTNET_REST_HOST = "https://api-testnet.bybit.com"

# 模拟盘Websocket API地址
TESTNET_INVERSE_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime"
TESTNET_PUBLIC_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime_public"
TESTNET_PRIVATE_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/realtime_private"
TESTNET_SPOT_PUBLIC_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/spot/public/v3"
TESTNET_SPOT_PRIVATE_WEBSOCKET_HOST = "wss://stream-testnet.bybit.com/spot/private/v3"

# 委托状态映射
STATUS_BYBIT2VT: Dict[str, Status] = {
    "Created": Status.NOTTRADED,
    "New": Status.NOTTRADED,
    "PartiallyFilled": Status.PARTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "Rejected": Status.REJECTED,
}

# 委托状态【现货】
STATUS_BYBIT2VT_SPOT: Dict[str, Status] = {
    "CREATED": Status.NOTTRADED,
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
}

# 委托类型映射
ORDER_TYPE_VT2BYBIT: Dict[OrderType, str] = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
}
ORDER_TYPE_BYBIT2VT: Dict[str, OrderType] = {v: k for k, v in ORDER_TYPE_VT2BYBIT.items()}

# 委托类型映射【现货】
ORDER_TYPE_VT2BYBIT_SPOT: Dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.LIMIT_MAKER: "LIMIT_MAKER",
    OrderType.MARKET: "MARKET",
}
ORDER_TYPE_BYBIT2VT_SPOT: Dict[str, OrderType] = {v: k for k, v in ORDER_TYPE_VT2BYBIT_SPOT.items()}

# 买卖方向映射
DIRECTION_VT2BYBIT: Dict[Direction, str] = {Direction.LONG: "Buy", Direction.SHORT: "Sell"}
DIRECTION_BYBIT2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BYBIT.items()}

# 数据频率映射
INTERVAL_VT2BYBIT: Dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60",
    Interval.DAILY: "D",
    Interval.WEEKLY: "W",
}
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
    Interval.WEEKLY: timedelta(days=7),
}

# 反向永续合约类型列表
swap_symbols: Set[str] = set()

# 反向交割合约类型列表
futures_symbols: Set[str] = set()

# USDT永续合约类型列表
usdt_symbols: Set[str] = set()

# 现货类型列表
spot_symbols: Set[str] = set()

# 本地委托号缓存集合
local_orderids: Set[str] = set()


class BybitGateway(BaseGateway):
    """
    vn.py用于对接Bybit交易所的交易接口。
    """

    default_setting: Dict[str, str] = {
        "ID": "",
        "Secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": "",
        "合约模式": ["反向", "正向"]
    }

    exchanges: List[Exchange] = [Exchange.BYBIT]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "BYBIT") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.rest_api = None
        self.private_ws_api = None
        self.public_ws_api = None

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        if setting["合约模式"] == "正向":
            self.rest_api: "BybitUsdtRestApi" = BybitUsdtRestApi(self)
            self.private_ws_api: "BybitUsdtPrivateWebsocketApi" = BybitUsdtPrivateWebsocketApi(self)
            self.public_ws_api: "BybitUsdtPublicWebsocketApi" = BybitUsdtPublicWebsocketApi(self)

        else:
            self.rest_api: "BybitInverseRestApi" = BybitInverseRestApi(self)
            self.private_ws_api: "BybitInversePrivateWebsocketApi" = BybitInversePrivateWebsocketApi(self)
            self.public_ws_api: "BybitInversePublicWebsocketApi" = BybitInversePublicWebsocketApi(self)

        key: str = setting["ID"]
        secret: str = setting["Secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: str = setting["代理端口"]

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        self.rest_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.private_ws_api.connect(
            key,
            secret,
            server,
            proxy_host,
            proxy_port
        )
        self.public_ws_api.connect(
            server,
            proxy_host,
            proxy_port
        )

        self.timer_count = 0
        self.register_event()

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def process_timer_event(self, event: Any):
        self.timer_count += 1
        if self.timer_count >= 60:
            self.timer_count = 0

            self.private_ws_api.ping()
            self.public_ws_api.ping()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.public_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        return

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        if self.rest_api:
            self.rest_api.stop()
            self.private_ws_api.stop()
            self.public_ws_api.stop()

    """ modify by loe """
    def query_predicted_funding(self, symbol:str):
        if self.rest_api:
            self.rest_api.query_predicted_funding(symbol=symbol)

class BybitSpotRestApi(RestClient):
    """现货的REST接口"""

    def __init__(self, gateway: BybitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: bytes = b""

        self.order_count: int = 0

    def sign(self, request: Request) -> Request:
        """生成签名"""
        request.headers = {"Referer": "vn.py"}

        if request.method == "GET":
            api_params: dict = request.params
            if api_params is None:
                api_params = request.params = {}
        else:
            api_params: dict = request.data
            if api_params is None:
                api_params = request.data = {}

        api_params["api_key"] = self.key
        api_params["recv_window"] = 30 * 1000
        api_params["timestamp"] = generate_timestamp(-5)

        data2sign = "&".join([f"{k}={v}" for k, v in sorted(api_params.items())])
        signature: str = sign(self.secret, data2sign.encode())
        api_params["sign"] = signature

        return request

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("%Y%m%d-%H%M%S-")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """连接服务器"""
        self.key = key
        self.secret = secret.encode()

        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start(3)
        self.gateway.write_log("REST API启动成功")

        self.query_contract()

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        # 检查委托类型是否正确
        if req.type not in ORDER_TYPE_VT2BYBIT_SPOT:
            self.gateway.write_log(f"委托失败，不支持的委托类型：{req.type.value}")
            return

        # 检查合约代码是否正确并根据合约类型判断下单接口
        if req.symbol in spot_symbols:
            path: str = "/spot/v3/private/order"
        else:
            self.gateway.write_log(f"委托失败，找不到该合约代码{req.symbol}")
            return

        # 生成本地委托号
        orderid: str = self.new_orderid()
        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name)

        # 生成委托请求
        data: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BYBIT[req.direction],
            "qty": req.volume,
            "order_link_id": orderid,
            "time_in_force": "GoodTillCancel",
            "reduce_only": False,
            "close_on_trigger": False
        }

        data["order_type"] = ORDER_TYPE_VT2BYBIT_SPOT[req.type]
        data["price"] = req.price

        """ modify by loe """
        # 增加了CLOSETODAY\CLOSEYESTERDAY
        if req.offset == Offset.CLOSE or req.offset == Offset.CLOSETODAY or req.offset == Offset.CLOSEYESTERDAY:
            data["reduce_only"] = True

        self.add_request(
            "POST",
            path,
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        self.gateway.on_order(order)
        return order.vt_orderid

    def on_send_order_failed(
        self,
        status_code: int,
        request: Request
    ) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        data: dict = request.response.json()
        error_msg: str = data["ret_msg"]
        error_code: int = data["ret_code"]
        msg = f"委托失败，错误代码:{error_code},  错误信息：{error_msg}"
        self.gateway.write_log(msg)

    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        if self.check_error("委托下单", data):
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        # 检查合约代码是否正确并根据合约类型判断撤单接口
        if req.symbol in spot_symbols:
            path: str = "/spot/v3/private/cancel-order"
        else:
            self.gateway.write_log(f"撤单失败，找不到该合约代码{req.symbol}")
            return

        data: dict = {"symbol": req.symbol}

        # 检查是否为本地委托号
        if req.orderid in local_orderids:
            data["orderLinkId"] = req.orderid
        else:
            data["orderId"] = req.orderid

        self.add_request(
            "POST",
            path,
            data=data,
            callback=self.on_cancel_order
        )

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """委托撤单回报"""
        if self.check_error("委托撤单", data):
            return

    def on_failed(self, status_code: int, request: Request) -> None:
        """处理请求失败回报"""
        data: dict = request.response.json()
        error_msg: str = data["ret_msg"]
        error_code: int = data["ret_code"]

        msg = f"请求失败，状态码：{request.status}，错误代码：{error_code}, 信息：{error_msg}"
        self.gateway.write_log(msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """触发异常回报"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约查询回报"""
        if self.check_error("查询合约", data):
            return

        for d in data["result"]:
            # 提取信息生成合约对象
            contract: ContractData = ContractData(
                symbol=d["name"],
                exchange=Exchange.BYBIT,
                name=d["name"],
                product=Product.SPOT,
                size=1,
                pricetick=float(d["minPricePrecision"]),
                min_volume=float(d["minTradeQty"]),
                history_data=True,
                gateway_name=self.gateway_name
            )

            # 缓存现货交易对信息并推送
            spot_symbols.add(d["name"])
            self.gateway.on_contract(contract)

        self.gateway.write_log("合约信息查询成功")
        self.query_account()
        self.query_order()

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        if self.check_error("查询账号", data):
            return

        for balance_data in data["result"]["balances"]:
            coin = balance_data["coin"]
            account: AccountData = AccountData(
                accountid=coin,
                balance=balance_data["total"],
                frozen=balance_data["locked"],
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)
            self.gateway.write_log(f"{coin}资金信息查询成功")

    def on_query_order(self, data: dict, request: Request):
        """未成交委托查询回报"""
        if self.check_error("查询委托", data):
            return

        if not data["result"]:
            return

        for d in data["result"]["list"]:
            orderid: str = d["orderLinkId"]
            if orderid:
                local_orderids.add(orderid)
            else:
                orderid: str = d["orderId"]

            dt: datetime = generate_datetime(d["createTime"])

            order: OrderData = OrderData(
                symbol=d["symbol"],
                exchange=Exchange.BYBIT,
                orderid=orderid,
                type=ORDER_TYPE_BYBIT2VT_SPOT[d["orderType"]],
                direction=DIRECTION_BYBIT2VT[d["side"]],
                price=d["orderPrice"],
                volume=d["orderQty"],
                traded=d["execQty"],
                status=STATUS_BYBIT2VT_SPOT[d["status"]],
                datetime=dt,
                gateway_name=self.gateway_name
            )
            order.offset = Offset.OPEN
            self.gateway.on_order(order)

        self.gateway.write_log(f"{order.symbol}委托信息查询成功")

    def query_contract(self) -> None:
        """查询交易对信息"""
        self.add_request(
            "GET",
            "/spot/v3/public/symbols",
            self.on_query_contract
        )

    def check_error(self, name: str, data: dict) -> bool:
        """回报状态检查"""
        if data["ret_code"]:
            error_code: int = data["ret_code"]
            error_msg: str = data["ret_msg"]
            msg = f"{name}失败，错误代码：{error_code}，信息：{error_msg}"
            self.gateway.write_log(msg)
            return True

        return False

    def query_account(self) -> None:
        """查询资金"""
        self.add_request(
            "GET",
            "/spot/v3/private/account",
            self.on_query_account
        )

    def query_order(self) -> None:
        """查询未成交委托"""
        path_spot: str = "/spot/v3/private/open-orders"

        for symbol in spot_symbols:
            params: dict = {
                "symbol": symbol
            }

            self.add_request(
                "GET",
                path_spot,
                callback=self.on_query_order,
                params=params
            )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return

    """ modify by loe """
    # =================================================
    def query_predicted_funding(self, symbol:str) -> None:
        """查询预测资金费率"""
        return
    # =================================================


def generate_timestamp(expire_after: float = 30) -> int:
    """生成时间戳"""
    return int(time.time() * 1000 + expire_after * 1000)

def sign(secret: bytes, data: bytes) -> str:
    """生成签名"""
    return hmac.new(
        secret, data, digestmod=hashlib.sha256
    ).hexdigest()

def generate_datetime(timestamp: str) -> datetime:
    """生成时间"""
    if "." in timestamp:
        part1, part2 = timestamp.split(".")
        if len(part2) > 7:
            part2 = part2[:6] + "Z"
            timestamp = ".".join([part1, part2])

        dt: datetime = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        dt: datetime = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")

    dt = utc.localize(dt)
    return dt.astimezone(CHINA_TZ)

def generate_datetime_2(timestamp: int) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp)
    return CHINA_TZ.localize(dt)

def get_float_value(data: dict, key: str) -> float:
    """获取字典中对应键的浮点数值"""
    data_str: str = data.get(key, "")
    if not data_str:
        return 0.0
    return float(data_str)
