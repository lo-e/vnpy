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

# 买卖方向映射【现货】
DIRECTION_VT2BYBIT_SPOT: Dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
DIRECTION_BYBIT2VT_SPOT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BYBIT_SPOT.items()}

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

class BybitSpotPublicWebsocketApi(WebsocketClient):
    """正向合约的行情Websocket接口"""

    def __init__(self, gateway: BybitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BybitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.callbacks: Dict[str, Callable] = {}
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.symbol_bids: Dict[str, dict] = {}
        self.symbol_asks: Dict[str, dict] = {}

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接Websocket公共频道"""
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.server = server

        if self.server == "REAL":
            url = SPOT_PUBLIC_WEBSOCKET_HOST
        else:
            url = TESTNET_SPOT_PUBLIC_WEBSOCKET_HOST

        self.init(url, self.proxy_host, self.proxy_port)
        self.start()

    def ping(self):
        req: dict = {
            "op": "ping",
        }
        self.send_packet(req)

    def on_ping(self, packet: dict):
        self._connect_id = packet.get('conn_id', '')

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("行情Websocket API连接成功")

        if self.subscribed:
            for req in self.subscribed.values():
                self.subscribe(req, for_reconnect=True)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("行情Websocket API连接断开")

    """ modify by loe """
    # 增加了for_reconnect参数
    def subscribe(self, req: SubscribeRequest, for_reconnect: bool = False) -> None:
        """订阅行情"""
        if req.symbol in self.subscribed and not for_reconnect:
            return

        # 缓存订阅记录
        self.subscribed[req.symbol] = req

        # 创建TICK对象
        tick: TickData = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(),
            name=req.symbol,
            gateway_name=self.gateway_name
        )
        self.ticks[req.symbol] = tick

        # 发送订阅请求
        self.subscribe_topic(f"instrument_info.100ms.{req.symbol}", self.on_tick)
        self.subscribe_topic(f"orderbook.40.{req.symbol}", self.on_depth)

    def subscribe_topic(
        self,
        topic: str,
        callback: Callable[[str, dict], Any]
    ) -> None:
        """订阅公共频道推送"""
        self.callbacks[topic] = callback

        req: dict = {
            "op": "subscribe",
            "args": [topic],
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if "topic" not in packet:
            op: str = packet["request"]["op"]
            if op == "auth":
                self.on_login(packet)

            elif op == 'ping':
                self.on_ping(packet)
        else:
            channel: str = packet["topic"]
            callback: callable = self.callbacks[channel]
            callback(packet)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb
    ) -> None:
        """触发异常回报"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))

    def on_tick(self, packet: dict) -> None:
        """行情推送回报"""
        topic: str = packet["topic"]
        type_: str = packet["type"]
        data: dict = packet["data"]

        symbol: str = topic.replace("instrument_info.100ms.", "")
        tick: TickData = self.ticks[symbol]

        if type_ == "snapshot":
            if not data["last_price"]:           # 过滤最新价为0的数据
                return

            tick.last_price = float(data["last_price"])

            tick.volume = int(data.get('volume_24h_e8', 0)) / 100000000

            tick.datetime = generate_datetime(data["updated_at"])

        else:
            update: dict = data["update"][0]

            if "last_price" not in update:      # 过滤最新价为0的数据
                return

            tick.last_price = float(update["last_price"])

            tick.volume = int(update.get('volume_24h_e8', 0)) / 100000000

            tick.datetime = generate_datetime(update["updated_at"])

        self.gateway.on_tick(copy(tick))

    def on_depth(self, packet: dict) -> None:
        """盘口推送回报"""
        topic: str = packet["topic"]
        type_: str = packet["type"]
        data: dict = packet["data"]
        if not data:
            return

        symbol: str = topic.replace("orderbook.40.", "")
        tick: TickData = self.ticks[symbol]
        bids: dict = self.symbol_bids.setdefault(symbol, {})
        asks: dict = self.symbol_asks.setdefault(symbol, {})

        if type_ == "snapshot":

            buf: list = data["order_book"]

            for d in buf:
                price: float = float(d["price"])

                if d["side"] == "Buy":
                    bids[price] = d
                else:
                    asks[price] = d
        else:
            for d in data["delete"]:
                price: float = float(d["price"])

                if d["side"] == "Buy":
                    bids.pop(price)
                else:
                    asks.pop(price)

            for d in (data["update"] + data["insert"]):

                price: float = float(d["price"])
                if d["side"] == "Buy":
                    bids[price] = d
                else:
                    asks[price] = d

        bid_keys: list = list(bids.keys())
        bid_keys.sort(reverse=True)

        ask_keys: list = list(asks.keys())
        ask_keys.sort()

        for i in range(5):
            n = i + 1

            bid_price = bid_keys[i]
            bid_data = bids[bid_price]
            ask_price = ask_keys[i]
            ask_data = asks[ask_price]

            setattr(tick, f"bid_price_{n}", bid_price)
            setattr(tick, f"bid_volume_{n}", bid_data["size"])
            setattr(tick, f"ask_price_{n}", ask_price)
            setattr(tick, f"ask_volume_{n}", ask_data["size"])

        tick.datetime = generate_datetime_2(int(packet["timestamp_e6"]) / 1000000)
        self.gateway.on_tick(copy(tick))


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
