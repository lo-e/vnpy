"""
1. 只支持全仓模式
2. 只支持正向合约
"""

import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Any, Dict, List, Tuple
from asyncio import run_coroutine_threadsafe

from regex import R
from requests.exceptions import SSLError

from vnpy.event import Event, EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval,
    Offset,
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    SubscribeLotsRequest,
    HistoryRequest,
)
from vnpy.trader.event import EVENT_TIMER, EVENT_GATEWAY_LEVERAGE_FAILED, EVENT_GATEWAY_FUNDING_RATES
from vnpy.trader.utility import round_to

# from vnpy_rest import Request, RestClient, Response
# from vnpy_websocket import WebsocketClient
from ..rest import Request, RestClient, Response
from ..websocket import WebsocketClient
from pytz import timezone
from queue import Empty, Queue
from threading import Thread
from time import sleep


# 中国时区
CHINA_TZ: timezone = timezone("Asia/Shanghai")

# 实盘正向合约REST API地址
F_REST_HOST: str = "https://fapi.binance.com"

# 实盘正向合约Websocket API地址
F_WEBSOCKET_TRADE_HOST: str = "wss://fstream.binance.com/ws/"
F_WEBSOCKET_DATA_HOST: str = "wss://fstream.binance.com/stream"

# 模拟盘正向合约REST API地址
F_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"

# 模拟盘正向合约Websocket API地址
F_TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://stream.binancefuture.com/ws/"
F_TESTNET_WEBSOCKET_DATA_HOST: str = "wss://stream.binancefuture.com/stream"

# 委托状态映射
STATUS_BINANCES2VT: Dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED,
    "TRIGGERING": Status.TRIGGERED
}

# 委托类型映射
ORDERTYPE_VT2BINANCES: Dict[OrderType, Tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
    OrderType.STOP: ("STOP_MARKET", "GTE_GTC"),
    OrderType.STOP_MARKET: ("STOP_MARKET", "GTC")
}
ORDERTYPE_BINANCES2VT: Dict[Tuple[str, str], OrderType] = {
    v: k for k, v in ORDERTYPE_VT2BINANCES.items()
}

# 买卖方向映射
DIRECTION_VT2BINANCES: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL",
}
DIRECTION_BINANCES2VT: Dict[str, Direction] = {
    v: k for k, v in DIRECTION_VT2BINANCES.items()
}

# 数据频率映射
INTERVAL_VT2BINANCES: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}


# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinanceUsdtGateway(BaseGateway):
    """
    vn.py用于对接币安正向合约的交易接口。
    """

    gateway_name: str = "BINANCE"

    default_setting: Dict[str, Any] = {
        "账户名称":"",
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(
        self, event_engine: EventEngine
    ) -> None:
        """构造函数"""
        super().__init__(event_engine)

        self.trade_ws_api: "BinanceUsdtTradeWebsocketApi" = (BinanceUsdtTradeWebsocketApi(self))
        self.market_ws_api: "BinanceUsdtDataWebsocketApi" = BinanceUsdtDataWebsocketApi(self)
        self.rest_api: "BinanceUsdtRestApi" = BinanceUsdtRestApi(self)

        self.orders: Dict[str, OrderData] = {}
        self.account_positon_update_wait = 0
        self.server_time_update_wait = 0

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["key"]
        secret: str = setting["secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.market_ws_api.connect(proxy_host, proxy_port, server)

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.market_ws_api.subscribe(req)

    def subscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 订阅行情 """
        self.market_ws_api.subscribe_lots(req)

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """ 取消订阅 """
        self.market_ws_api.unsubscribe(req)

    def unsubscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 取消订阅 """
        self.market_ws_api.unsubscribe_lots(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_funding_rate(self) -> None:
        """查询资金费率"""
        self.rest_api.query_funding_rate()

    def on_query_funding_rates(self, funding_rates: List[dict]) -> None:
        """推送资金费率"""
        self.event_engine.put(
            Event(
                EVENT_GATEWAY_FUNDING_RATES,
                {
                    "funding_rates": funding_rates,
                    "gateway_name": self.gateway_name,
                },
            )
        )

    def query_contract(self) -> None:
        """查询合约列表"""
        self.rest_api.query_contract()

    def query_account(self) -> None:
        """查询资金"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.rest_api.query_position()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        # 延长listenKey
        self.rest_api.keep_user_stream()
        # self.rest_api.check_trade_ws()

        # 更新账户、持仓
        self.account_positon_update_wait += 1
        if self.account_positon_update_wait >= 10:
            self.account_positon_update_wait = 0
            self.query_account()
            self.query_position()

        self.server_time_update_wait += 1
        if self.server_time_update_wait >= 10:
            self.server_time_update_wait = 0
            self.rest_api.query_time()

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)

    def check_connected(self) -> Dict[str, Any]:
        """检查连接状态"""
        connected = True
        msg = ""

        if not self.rest_api.contract_info_ready:
            connected = False
            msg += "Rest API 合约信息未就绪"

        if not self.market_ws_api.connected:
            connected = False
            msg += "行情Websocket API连接断开"
        
        if not self.trade_ws_api.connected:
            connected = False
            if msg:
                msg += "\n"
            msg += "交易Websocket API连接断开"

        res = {"gateway":self.gateway_name, "connected":connected, "msg":msg}
        return res
    
    def set_leverage(self, vt_symbol: str, target: int):
        """
        设置合约杠杆
        """
        self.rest_api.set_leverage(vt_symbol, target)

    def get_accounts(self) -> Dict[str, AccountData]:
        """
        获取账户信息
        """
        return self.rest_api.accounts
    
    def get_positions(self) -> Dict[str, PositionData]:
        """
        获取持仓信息
        """
        return self.rest_api.positions
class BinanceUsdtRestApi(RestClient):
    """币安正向合约的REST API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceUsdtTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0
        self.trade_ws_ping_wait = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.accounts: dict = {}
        self.positions: dict= {}
        self.contract_info_ready = False

    def sign(self, request: Request) -> Request:
        """生成币安签名"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)

            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret, query.encode("utf-8"), hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # 添加请求头
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close",
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self, key: str, secret: str, server: str, proxy_host: str, proxy_port: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        if self.server == "REAL":
            self.init(F_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("REST API启动成功")

        self.query_time()
        self.query_account()
        self.query_position()
        self.query_order()
        self.query_contract()
        self.start_user_stream()

    def set_leverage(self, vt_symbol: str, target: int):
        symbol = vt_symbol.split(".")[0]

        data: dict = {"security": Security.SIGNED}

        params = {"symbol": symbol,
                  "leverage": target}

        path: str = "/fapi/v1/leverage"

        return self.add_request("POST", path, callback=self.on_leverage, data=data, params=params, on_failed=self.on_leverage_failed, on_error=self.on_leverage_error, extra=params)
    
    def on_leverage(self, data: dict, request: Request) -> None:
        # symbol = request.extra.get("symbol", "")
        # leverage = request.extra.get("leverage", "")
        # print(f"{symbol} leverage {leverage} 成功")
        pass

    def on_leverage_failed(self, status_code: str, request: Request) -> None:
        symbol = request.extra.get("symbol", "")
        leverage = request.extra.get("leverage", "")
        print(f"{symbol} leverage {leverage} 失败")

        event_data = {"symbol": request.extra["symbol"],
                      "leverage": request.extra["leverage"],
                      "gateway_name": self.gateway_name,
                      "account_name": self.gateway.account_name}
        event = Event(EVENT_GATEWAY_LEVERAGE_FAILED, event_data)
        self.gateway.event_engine.put(event)

    def on_leverage_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        symbol = request.extra.get("symbol", "")
        leverage = request.extra.get("leverage", "")
        print(f"{symbol} leverage {leverage} 失败")

        event_data = {"symbol": request.extra["symbol"],
                      "leverage": request.extra["leverage"],
                      "gateway_name": self.gateway_name}
        event = Event(EVENT_GATEWAY_LEVERAGE_FAILED, event_data)
        self.gateway.event_engine.put(event)

    def query_time(self) -> None:
        """查询时间"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/time"

        return self.add_request("GET", path, callback=self.on_query_time, data=data)
    
    def query_funding_rate(self) -> None:
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/premiumIndex"

        return self.add_request("GET", path, callback=self.on_query_funding_rate, data=data)
    
    def query_account(self) -> None:
        """查询资金"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v2/account"

        self.add_request(
            method="GET", path=path, callback=self.on_query_account, data=data
        )

    def query_position(self) -> None:
        """查询持仓"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v2/positionRisk"

        self.add_request(
            method="GET", path=path, callback=self.on_query_position, data=data
        )

    def query_order(self) -> None:
        """查询未成交委托"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"

        self.add_request(
            method="GET", path=path, callback=self.on_query_order, data=data
        )

    def query_contract(self) -> None:
        """查询合约信息"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET", path=path, callback=self.on_query_contract, data=data
        )

    def _new_order_id(self) -> int:
        """生成本地委托号"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        path: str = "/fapi/v1/order"

        # 生成本地委托号
        orderid: str = str(self.connect_time + self._new_order_id())

        data: dict = {"security": Security.SIGNED}

        position_side = ""
        if (req.direction == Direction.LONG and req.offset == Offset.OPEN) or (
            req.direction == Direction.SHORT and req.offset != Offset.OPEN
        ):
            position_side = "LONG"

        elif (req.direction == Direction.SHORT and req.offset == Offset.OPEN) or (
            req.direction == Direction.LONG and req.offset != Offset.OPEN
        ):
            position_side = "SHORT"

        # 生成委托请求
        params: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINANCES[req.direction],
            "quantity": float(req.volume),
            "newClientOrderId": orderid,
        }

        if position_side:
            params["positionSide"] = position_side

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"

        elif req.type == OrderType.STOP or req.type == OrderType.STOP_MARKET:
            path = "/fapi/v1/algoOrder"
            params["algoType"] = "CONDITIONAL"
            params["type"] = "STOP_MARKET"
            params["triggerPrice"] = float(req.price)
            params["closePosition"] = "true"
            params["clientAlgoId"] = orderid

        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = float(req.price)

        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name, self.gateway.account_name)
        self.gateway.on_order(order)

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        data: dict = {"security": Security.SIGNED}

        params: dict = {"symbol": req.symbol, "origClientOrderId": req.orderid}

        path: str = "/fapi/v1/order"

        order: OrderData = self.gateway.get_order(req.orderid)

        if order.type == OrderType.STOP or order.type == OrderType.STOP_MARKET:
            path = "/fapi/v1/algoOrder"
            params = {"clientalgoid": req.orderid}

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order,
        )

    def start_user_stream(self) -> Request:
        """生成listenKey"""
        data: dict = {"security": Security.API_KEY}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="POST", path=path, callback=self.on_start_user_stream, data=data
        )

    def keep_user_stream(self) -> Request:
        """延长listenKey有效期"""
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return
        self.keep_alive_count = 0

        data: dict = {"security": Security.API_KEY}

        params: dict = {"listenKey": self.user_stream_key}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_error=self.on_keep_user_stream_error,
        )

    def check_trade_ws(self):
        self.trade_ws_ping_wait += 1
        if self.trade_ws_ping_wait < 2 * 60:
            return
        self.trade_ws_ping_wait = 0
        self.trade_ws_api.ping()

    def on_query_time(self, data: dict, request: Request) -> None:
        """时间查询回报"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

    def on_query_funding_rate(self, data: dict, request: Request) -> None:
        """资金费率查询回报"""
        funding_rates = []
        for d in data:
            symbol = d["symbol"]
            quote = symbol[-4:]
            if quote != "USDT":
                continue

            funding_rate = float(d["lastFundingRate"])
            if not funding_rate:
                continue

            timestamp = int(d["time"])
            dt = datetime.fromtimestamp(timestamp / 1000, tz=CHINA_TZ)
            next_fundingtimestamp = int(d["nextFundingTime"])
            next_funding_dt = datetime.fromtimestamp(next_fundingtimestamp / 1000)
            
            funding_rate_data = {"symbol": symbol,
                                 "funding_rate": funding_rate,
                                 "datetime": dt,
                                 "next_funding_datetime": next_funding_dt,
                                 "mark_price": float(d["markPrice"]),
                                 "gateway_name": self.gateway_name}
            funding_rates.append(funding_rate_data)
        self.gateway.on_query_funding_rates(funding_rates)

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        accountids = set()
        for asset in data["assets"]:
            unrealized_profit = float(asset["unrealizedProfit"])
            frozen = abs(unrealized_profit) if unrealized_profit < 0 else 0
            accountid = asset["asset"]
            accountids.add(accountid)
            account: AccountData = AccountData(
                accountid=accountid,
                balance=float(asset["walletBalance"]),
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

        # self.gateway.write_log("账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """持仓查询回报"""
        for d in data:
            # 持仓数量
            volume = d["positionAmt"]
            if "." in volume:
                volume = float(d["positionAmt"])
            else:
                volume = int(d["positionAmt"])

            # 持仓方向
            direction = Direction.NET
            position_side = d.get("positionSide", "")
            if position_side:
                if position_side == "LONG":
                    direction = Direction.LONG

                elif position_side == "SHORT":
                    direction = Direction.SHORT
                
            else:
                if volume > 0:
                    direction = Direction.LONG

                elif volume < 0:
                    direction = Direction.SHORT

            # 持仓合约
            symbol = d["symbol"]
            direction_symbol = f"{symbol}_{direction.value}"

            if volume:
                # 创建
                position: PositionData = PositionData(
                    symbol=symbol,
                    exchange=Exchange.BINANCE,
                    exchange_user=self.gateway.account_name,
                    direction=direction,
                    volume=abs(volume),
                    price=float(d["entryPrice"]),
                    pnl=float(d["unRealizedProfit"]),
                    gateway_name=self.gateway_name,
                )

                # 回调
                self.positions[direction_symbol] = position
                self.gateway.on_position(position)
            
            elif direction_symbol in self.positions:
                # 清仓
                position = self.positions[direction_symbol]
                position.volume = 0
                position.price = 0
                position.pnl = 0
                self.gateway.on_position(position)

        # self.gateway.write_log("持仓信息查询成功")

    def on_query_order(self, data: dict, request: Request) -> None:
        """未成交委托查询回报"""
        for d in data:
            key: Tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
            if not order_type:
                continue

            order: OrderData = OrderData(
                orderid=d["clientOrderId"],
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCES2VT[d["side"]],
                traded=float(d["executedQty"]),
                status=STATUS_BINANCES2VT.get(d["status"], None),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
                account_name=self.gateway.account_name
            )
            self.gateway.on_order(order)

        self.gateway.write_log("委托信息查询成功")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            # 排除未正式交易的合约、交割合约
            if quote_currency != "USDT" or d["contractType"] != "PERPETUAL" or d["status"] != "TRADING":
                continue

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])

            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True,
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.contract_info_ready = True
        self.gateway.write_log(f"合约信息查询成功：{len(symbol_contract_map)}")

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """委托撤单回报"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """撤单回报函数报错回报"""
        if request.extra:
            order = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """生成listenKey回报"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = F_WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = F_TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """延长listenKey有效期回报"""
        pass

    def on_keep_user_stream_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """延长listenKey有效期函数报错回报"""
        # 当延长listenKey有效期时，忽略超时报错
        if not issubclass(exception_type, TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: List[BarData] = []
        limit: int = 1500

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # 创建查询参数
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit,
            }

            params["startTime"] = start_time * 1000
            path: str = "/fapi/v1/klines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000  # 转换成毫秒

            resp: Response = self.request(
                "GET", path=path, data={"security": Security.NONE}, params=params
            )

            # 如果请求失败则终止循环
            if resp.status_code // 100 != 2:
                msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(msg)
                    break

                buf: List[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name,
                    )
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg: str = (
                    f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                )
                self.gateway.write_log(msg)

                # 如果收到了最后一批数据则终止循环
                if len(data) < limit:
                    break

                # 更新开始时间
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

        return history

class BinanceUsdtTradeWebsocketApi(WebsocketClient):
    """币安正向合约的交易Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """连接Websocket交易频道"""
        self.init(url, proxy_host, proxy_port)
        self.start()
    
    def disconnect(self) -> None:
        """ "主动断开webscoket链接"""
        self._active = False
        ws = self._ws
        if ws:
            coro = ws.close()
            run_coroutine_threadsafe(coro, self._loop)

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("交易Websocket API连接成功")
        self.connected = True

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if "e" not in packet:
            return

        if packet["e"] == "ACCOUNT_UPDATE":
            self.gateway.query_account()
            self.gateway.query_position()
            # self.on_account(packet)

        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)
        
        elif packet["e"] == "ALGO_UPDATE":
            self.on_algo_order(packet)

        elif packet["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """ListenKey过期"""
        self.gateway.write_log("listenKey过期")
        self.disconnect()

    def on_account(self, packet: dict) -> None:
        """资金更新推送"""
        for acc_data in packet["a"]["B"]:
            account: AccountData = AccountData(
                accountid=acc_data["a"],
                balance=float(acc_data["wb"]),
                frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
                gateway_name=self.gateway_name,
                exchange_user=self.gateway.account_name,
            )
            
            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            volume = pos_data["pa"]
            if "." in volume:
                volume = float(volume)
            else:
                volume = int(volume)

            direction = Direction.NET
            position_side = pos_data.get("ps", "")
            if position_side:
                if position_side == "LONG":
                    direction = Direction.LONG

                elif position_side == "SHORT":
                    direction = Direction.SHORT
                
            else:
                if volume > 0:
                    direction = Direction.LONG

                elif volume < 0:
                    direction = Direction.SHORT
                
            position: PositionData = PositionData(
                symbol=pos_data["s"],
                exchange=Exchange.BINANCE,
                exchange_user=self.gateway.account_name,
                direction=direction,
                volume=abs(volume),
                price=float(pos_data["ep"]),
                pnl=float(pos_data["up"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """委托更新推送"""
        ord_data: dict = packet["o"]
        key: Tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        offset = Offset.NONE
        if self.gateway.get_order(ord_data["c"]):
            offset = self.gateway.get_order(ord_data["c"]).offset

        if ord_data["R"]:
            offset = Offset.CLOSE

        order: OrderData = OrderData(
            symbol=ord_data["s"],
            exchange=Exchange.BINANCE,
            orderid=str(ord_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[ord_data["S"]],
            price=float(ord_data["p"]),
            volume=float(ord_data["q"]),
            traded=float(ord_data["z"]),
            status=STATUS_BINANCES2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
            offset=offset,
            account_name=self.gateway.account_name
        )
        self.gateway.on_order(order)

        # 将成交数量四舍五入到正确精度
        trade_volume: float = float(ord_data["l"])
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            trade_volume = round_to(trade_volume, contract.min_volume)

        if not trade_volume:
            return

        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=ord_data["t"],
            direction=order.direction,
            price=float(ord_data["L"]),
            volume=trade_volume,
            datetime=generate_datetime(ord_data["T"]),
            gateway_name=self.gateway_name,
            offset=offset,
            account_name=self.gateway.account_name
        )
        self.gateway.on_trade(trade)

    def on_algo_order(self, packet: dict) -> None:
        """委托更新推送"""
        ord_data: dict = packet["o"]
        key: Tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        offset = Offset.NONE
        if self.gateway.get_order(ord_data["caid"]):
            offset = self.gateway.get_order(ord_data["caid"]).offset

        if ord_data["R"]:
            offset = Offset.CLOSE

        order: OrderData = OrderData(
            symbol=ord_data["s"],
            exchange=Exchange.BINANCE,
            orderid=str(ord_data["caid"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[ord_data["S"]],
            price=float(ord_data["p"]),
            volume=float(ord_data["q"]),
            status=STATUS_BINANCES2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
            offset=offset,
            account_name=self.gateway.account_name
        )
        self.gateway.on_order(order)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("交易Websocket API连接断开")
        self.connected = False
        self.gateway.rest_api.start_user_stream()

    def ping(self):
        req: dict = {"op": "ping"}
        self.send_packet(req)

class BinanceUsdtDataWebsocketApi(WebsocketClient):
    """币安正向合约的行情Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: Dict[str, TickData] = {}
        self.tick_ts_data: Dict[str, int] = {}
        self.reqid: int = 0

        self.subscribed: Dict[str, SubscribeRequest] = {}

        self.subscribe_thread = Thread(target=self.run_subscribe)
        self.subscribe_thread.start()
        self.subscribe_queue = Queue()

        self.unsubscribe_thread = Thread(target=self.run_unsubscribe)
        self.unsubscribe_thread.start()
        self.unsubscribe_queue = Queue()

    def connect(self, proxy_host: str, proxy_port: int, server: str) -> None:
        """连接Websocket行情频道"""
        if server == "REAL":
            self.init(F_WEBSOCKET_DATA_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_WEBSOCKET_DATA_HOST, proxy_host, proxy_port)

        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("行情Websocket API连接成功")
        self.connected = True

        # 重新订阅行情
        exchange_symbols_data = {}
        for vt_symbol in self.subscribed.keys():
            req: SubscribeRequest = self.subscribed[vt_symbol]
            exchange_symbols = exchange_symbols_data.get(req.exchange, set())
            exchange_symbols.add(req.symbol)
            exchange_symbols_data[req.exchange] = exchange_symbols
        
        for _, exchange_symbols in exchange_symbols_data.items():
            self.subscribe_queue.put(list(exchange_symbols))
            
    def subscribe(self, req: SubscribeRequest) -> None:
        """ 订阅行情 """
        if req.vt_symbol in self.subscribed:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        # 缓存订阅记录
        self.subscribed[req.vt_symbol] = req

        # 加入订阅队列
        self.subscribe_queue.put(req.symbol)

    def subscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 订阅行情 """
        for symbol in req.symbols.copy():
            vt_symbol = f"{symbol}.{req.exchange.value}"

            remove = False
            if vt_symbol in self.subscribed:
                req.symbols.remove(symbol)
                remove = True

            if symbol not in symbol_contract_map:
                self.gateway.write_log(f"找不到该合约代码{symbol}")
                req.symbols.remove(symbol)
                remove = True

            # 缓存订阅记录
            if not remove:
                self.subscribed[vt_symbol] = SubscribeRequest(symbol=symbol, exchange=req.exchange)

        if not req.symbols:
            return
        
        # 加入订阅队列
        self.subscribe_queue.put(req.symbols)

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """ 取消订阅 """

        # 清除订阅记录
        if req.vt_symbol in self.subscribed:
            self.subscribed.pop(req.vt_symbol)
        
        # 加入取消订阅队列
        self.unsubscribe_queue.put(req.symbol)

    def unsubscribe_lots(self, req: SubscribeLotsRequest) -> None:
        """ 取消订阅 """
        # 清除缓存订阅记录
        for symbol in req.symbols:
            vt_symbol = f"{symbol}.{req.exchange.value}"
            if vt_symbol in self.subscribed:
                self.subscribed.pop(vt_symbol)
        
        # 加入取消订阅队列
        self.unsubscribe_queue.put(req.symbols)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        stream: str = packet.get("stream", None)
        if not stream:
            return

        data: dict = packet["data"]
        symbol, channel = stream.split("@")[:2]
        symbol_upper = symbol.upper()
        tick = self.ticks.get(symbol_upper, None)

        if not tick:
            # 创建TICK对象
            tick: TickData = TickData(
                symbol=symbol_upper,
                name=symbol_contract_map[symbol_upper].name,
                exchange=Exchange.BINANCE,
                datetime=datetime.now(),
                gateway_name=self.gateway_name,
            )
            self.ticks[symbol_upper] = tick

        if channel == "ticker":
            tick.volume = float(data["v"])
            tick.turnover = float(data["q"])
            tick.open_price = float(data["o"])
            tick.high_price = float(data["h"])
            tick.low_price = float(data["l"])
            tick.last_price = float(data["c"])
            tick.datetime = generate_datetime(float(data["E"]))

        elif channel == "aggTrade":
            last_price = float(data["p"])
            timestamp = float(data["T"])
            dt = generate_datetime(timestamp)

            # 高频行情数据过滤（按时间）
            # last_ts = self.tick_ts_data.get(symbol_upper, 0)
            # current_ts = int(time.time()*1000)
            # # current_ts = timestamp
            # if current_ts - last_ts < 100:
            #     return
            # self.tick_ts_data[symbol_upper] = current_ts

            # 高频行情数据过滤（按价格）
            # if tick.datetime.minute == dt.minute and tick.last_price == float(data["p"]):
            #     return
        
            tick.volume = float(data["q"])
            tick.last_price = last_price
            tick.datetime = dt

        elif channel == "depth5":
            dt = generate_datetime(data["E"])
            tick.datetime = dt
            bids: list = data["b"]
            for n in range(min(5, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["a"]
            for n in range(min(5, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("行情Websocket API连接断开")
        self.connected = False
    
    def run_subscribe(self):
        # @ticker 按Symbol刷新的24小时完整ticker信息
        # @aggTrade # 同一价格、同一方向、同一时间(100ms计算)的归集交易、有限档深度信息
        # @depth5@100ms 有限档深度信息
        while True:
            try:
                data = self.subscribe_queue.get(block=True, timeout=1)
                if isinstance(data, str):
                    symbol = data
                    self.reqid += 1
                    channels = [f"{symbol.lower()}@aggTrade"]
                    req: dict = {"method": "SUBSCRIBE", "params": channels, "id": self.reqid}
                    self.send_packet(req)

                if isinstance(data, list):
                    symbols = data
                    if len(data) > 100:
                        symbols = data[:100]
                        self.subscribe_queue.put(data[100:])

                    self.reqid += 1
                    channels = []
                    for symbol in symbols:
                        channels.append(f"{symbol.lower()}@aggTrade")

                    req: dict = {"method": "SUBSCRIBE", "params": channels, "id": self.reqid}
                    self.send_packet(req)
                
                    time.sleep(10)

            except:
                pass

    def run_unsubscribe(self):
        # @ticker 按Symbol刷新的24小时完整ticker信息
        # @aggTrade # 同一价格、同一方向、同一时间(100ms计算)的归集交易、有限档深度信息
        # @depth5@100ms 有限档深度信息
        while True:
            try:
                data = self.unsubscribe_queue.get(block=True, timeout=1)
                if isinstance(data, str):
                    symbol = data
                    self.reqid += 1
                    channels = [f"{symbol.lower()}@aggTrade"]
                    req: dict = {"method": "UNSUBSCRIBE", "params": channels, "id": self.reqid}
                    self.send_packet(req)

                if isinstance(data, list):
                    symbols = data
                    if len(data) > 100:
                        symbols = data[:100]
                        self.unsubscribe_queue.put(data[100:])

                    self.reqid += 1
                    channels = []
                    for symbol in symbols:
                        channels.append(f"{symbol.lower()}@aggTrade")

                    req: dict = {"method": "UNSUBSCRIBE", "params": channels, "id": self.reqid}
                    self.send_packet(req)

                    time.sleep(10)

            except:
                pass

def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    # dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt
