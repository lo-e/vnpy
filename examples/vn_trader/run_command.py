from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from gateway.binance import BinanceUsdtGateway
from gateway.okx import OkxGateway
from vnpy.trader.utility import load_json
from vnpy.trader.object import SubscribeRequest
import time
from vnpy.trader.event import EVENT_TICK, EVENT_TIMER
from vnpy.trader.object import TickData
from threading import Thread
from datetime import datetime, timedelta
from copy import copy
from App_Command.hit_new.engine import HitNewEngine

GATEWAYS = [[OkxGateway, "lo-e"], [BinanceUsdtGateway, "lo-e"]]
class SecondTick(object):
    def __init__(self) -> None:
        self.vt_symbol: str = ""
        self.timestamp: int = 0
        self.datetime: datetime = None
        self.price: float = 0
        self.count: int = 0

class MonitorEngine(object):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.event_engine.register(EVENT_TIMER, self.on_timer)

    def check_gateway_connected(self):
        all_connected = True
        for gateway_info in GATEWAYS:
            gateway_class: BaseGateway = gateway_info[0]
            account_name = gateway_info[1]
            connected = main_engine.get_gateway_connect_status(gateway_class.gateway_name, account_name)
            if not connected:
                all_connected = False
                break
        return all_connected
    
    def on_timer(self, event):
        now = datetime.now()
        if (now.minute % 5 == 0) and (now.second == 0):
            gateway_all_connected = self.check_gateway_connected()
            print(f"{now}\t交易所连接状态：{gateway_all_connected}")
class SubscribeEngine(object):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.event_engine.register(EVENT_TICK, self.on_tick)

        self.symbol_second_tick_data = {}

    def subscribe(self, vt_symbol: str):
        # 订阅合约
        start = time.time()
        success = False
        while not success:
            contract = main_engine.get_contract(vt_symbol)
            if contract:
                time.sleep(1)
                req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
                main_engine.subscribe(req, contract.gateway_name)
                success = True
            
            if time.time() - start >= 5:
                break

        if not success:
            print(f"行情订阅失败，找不到合约{vt_symbol}")

    def on_tick(self, event):
        return

        # 行情数据处理
        tick: TickData = event.data
        tick_timestamp: int = int(tick.datetime.timestamp())
        second_tick: SecondTick = self.symbol_second_tick_data.get(tick.vt_symbol, SecondTick())
        if second_tick.timestamp != tick_timestamp:
            if second_tick.timestamp:
                print(f"{second_tick.vt_symbol} {second_tick.price}@{second_tick.count} {second_tick.datetime}")

            second_tick = SecondTick()
            second_tick.vt_symbol = tick.vt_symbol
            second_tick.timestamp = tick_timestamp
            second_tick.datetime = datetime.fromtimestamp(tick_timestamp)
            second_tick.price = tick.last_price
            second_tick.count = 1
            self.symbol_second_tick_data[tick.vt_symbol] = second_tick
        
        else:
            second_tick.count += 1

if __name__ == "__main__":
    # 引擎
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    subscribe_engine = SubscribeEngine(main_engine, event_engine)
    monitor_engine = MonitorEngine(main_engine, event_engine)

    # 数据库
    main_engine.dbConnect()

    # 连接交易所
    for gateway_info in GATEWAYS:
        gateway_class: BaseGateway = gateway_info[0]
        account_name = gateway_info[1]

        main_engine.add_gateway(gateway_class)
        gateway_setting_filename = f"connect_{gateway_class.gateway_name.lower()}.json"
        connect_setting = load_json(gateway_setting_filename)
        connect_setting = connect_setting.get(account_name, None)
        main_engine.connect(connect_setting, gateway_class.gateway_name)

    # 等待交易所连接成功
    while True:
        gateway_all_connected = monitor_engine.check_gateway_connected()
        if gateway_all_connected:
            break

        else:
            time.sleep(1)

    # 执行策略
    hit_new_app = HitNewEngine(main_engine=main_engine, event_engine=event_engine)
    hit_new_app.init_engine()
    hit_new_app.init_portfolio()
    hit_new_app.start_portfolio()

