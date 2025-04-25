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

GATEWAYS = [[OkxGateway, "lo-e(test)"], [BinanceUsdtGateway, "lo-e"]]
class DurationBar(object):
    def __init__(self) -> None:
        self.vt_symbol: str = ""
        self.datetime: datetime = None
        self.open: float = 0
        self.high: float = 0
        self.low: float = 0
        self.close: float = 0
        self.tick_count: int = 0

class MonitorEngine(object):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.event_engine.register(EVENT_TIMER, self.on_timer)
        self.event_engine.register(EVENT_TICK, self.on_tick)

        self.duration_bar_data = {}

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
        # 收到Tick数据
        tick: TickData = event.data
        minute = tick.datetime.minute
        while minute % 5:
            minute -= 1
        duration_dt = tick.datetime.replace(minute=minute, second=0, microsecond=0)

        duration_bar: DurationBar = self.duration_bar_data.get(tick.vt_symbol, DurationBar())
        if duration_bar.datetime != duration_dt:
            if duration_bar.datetime:
                dt_str = duration_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                print(f"{dt_str}\t{duration_bar.tick_count}\t{duration_bar.vt_symbol}\t{duration_bar.open}\t{duration_bar.high}\t{duration_bar.low}\t{duration_bar.close}")
            
            else:
                dt_str = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S.%f")
                print(f"{dt_str}\t{tick.vt_symbol}\t{tick.last_price}")

            duration_bar = DurationBar()
            duration_bar.vt_symbol = tick.vt_symbol
            duration_bar.datetime = duration_dt
            duration_bar.open = tick.last_price
            duration_bar.high = tick.last_price
            duration_bar.low = tick.last_price
            duration_bar.close = tick.last_price
            duration_bar.tick_count = 1
            self.duration_bar_data[tick.vt_symbol] = duration_bar
        
        else:
            duration_bar.high = max(duration_bar.high, tick.last_price)
            duration_bar.low = min(duration_bar.low, tick.last_price)
            duration_bar.close = tick.last_price
            duration_bar.tick_count += 1
    
    def on_timer(self, event):
        now = datetime.now()
        if (now.minute % 5 == 0) and (now.second == 0):
            gateway_all_connected = self.check_gateway_connected()
            if not gateway_all_connected:
                msg = f"交易所连接断开"
                self.main_engine.send_ding_talk(msg)
            print(f"{now}\t交易所连接状态：{gateway_all_connected}")

if __name__ == "__main__":
    # 引擎
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
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

