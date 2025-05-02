from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine
from vnpy.trader.gateway import BaseGateway
from gateway.binance import BinanceUsdtGateway
from gateway.bybit import BybitGateway
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
from App_Command.trending_sniper.engine import TrendingSniperEngine

GATEWAYS = [[OkxGateway, "lo-e"], [BybitGateway, "loesuperman"], [BinanceUsdtGateway, "lo-e"]]
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

        self.gateway_connected = False
        self.duration_bar_data = {}
        self.history_duration_bar_data = {}
        self.tick_delay_time = 0
        self.tick_delay_count = 0
        self.tick = None

    def check_gateway_connected(self):
        all_connected = True
        for gateway_info in GATEWAYS:
            gateway_class: BaseGateway = gateway_info[0]
            account_name = gateway_info[1]
            connected = self.main_engine.get_gateway_connect_status(gateway_class.gateway_name, account_name)

            if not connected:
                all_connected = False
                break

        return all_connected
    
    def subscribe(self, vt_symbol: str):
        # 订阅合约
        start = time.time()
        success = False
        while not success:
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                time.sleep(1)
                req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
                self.main_engine.subscribe(req, contract.gateway_name)
                success = True
            
            if time.time() - start >= 5:
                break

        if not success:
            print(f"行情订阅失败，找不到合约{vt_symbol}")

    def on_tick(self, event):
        # 收到Tick数据
        tick: TickData = event.data
        self.tick = copy(tick)
        tick_dt = tick.datetime.replace(tzinfo=None)
        delay = time.time() - tick_dt.timestamp()
        if delay >= 5 and len(self.history_duration_bar_data):
            self.tick_delay_count += 1
            if time.time() > self.tick_delay_time + 20:
                msg = f"Tick数据延迟\n时长 {delay:.2f}s 数量 {self.tick_delay_count}"
                self.main_engine.send_ding_talk(msg)
                print_(msg)

                self.tick_delay_time = time.time()
                self.tick_delay_count = 0

        minute = tick.datetime.minute
        while minute % 1:
            minute -= 1
        duration_dt = tick.datetime.replace(minute=minute, second=0, microsecond=0)
        duration_bar: DurationBar = self.duration_bar_data.get(tick.vt_symbol, DurationBar())
        if duration_bar.datetime != duration_dt:
            duration_bar = DurationBar()
            duration_bar.vt_symbol = tick.vt_symbol
            duration_bar.datetime = duration_dt
            duration_bar.open = tick.last_price
            duration_bar.high = tick.last_price
            duration_bar.low = tick.last_price
            duration_bar.close = tick.last_price
            duration_bar.tick_count = 1

            history_duration_bar = self.duration_bar_data.get(tick.vt_symbol, None)
            if history_duration_bar:
                self.history_duration_bar_data[tick.vt_symbol] = history_duration_bar
            self.duration_bar_data[tick.vt_symbol] = duration_bar
        
        else:
            duration_bar.high = max(duration_bar.high, tick.last_price)
            duration_bar.low = min(duration_bar.low, tick.last_price)
            duration_bar.close = tick.last_price
            duration_bar.tick_count += 1
    
    def on_timer(self, event):
        now = datetime.now()
        if (now.minute % 1 == 0) and (now.second == 15) and self.tick:
            # 输出Tick信息
            sorted_duration_bar_list = sorted(self.history_duration_bar_data.values(), key=lambda x: x.tick_count)
            for duration_bar in sorted_duration_bar_list[:5]:
                dt_str = duration_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                print_(f"{duration_bar.vt_symbol}({duration_bar.tick_count})\t{duration_bar.open}\t{duration_bar.high}\t{duration_bar.low}\t{duration_bar.close}")

            if len(sorted_duration_bar_list):
                print("------")
                
            for duration_bar in sorted_duration_bar_list[-10:]:
                dt_str = duration_bar.datetime.strftime(f"%Y-%m-%d %H:%M:%S")
                print_(f"{duration_bar.vt_symbol}({duration_bar.tick_count})\t{duration_bar.open}\t{duration_bar.high}\t{duration_bar.low}\t{duration_bar.close}")
            print_(f"Tick数据合约总数 {len(sorted_duration_bar_list)} 最新 {self.tick.vt_symbol} {self.tick.datetime.replace(microsecond=0)}")

            # 检查交易所连接
            gateway_all_connected = self.check_gateway_connected()
            if not gateway_all_connected and self.gateway_connected:
                msg = f"交易所连接断开"
                self.main_engine.send_ding_talk(msg)
            
            self.gateway_connected = gateway_all_connected
            dt_str = now.strftime(f"%Y-%m-%d %H:%M:%S")
            print(f"{dt_str}\t交易所连接状态：{gateway_all_connected}\n")

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")

def main():
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

    # # 执行策略（HitNew）
    # hit_new_app = HitNewEngine(main_engine=main_engine, event_engine=event_engine)
    # hit_new_app.init_engine()
    # hit_new_app.init_portfolio()
    # hit_new_app.start_portfolio()

    # 执行策略（TrendingSniper）
    trendign_sniper_app = TrendingSniperEngine(main_engine=main_engine, event_engine=event_engine)
    trendign_sniper_app.init_engine()
    trendign_sniper_app.init_portfolio()
    trendign_sniper_app.start_portfolio()
    
if __name__ == "__main__":
    main()
