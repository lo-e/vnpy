from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine, Event
from vnpy.trader.gateway import BaseGateway
from gateway.binance import BinanceUsdtGateway
from gateway.bybit import BybitGateway
from gateway.okx import OkxGateway
from vnpy.trader.utility import load_json
from vnpy.trader.object import SubscribeRequest, SubscribeLotsRequest, TickData, ContractData
import time
from vnpy.trader.event import EVENT_TICK, EVENT_TICK_DELAY, EVENT_TIMER
from threading import Thread
from datetime import datetime, timedelta
from copy import copy
from App_Command.hit_new.engine import HitNewEngine
from App_Command.trending_sniper.engine import TrendingSniperEngine
from App_Command.top_gainers_losers.engine import TopGainersLosersEngine
from vnpy.trader.constant import Exchange

# GATEWAYS = [[OkxGateway, "lo-e"], [BybitGateway, "loesuperman"], [BinanceUsdtGateway, "lo-e"]]
# GATEWAYS = [[OkxGateway, "lo-e(test)"], [BybitGateway, "loesuperman(test)"], [BinanceUsdtGateway, "lo-e(test)"]]
GATEWAYS = [[OkxGateway, "lo-e"], [BinanceUsdtGateway, "lo-e"], [BybitGateway, "loesuperman"]]
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
        contract = self.main_engine.get_contract(vt_symbol)
        if contract:
            req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
            self.main_engine.subscribe(req, contract.gateway_name)
            
        else:
            print(f"行情订阅失败，找不到合约{vt_symbol}")

    def subscribe_lots(self, symbols: str, exchange: Exchange):
        # 订阅合约
        gateway_symbols_data = {}
        for symbol in symbols:
            vt_symbol = f"{symbol}.{exchange.value}"
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                gateway_symbols = gateway_symbols_data.get(contract.gateway_name, set())
                gateway_symbols.add(symbol)
                gateway_symbols_data[contract.gateway_name] = gateway_symbols

            else:
                print(f"行情订阅失败，找不到合约{vt_symbol}")

        for gateway_name, gateway_symbols in gateway_symbols_data.items():
            req = SubscribeLotsRequest(symbols=list(gateway_symbols), exchange=exchange)
            self.main_engine.subscribe_lots(req, gateway_name)

    def unsubscribe(self, vt_symbol: str):
        # 取消订阅
        contract = self.main_engine.get_contract(vt_symbol)
        if contract:
            req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)
            self.main_engine.unsubscribe(req, contract.gateway_name)
            
        else:
            print(f"行情订阅失败，找不到合约{vt_symbol}")

    def unsubscribe_lots(self, symbols: str, exchange: Exchange):
        # 订阅合约
        gateway_symbols_data = {}
        for symbol in symbols:
            vt_symbol = f"{symbol}.{exchange.value}"
            contract = self.main_engine.get_contract(vt_symbol)
            if contract:
                gateway_symbols = gateway_symbols_data.get(contract.gateway_name, set())
                gateway_symbols.add(symbol)
                gateway_symbols_data[contract.gateway_name] = gateway_symbols

            else:
                print(f"取消订阅失败，找不到合约{vt_symbol}")

        for gateway_name, gateway_symbols in gateway_symbols_data.items():
            req = SubscribeLotsRequest(symbols=list(gateway_symbols), exchange=exchange)
            self.main_engine.unsubscribe_lots(req, gateway_name)

    def on_tick(self, event):
        # 收到Tick数据
        tick: TickData = event.data
        self.tick = copy(tick)
        tick_dt = tick.datetime.replace(tzinfo=None)
        delay = time.time() - tick_dt.timestamp()
        if delay >= 5 and len(self.history_duration_bar_data):
            # print_(f"Tick数据延迟 {tick.vt_symbol} {delay:.2f}s {tick.datetime}")

            self.tick_delay_count += 1
            if time.time() > self.tick_delay_time + 20:
                msg = f"Tick数据延迟 时长 {delay:.2f}s 数量 {self.tick_delay_count}"
                self.main_engine.send_ding_talk(msg)
                print_(msg)

                self.tick_delay_time = time.time()
                self.tick_delay_count = 0

                # 发出延迟事件
                event = Event(type=EVENT_TICK_DELAY)
                self.event_engine.put(event)

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
        # 交易所延迟信息
        now = datetime.now()
        if now.second == 0:
            gateway: BinanceUsdtGateway = self.main_engine.get_default_gateway("BINANCE")
            if gateway:
                print_(f"交易所延迟 {gateway.rest_api.time_offset}")

        if (now.minute % 1 == 0) and (now.second == 15) and self.tick:
            # 输出Tick信息
            for vt_symbol in self.history_duration_bar_data.copy().keys():
                duration_bar: DurationBar = self.history_duration_bar_data[vt_symbol]
                last_minute_dt = (now - timedelta(minutes=1)).replace(second=0, microsecond=0)
                if duration_bar.datetime < last_minute_dt:
                    self.history_duration_bar_data.pop(vt_symbol)
                    if vt_symbol in self.duration_bar_data:
                        self.duration_bar_data.pop(vt_symbol)

            sorted_duration_bar_list = sorted(self.history_duration_bar_data.values(), key=lambda x: x.tick_count)
            for duration_bar in sorted_duration_bar_list[:5]:
                print_(f"{duration_bar.vt_symbol}({duration_bar.tick_count})\t{duration_bar.open}\t{duration_bar.high}\t{duration_bar.low}\t{duration_bar.close}")

            if len(sorted_duration_bar_list):
                print("------")

            for duration_bar in sorted_duration_bar_list[-10:]:
                print_(f"{duration_bar.vt_symbol}({duration_bar.tick_count})\t{duration_bar.open}\t{duration_bar.high}\t{duration_bar.low}\t{duration_bar.close}")
            print_(f"Tick数据合约总数 {len(sorted_duration_bar_list)} 最新 {self.tick.vt_symbol} {self.tick.datetime.replace(microsecond=0)}")

            # 检查交易所连接
            gateway_all_connected = self.check_gateway_connected()
            if not gateway_all_connected and self.gateway_connected:
                msg = f"交易所连接断开"
                self.main_engine.send_ding_talk(msg)
            
            self.gateway_connected = gateway_all_connected
            print_(f"交易所连接状态：{gateway_all_connected}\n")

def print_(msg: str):
    dt = datetime.now().replace(microsecond=0)
    print(f"{dt}\t{msg}")

def set_leverage(target_gateway_name:str, leverage: int = 20, target: list = [], specials: dict = {}):
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

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
        all_connected = True
        for gateway_info in GATEWAYS:
            gateway_class: BaseGateway = gateway_info[0]
            account_name = gateway_info[1]
            connected = main_engine.get_gateway_connect_status(gateway_class.gateway_name, account_name)

            if not connected:
                all_connected = False
                break

        if all_connected:
            break

        else:
            time.sleep(1)

    # 获取交易所合约数据
    vt_symbols = set()
    if target:
        for token in target:
            if target_gateway_name == "OKX":
                vt_symbols.add(f"{token}-USDT-SWAP.OKX")

            elif target_gateway_name == "BINANCE":
                vt_symbols.add(f"{token}USDT.BINANCE")

            elif target_gateway_name == "BYBIT":
                vt_symbols.add(f"{token}USDT.BYBIT")

    else:
        contracts = main_engine.engines["oms"].contracts
        for key in contracts.keys():
            contract: ContractData = contracts[key]
            if contract.gateway_name == target_gateway_name:
                vt_symbols.add(contract.vt_symbol)
    
    # 设置杠杆
    gateway = main_engine.get_default_gateway(target_gateway_name)
    if gateway:
        count = 0
        for vt_symbol in vt_symbols:
            token = ""
            if target_gateway_name == "OKX":
                token = vt_symbol.split("-USDT")[0]

            elif target_gateway_name == "BINANCE":
                token = vt_symbol.split("USDT")[0]

            elif target_gateway_name == "BYBIT":
                token = vt_symbol.split("USDT")[0]

            if token in specials:
                gateway.set_leverage(vt_symbol, specials[token])
            
            else:
                gateway.set_leverage(vt_symbol, leverage)

            count += 1
            print(f"{vt_symbol}\t{count}")
            time.sleep(1)
    
    print(f"杠杆设置完成！\n合约数：{len(vt_symbols)}")

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
    # trendign_sniper_app = TrendingSniperEngine(main_engine=main_engine, event_engine=event_engine)
    # trendign_sniper_app.init_engine()
    # trendign_sniper_app.init_portfolio()
    # trendign_sniper_app.start_portfolio()

    # 执行策略（TopGainersLosers）
    top_gainers_losers_app = TopGainersLosersEngine(main_engine=main_engine, event_engine=event_engine)
    top_gainers_losers_app.init_engine()
    top_gainers_losers_app.init_portfolio()
    top_gainers_losers_app.start_portfolio()

    # 启动事件循环并保持运行
    while True:
        time.sleep(10)
    
if __name__ == "__main__":
    main()

    # set_leverage(target_gateway_name="OKX", leverage=20, target=[], specials={"BTC": 100, "ETH": 50, "SOL": 50})