from vnpy.trader.engine import MainEngine
from vnpy.event import EventEngine
from gateway.binance import BinanceUsdtGateway
from gateway.okx import OkxGateway
from vnpy.trader.utility import load_json
from vnpy.trader.object import SubscribeRequest
import time
from vnpy.trader.event import EVENT_TICK
from vnpy.trader.object import TickData
from threading import Thread

class SubscribeEngine(object):
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine

        # 注册事件
        self.event_engine.register(EVENT_TICK, self.on_tick)

    def subscribe(self, vt_symbol: str):
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
        tick: TickData = event.data
        dt = tick.datetime.strftime(f"%Y-%m-%d %H:%M:%S.%f")
        if "BINANCE" in tick.vt_symbol:
            print(f"{tick.vt_symbol} {tick.last_price} {dt}")

if __name__ == "__main__":
    # 引擎
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    subscribe_engine = SubscribeEngine(main_engine, event_engine)

    # 数据库
    main_engine.dbConnect()

    # 连接交易所
    gateways = [OkxGateway, BinanceUsdtGateway]
    for gateway in gateways:
        main_engine.add_gateway(gateway)
        gateway_setting_filename = f"connect_{gateway.gateway_name.lower()}.json"
        connect_setting = load_json(gateway_setting_filename)
        connect_setting = connect_setting.get(f"lo-e")
        main_engine.connect(connect_setting, gateway.gateway_name)

    # 订阅行情
    vt_symbols = ["BTCUSDT.BINANCE", "BTC-USDT-SWAP.OKX"]
    for vt_symbol in vt_symbols:
        Thread(target=subscribe_engine.subscribe, args=(vt_symbol,)).start()
    

