
from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK, EVENT_TIMER, EVENT_ORDER, EVENT_TRADE)
from vnpy.trader.constant import (Direction, Offset, OrderType)
from vnpy.trader.object import (SubscribeRequest, OrderRequest, LogData)
from vnpy.trader.utility import load_json, save_json, round_to
from vnpy.trader.setting import SETTINGS
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG

from .template import AlgoTemplate
from .algos.best_limit_algo import BestLimitAlgo
from vnpy.app.cta_strategy.template import CtaTemplate

APP_NAME = 'AlgoEngine'

class AlgoEngine(BaseEngine):
    """"""

    def __init__(self, cta_engine:BaseEngine, main_engine: MainEngine, event_engine: EventEngine):
        """Constructor"""
        super().__init__(main_engine, event_engine, APP_NAME)
        self.cta_engine = cta_engine

        self.algos = {}
        self.symbol_algo_map = {}
        self.orderid_algo_map = {}

        self.algo_templates = {}
        self.algo_settings = {}

        self.load_algo_template()
        self.register_event()

    def init_engine(self):
        """"""
        self.write_log("算法交易引擎启动")

    def close(self):
        """"""
        pass

    def load_algo_template(self):
        """"""
        self.add_algo_template(BestLimitAlgo)

    def add_algo_template(self, template: AlgoTemplate):
        """"""
        self.algo_templates[template.__name__] = template

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data

        algos = self.symbol_algo_map.get(tick.vt_symbol, None)
        if algos:
            for algo in algos:
                algo.update_tick(tick)

    def process_timer_event(self, event: Event):
        """"""
        # Generate a list of algos first to avoid dict size change
        algos = list(self.algos.values())

        for algo in algos:
            algo.update_timer()

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data

        algo = self.orderid_algo_map.get(trade.vt_orderid, None)
        if algo:
            algo.update_trade(trade)

    def process_order_event(self, event: Event):
        """"""
        order = event.data

        algo = self.orderid_algo_map.get(order.vt_orderid, None)
        if algo:
            algo.update_order(order)

    def start_algo(self, setting: dict):
        """"""
        template_name: str = setting["template_name"]

        algo_template = self.algo_templates[template_name]

        algo = algo_template.new(self, setting)
        algo.start()

        self.algos[algo.algo_name] = algo
        return algo.algo_name

    def stop_algo(self, algo_name: str):
        """"""
        algo = self.algos.get(algo_name, None)
        if algo:
            algo.stop()
            self.algos.pop(algo_name)

    def stop_all(self):
        """"""
        for algo_name in list(self.algos.keys()):
            self.stop_algo(algo_name)

    def subscribe(self, algo: AlgoTemplate, vt_symbol: str):
        """"""
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f'订阅行情失败，找不到合约：{vt_symbol}', algo)
            return

        algos = self.symbol_algo_map.setdefault(vt_symbol, set())

        """
        if not algos:
            req = SubscribeRequest(
                symbol=contract.symbol,
                exchange=contract.exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)
        """

        algos.add(algo)

    def send_order(
        self,
        algo: AlgoTemplate,
        strategy:CtaTemplate,
        vt_symbol: str,
        direction: Direction,
        price: float,
        volume: float,
        order_type: OrderType,
        offset: Offset
    ):
        orderid_result = self.cta_engine.send_order(
            strategy, direction, offset, price, volume, False, False
        )
        if not orderid_result:
            return ''
        elif isinstance(orderid_result, list):
            vt_orderid = orderid_result[0]
        elif isinstance(orderid_result, str):
            vt_orderid = orderid_result
        else:
            return ''

        self.orderid_algo_map[vt_orderid] = algo
        return vt_orderid

    def cancel_order(self, algo: AlgoTemplate, vt_orderid: str):
        """"""
        self.cta_engine.cancel_order(algo.strategy, vt_orderid)

    def get_tick(self, algo: AlgoTemplate, vt_symbol: str):
        """"""
        tick = self.main_engine.get_tick(vt_symbol)

        if not tick:
            self.write_log(f"查询行情失败，找不到行情：{vt_symbol}", algo)

        return tick

    def get_contract(self, algo: AlgoTemplate, vt_symbol: str):
        """"""
        contract = self.main_engine.get_contract(vt_symbol)

        if not contract:
            self.write_log(f"查询合约失败，找不到合约：{vt_symbol}", algo)

        return contract

    def write_log(self, msg: str, algo: AlgoTemplate = None):
        """"""
        if algo:
            msg = f"{algo.algo_name}：{msg}"

        log = LogData(msg=msg, gateway_name=APP_NAME)
        event = Event(EVENT_CTA_LOG, data=log)
        self.event_engine.put(event)
