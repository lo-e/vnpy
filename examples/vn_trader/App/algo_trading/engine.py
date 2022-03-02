
from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK, EVENT_TIMER, EVENT_ORDER, EVENT_TRADE)
from vnpy.trader.constant import (Direction, Offset, OrderType, Interval, Exchange)
from vnpy.trader.object import (SubscribeRequest, OrderRequest, LogData, BarData)
from vnpy.trader.utility import load_json, save_json, round_to
from vnpy.trader.setting import SETTINGS

from .template import AlgoTemplate
from .base import (
    EVENT_ALGO_LOG,
    EVENT_ALGO_PARAMETERS,
    EVENT_ALGO_SETTING,
    EVENT_ALGO_VARIABLES,
    APP_NAME
)

from vnpy.app.cta_strategy.base import POSITION_DB_NAME
from copy import copy
from vnpy.app.cta_strategy.base import (TICK_DB_NAME,
                                        DAILY_DB_NAME,
                                        MINUTE_DB_NAME)
from datetime import datetime, timedelta
from threading import Thread
import traceback
from time import sleep
from ..Turtle_crypto.dataservice import TurtleCryptoDataDownloading

class AlgoEngine(BaseEngine):
    """"""
    setting_filename = "algo_trading_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """Constructor"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.algos = {}

        self.symbol_algo_map = {}
        """ modify by loe """
        self.subscribe_symbols = set()
        self.query_funding_time = 0

        self.orderid_algo_map = {}
        self.orderid_offset_map = {}

        self.algo_templates = {}
        self.algo_settings = {}

        self.load_algo_template()
        self.register_event()

        # 数据引擎
        self.autoEngine = AlgoAutoEngine(main_engine=self.main_engine,
                                         algo_engine=self,
                                         updating_time_list=['00:00:01', '08:00:01', '16:00:01'])

    def init_engine(self):
        """"""
        self.write_log("算法交易引擎启动")
        self.load_algo_setting()

        # 数据引擎启动
        self.autoEngine.start()

    def close(self):
        """"""
        pass

    """ modify by loe """
    def load_bar(self, vt_symbol, days, interval, callback):
        if interval == Interval.DAILY:
            dbName = DAILY_DB_NAME
        elif interval == Interval.MINUTE:
            dbName = MINUTE_DB_NAME
        else:
            dbName = TICK_DB_NAME

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        startDate = today - timedelta(days)
        d = {'datetime': {'$gte': startDate}}

        """ modify by loe """
        collectionName = vt_symbol.upper()
        barData = self.main_engine.dbQuery(dbName, collectionName, d, 'datetime')

        l = []
        for d in barData:
            gateway_name = d['gateway_name']
            symbol = d['symbol']
            exchange = Exchange.BYBIT
            theDatetime = d['datetime']
            endDatetime = None

            bar = BarData(gateway_name=gateway_name, symbol=symbol, exchange=exchange, datetime=theDatetime,
                          endDatetime=endDatetime)
            bar.__dict__ = d
            # 检查Bar数据是否有效
            if not bar.check_valid():
                raise ('Bar数据校验不通过！！')

            if callback:
                callback(bar)
            l.append(bar)
        return l

    def load_algo_template(self):
        """"""
        from .algos.grid_algo import GridAlgo
        from .algos.grid_algo_test import GridAlgoTest
        from .algos.best_limit_algo import BestLimitAlgo

        self.add_algo_template(GridAlgo)
        self.add_algo_template(GridAlgoTest)
        self.add_algo_template(BestLimitAlgo)

    def add_algo_template(self, template: AlgoTemplate):
        """"""
        self.algo_templates[template.__name__] = template

    def load_algo_setting(self):
        """"""
        self.algo_settings = load_json(self.setting_filename)

        for setting_name, setting in self.algo_settings.items():
            self.put_setting_event(setting_name, setting)

        self.write_log("算法配置载入成功")

    def save_algo_setting(self):
        """"""
        save_json(self.setting_filename, self.algo_settings)

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
        # 查询资金费率
        self.query_funding_time += 1
        if self.query_funding_time >= 60*60:
            self.query_funding_time = 0
            self.query_predicted_funding(symbol='BTCUSD')

        algos = list(self.algos.values())

        for algo in algos:
            algo.update_timer()

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data

        algo_array = self.orderid_algo_map.get(trade.vt_orderid, None)
        if algo_array:
            for algo in algo_array:
                algo.update_trade(trade)

    def process_order_event(self, event: Event):
        """"""
        order = event.data

        algo_array = self.orderid_algo_map.get(order.vt_orderid, None)
        if algo_array:
            for algo in algo_array:
                algo.update_order(order)

    def start_algo(self, setting: dict):
        """"""
        template_name: str = setting["template_name"]
        algo_template = self.algo_templates[template_name]

        algo = algo_template.new(self, setting)
        self.loadSyncData(algo=algo)
        if algo.algo_name in self.algos:
            return
        algo.start()

        self.algos[algo.algo_name] = algo
        return algo.algo_name

    """ modify by loe """
    def start_history_algo(self, template_name:str):
        dbData = self.main_engine.dbQuery(POSITION_DB_NAME, template_name, {})
        if not dbData:
            return

        for history_data in dbData:
            if 'setting_data' in history_data:
                setting = history_data['setting_data']
                setting['template_name'] = template_name
                self.start_algo(setting=setting)

    def stop_algo(self, algo_name: str):
        """"""
        algo = self.algos.get(algo_name, None)
        if algo:
            algo.stop()

    def stop_all(self):
        """"""
        for algo_name in list(self.algos.keys()):
            self.stop_algo(algo_name)

    """ modify by loe """
    # ============================
    def on_algo_stop(self, algo_name: str):
        if algo_name in self.algos:
            self.algos.pop(algo_name)

    def on_algo_update(self, algo_name: str):
        if algo_name in self.algos:
            target_algo = self.algos[algo_name]
            for algo in self.algos.values():
                if algo.algo_name != algo_name:
                    algo.on_algo_update(algo=target_algo)

    def reinit_algos(self):
        result = True
        for algo in self.algos.values():
            algo_result = algo.reinit()
            if not algo_result:
                result = False
                break

        return result


    # ============================

    def subscribe(self, algo: AlgoTemplate, vt_symbol: str):
        """"""
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f'订阅行情失败，找不到合约：{vt_symbol}', algo)
            return

        """ modify by loe """
        if vt_symbol not in self.subscribe_symbols:
            self.subscribe_symbols.add(vt_symbol)
            req = SubscribeRequest(
                symbol=contract.symbol,
                exchange=contract.exchange
            )
            self.main_engine.subscribe(req, contract.gateway_name)

        if algo:
            algos = self.symbol_algo_map.setdefault(vt_symbol, set())
            algos.add(algo)

    def send_order(
        self,
        algo: AlgoTemplate,
        vt_symbol: str,
        direction: Direction,
        price: float,
        volume: float,
        order_type: OrderType,
        offset: Offset
    ):
        """"""
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f'委托下单失败，找不到合约：{vt_symbol}', algo)
            return

        volume = round_to(volume, contract.min_volume)
        if not volume:
            return ""

        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=order_type,
            volume=volume,
            price=price,
            offset=offset,
            reference=f"{APP_NAME}_{algo.algo_name}"
        )
        vt_orderid = self.main_engine.send_order(req, contract.gateway_name)

        """ modify by loe """
        algo_array = set()
        algo_array.add(algo)
        if algo.top_algo:
            algo_array.add(algo.top_algo)
        self.orderid_algo_map[vt_orderid] = algo_array
        self.orderid_offset_map[vt_orderid] = offset
        return vt_orderid

    def cancel_order(self, algo: AlgoTemplate, vt_orderid: str):
        """"""
        order = self.main_engine.get_order(vt_orderid)

        if not order:
            self.write_log(f"委托撤单失败，找不到委托：{vt_orderid}", algo)
            return

        req = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    """ modify by loe """
    # ===============================================
    # 新增write_log
    def get_tick(self, algo: AlgoTemplate, vt_symbol: str, write_log=True):
        """"""
        tick = self.main_engine.get_tick(vt_symbol)

        if not tick and write_log:
            self.write_log(f"查询行情失败，找不到行情：{vt_symbol}", algo)

        return tick

    # 订阅并且获取tick
    def get_tick_subscribe(self, algo: AlgoTemplate, vt_symbol: str):
        self.subscribe(algo=None, vt_symbol=vt_symbol)
        tick = self.get_tick(algo=None, vt_symbol=vt_symbol, write_log=False)
        if not tick:
            sleep(1)
            return self.get_tick(algo=None, vt_symbol=vt_symbol)
        return tick

    def query_predicted_funding(self, symbol):
        gateway = self.main_engine.get_gateway('BYBIT')
        if gateway:
            gateway.query_predicted_funding(symbol=symbol)
    # ===============================================

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
        event = Event(EVENT_ALGO_LOG, data=log)
        self.event_engine.put(event)
        """ modify by loe """
        print(msg)

    def put_setting_event(self, setting_name: str, setting: dict):
        """"""
        event = Event(EVENT_ALGO_SETTING)
        event.data = {
            "setting_name": setting_name,
            "setting": setting
        }
        self.event_engine.put(event)

    def update_algo_setting(self, setting_name: str, setting: dict):
        """"""
        self.algo_settings[setting_name] = setting

        self.save_algo_setting()

        self.put_setting_event(setting_name, setting)

    def remove_algo_setting(self, setting_name: str):
        """"""
        if setting_name not in self.algo_settings:
            return
        self.algo_settings.pop(setting_name)

        event = Event(EVENT_ALGO_SETTING)
        event.data = {
            "setting_name": setting_name,
            "setting": None
        }
        self.event_engine.put(event)

        self.save_algo_setting()

    def put_parameters_event(self, algo: AlgoTemplate, parameters: dict):
        """"""
        event = Event(EVENT_ALGO_PARAMETERS)
        event.data = {
            "algo_name": algo.algo_name,
            "parameters": parameters
        }
        self.event_engine.put(event)

    def put_variables_event(self, algo: AlgoTemplate, variables: dict):
        """"""
        event = Event(EVENT_ALGO_VARIABLES)
        event.data = {
            "algo_name": algo.algo_name,
            "variables": variables
        }
        self.event_engine.put(event)

    def loadSyncData(self, algo: AlgoTemplate):
        """从数据库载入算法的持仓情况"""
        flt = {'algo_name': algo.algo_name,
               'vt_symbol': algo.vt_symbol}
        dbData = self.main_engine.dbQuery(POSITION_DB_NAME, algo.__class__.__name__, flt)

        if not dbData:
            return

        sync_data = dbData[0]
        for key in algo.syncs:
            if key in sync_data:
                algo.__setattr__(key, sync_data[key])

    def saveSyncData(self, algo: AlgoTemplate, syncs: list):
        """保存策略的持仓情况到数据库"""
        if not syncs:
            self.dbUpdateCallback()

        flt = {'algo_name': algo.algo_name,
               'vt_symbol': algo.vt_symbol}

        sync_data = copy(flt)
        for key in syncs:
            sync_data[key] = algo.__getattribute__(key)

        self.main_engine.dbUpdate(POSITION_DB_NAME,
                                  algo.__class__.__name__,
                                  sync_data,
                                  flt,
                                  True,
                                  callback=self.dbUpdateCallback)

    def dbUpdateCallback(self, back_data=None):
        try:
            if isinstance(back_data, dict):
                result = back_data.get('result', False)
                algo_name = back_data.get('algo_name', '')
                content = ''
                if result:
                    pass
                    #content = f'算法交易{algo_name}同步数据保存成功。'
                else:
                    content = f'算法交易{algo_name}同步数据保存失败！！'
                if content:
                    self.write_log(content)
            else:
                content = f'算法交易同步数据保存失败！！'
                self.write_log(content)
        except:
            content = f'算法交易同步数据保存失败！！'
            self.write_log(content)

# 数据下载引擎，每天固定时间从数据服务器自动下载策略回测及实盘必要的数据，策略自动重新初始化
class AlgoAutoEngine(object):

    def __init__(self, main_engine:MainEngine, algo_engine:AlgoEngine, updating_time_list:list):
        super(AlgoAutoEngine, self).__init__()
        self.contract_list = ['BTCUSDT', 'ETHUSDT']
        self.main_engine = main_engine
        self.algo_engine = algo_engine
        self.updating_time_list = updating_time_list
        self.downloading = False
        self.updating = False
        self.updating_needed = False
        self.download_timer = Thread(target=self.on_download_timer)
        self.updating_timer = Thread(target=self.on_updating_timer)

    def start(self):
        self.download_timer.start()
        self.updating_timer.start()

    def on_download_timer(self):
        while True:
            try:
                self.checkAndDownload()
            except:
                self.downloading = False
                try:
                    subject = 'ALGO_ENGINE 数据下载'
                    content = f'【未知错误】\n\n{traceback.format_exc()}'
                    self.main_engine.send_ding_talk(content=f'主题\n============\n{subject}\n\n内容\n============\n{content}')
                except:
                    pass

                if self.updating_needed:
                    self.updating = False
                    self.checkAndUpdating()

            sleep(60*60)

    def on_updating_timer(self):
        while True:
            try:
                self.checkAndUpdating()
            except:
                try:
                    subject = 'ALGO_ENGINE 数据更新'
                    content = f'【未知错误】\n\n{traceback.format_exc()}'
                    self.main_engine.send_ding_talk(content=f'主题\n============\n{subject}\n\n内容\n============\n{content}')
                except:
                    pass

                if self.updating_needed:
                    self.updating = False
                    self.checkAndUpdating()
            sleep(1)

    def checkAndDownload(self):
            if not self.downloading:
                self.downloading = True
                turtleCryptoDataD = TurtleCryptoDataDownloading()
                turtleCryptoDataD.download_from_bybit(contract_list=self.contract_list)
                self.downloading = False
                if self.updating_needed:
                    self.updating_needed = False
                    # 策略重新初始化
                    result = self.algo_engine.reinit_algos()
                    subject = 'ALGO_ENGINE 数据更新'
                    if result:
                        content = '算法重新初始化成功'
                    else:
                        content = '算法重新初始化失败!!'

                    try:
                        self.main_engine.send_ding_talk(content=f'主题\n============\n{subject}\n\n内容\n============\n{content}')
                    except:
                        pass

    def checkAndUpdating(self):
        now = datetime.now()
        check = False
        for updating_time in self.updating_time_list:
            start_time = datetime.strptime(f'{now.year}-{now.month}-{now.day} {updating_time}', '%Y-%m-%d %H:%M:%S')
            end_time = start_time + timedelta(seconds=60*5)
            if now >= start_time and now <= end_time:
                check = True
                break

        if check:
            if not self.updating:
                self.updating = True
                self.updating_needed = True
                self.checkAndDownload()
        else:
            self.updating = False
