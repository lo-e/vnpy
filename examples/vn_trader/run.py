# flake8: noqa
import os
from pathlib import Path
from vnpy.trader.utility import DIR_SYMBOL
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from gateway.okx import OkxGateway
from gateway.binance import BinanceUsdtGateway
from gateway.bybit import BybitGateway

from App.Turtle_crypto import TurtleCryptoApp
from App.custom_trading import CustomTradingApp
from App.support_resistance import SupportResistanceApp
import asyncio
import sys

def main():
    """
    强制使用 Selector 事件循环（Windows 专属修复）
    避免使用本地代理报错
    aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host fstream.binance.com:443 ssl:False [参数错误。]
    """
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # 创建引擎
    qapp = create_qapp()
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # Gateways
    main_engine.add_gateway(OkxGateway)
    main_engine.add_gateway(BinanceUsdtGateway)
    main_engine.add_gateway(BybitGateway)

    # Apps
    main_engine.add_app(TurtleCryptoApp)
    main_engine.add_app(CustomTradingApp)
    main_engine.add_app(SupportResistanceApp)
   
    # 监控程序运行状态
    dir = os.getcwd()
    dir_path = Path(dir).joinpath(f"BaiduSyncdisk{DIR_SYMBOL}")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = dir_path.joinpath(f"MONITORING.json")
    main_engine.monitor_updating_file(target_file=file_path)

    # 启动
    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()
    qapp.exec()


if __name__ == "__main__":
    main()