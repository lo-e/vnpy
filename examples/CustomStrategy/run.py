# encoding: UTF-8

from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import copy
from CustomEngine import BacktestingEngine
from csv import DictReader
import csv
import os
from collections import OrderedDict
import re
from pymongo import MongoClient, ASCENDING
from vnpy.app.cta_strategy.base import DAILY_DB_NAME
import pandas as pd
from vnpy.trader.constant import Direction, Offset
from vnpy.trader.utility import DIR_SYMBOL

def backtesting():
    engine = BacktestingEngine()
    engine.init()
    engine.setPeriod(datetime(2024, 8, 15), datetime(2024, 8, 20))
    figSavedName = ''
    if figSavedName:
        figSavedName = f'figSaved{DIR_SYMBOL}{figSavedName}'

    filename = 'setting.json'
    engine.initPortfolio(filename, 10000)
    engine.loadData()
    engine.runBacktesting()
    engine.showResult(figSavedName)

if __name__ == '__main__':
    backtesting()