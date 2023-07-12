"""
Defines constants and objects used in CtaStrategy App.
"""

APP_NAME = "Marting"
EVENT_MARTING_PORTFOLIO = 'eMartingPortfolio.'    # 马丁组合状态变化事件

from enum import Enum
class StrategyExecuteMode(Enum):
    # 策略执行模式
    FORWARD_ONLY = "FORWARD_ONLY" # 只进行趋势追踪
    INVERSE_ONLY = "INVERSE_ONLY" # 只进行趋势反转
    FORWARD_INVERSE = "FORWARD_INVERSE" # 同时进行趋势追踪和趋势反转