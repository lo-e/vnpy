from mimetypes import inited
from vnpy.trader.object import BarData
from vnpy.trader.utility import ArrayManager
import numpy as np

class SqueezeMomentum(object):
    def __init__(self, 
                 bb_length:int=20,
                 bb_factor:2.0,
                 kc_length:int=20,
                 kc_factor:1.5) -> None:
        self.bb_length:int = bb_length
        self.bb_factor:int = bb_factor
        self.kc_length: int = kc_length
        self.kc_factor:int = kc_factor

        self.size = max(bb_length, kc_length) + 1
        self.array_manager = ArrayManager(self.size)
    
    def update_bar(self, bar: BarData) -> None:
        if self.array_manager.inited:
            # 计算布林带通道
            basis = np.mean(self.close_array[-self.bb_length:])
            dev = self.bb_factor * np.std(self.close_array[-self.bb_length:])
            upper_bb = basis + dev
            lower_bb = basis - dev

            # 计算肯特纳通道
            ma = np.mean(self.close_array[-self.kc_length:])
            range = useTrueRange ? tr : (high - low)
            rangema = sma(range, lengthKC)
            upperKC = ma + rangema * multKC
            lowerKC = ma - rangema * multKC