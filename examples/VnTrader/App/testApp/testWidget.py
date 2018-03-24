# encoding: UTF-8

'''
测试App模块GUI控制组件
'''

from vnpy.event import Event
from vnpy.trader.uiQt import QtWidgets, QtCore
from testEngine import EVENT_TESTAPP_LOG

########################################################################
class TestEngineManager(QtWidgets.QWidget):
    """行情数据记录引擎管理组件"""
    signal = QtCore.Signal(type(Event()))

    #----------------------------------------------------------------------
    def __init__(self, testEngine, eventEngine, parent=None):
        """Constructor"""
        super(TestEngineManager, self).__init__(parent)
        
        self.testEngin = testEngine
        self.eventEngine = eventEngine
        
        self.initUi()
        self.registerEvent() 
        
    #----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setWindowTitle('测试App')

        # 订阅按钮
        subscribeButton = QtWidgets.QPushButton(u'订阅行情')
        subscribeButton.clicked.connect(self.testEngin.subscribe)

        # 日志监控
        self.logMonitor = QtWidgets.QTextEdit()
        self.logMonitor.setReadOnly(True)

        hbox = QtWidgets.QHBoxLayout()
        hbox.addWidget(subscribeButton)
        hbox.addStretch()
        
        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.addWidget(self.logMonitor)

        self.setLayout(vbox)

    #----------------------------------------------------------------------
    def updateLog(self, event):
        """更新日志"""
        msg = event.dict_['data']
        self.logMonitor.append(msg)
    
    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.updateLog)
        self.eventEngine.register(EVENT_TESTAPP_LOG, self.signal.emit)
    
    
    
    



    
    