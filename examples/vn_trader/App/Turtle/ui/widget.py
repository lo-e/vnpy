# encoding: UTF-8

'''
TURTLE 和 CTA模块相关的GUI控制组件
'''

from vnpy.event import Event, EventEngine
from vnpy.trader.ui import QtGui, QtCore, QtWidgets

from ..base import EVENT_TURTLE_PORTFOLIO
from vnpy.app.cta_strategy.base import EVENT_CTA_LOG, EVENT_CTA_STRATEGY
from .language import text
from vnpy.trader.engine import MainEngine
from ..base import APP_NAME

########################################################################
class CtaValueMonitor(QtWidgets.QTableWidget):
    """参数监控"""

    # ----------------------------------------------------------------------
    def __init__(self, parent=None):
        """Constructor"""
        super(CtaValueMonitor, self).__init__(parent)

        self.keyCellDict = {}
        self.data = None
        self.inited = False

        self.initUi()

    # ----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setRowCount(1)
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(self.NoEditTriggers)

        self.setMaximumHeight(self.sizeHint().height())

    # ----------------------------------------------------------------------
    def updateData(self, data):
        """更新数据"""
        if not self.inited:
            self.setColumnCount(len(data))
            self.setHorizontalHeaderLabels(data.keys())

            col = 0
            for k, v in data.items():
                cell = QtWidgets.QTableWidgetItem(str(v))
                self.keyCellDict[k] = cell
                self.setItem(0, col, cell)
                col += 1

            self.inited = True
        else:
            for k, v in data.items():
                cell = self.keyCellDict[k]
                cell.setText(str(v))

        # 自动调节宽度
        self.resizeColumnsToContents()


########################################################################
class CtaStrategyManager(QtWidgets.QGroupBox):
    """策略管理组件"""
    signal = QtCore.pyqtSignal(Event)

    # ----------------------------------------------------------------------
    def __init__(self, turtleEngine, eventEngine, name, parent=None):
        """Constructor"""
        super(CtaStrategyManager, self).__init__(parent)

        self.turtleEngine = turtleEngine
        self.eventEngine = eventEngine
        self.name = name

        self.initUi()
        self.updateMonitor()
        self.registerEvent()

    # ----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.paramMonitor = CtaValueMonitor(self)
        self.varMonitor = CtaValueMonitor(self)

        height = 120
        width = 5000
        self.paramMonitor.setFixedHeight(height)
        self.paramMonitor.setFixedWidth(width)
        self.varMonitor.setFixedHeight(height)
        self.varMonitor.setFixedWidth(width)

        buttonInit = QtWidgets.QPushButton(text.INIT)
        buttonStart = QtWidgets.QPushButton(text.START)
        buttonStop = QtWidgets.QPushButton(text.STOP)
        buttonInit.clicked.connect(self.init)
        buttonStart.clicked.connect(self.start)
        buttonStop.clicked.connect(self.stop)

        hbox1 = QtWidgets.QHBoxLayout()
        hbox1.addWidget(buttonInit)
        hbox1.addWidget(buttonStart)
        hbox1.addWidget(buttonStop)
        hbox1.addStretch()

        hbox2 = QtWidgets.QHBoxLayout()
        hbox2.addWidget(self.paramMonitor)

        hbox3 = QtWidgets.QHBoxLayout()
        hbox3.addWidget(self.varMonitor)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(hbox3)

        self.setLayout(vbox)

    # ----------------------------------------------------------------------
    def updateMonitor(self):
        """显示策略最新状态"""
        paramDict = self.turtleEngine.get_strategy_parameters(self.name)
        if paramDict:
            self.paramMonitor.updateData(paramDict)

        varDict = self.turtleEngine.get_strategy_variables(self.name)
        if varDict:
            self.varMonitor.updateData(varDict)

            # ----------------------------------------------------------------------

    def updateVar(self, event):
        """更新组合变量"""
        data = event.data
        variables = data['variables']
        self.varMonitor.updateData(variables)

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.updateVar)
        self.eventEngine.register(EVENT_CTA_STRATEGY + self.name, self.signal.emit)

    # ----------------------------------------------------------------------
    def init(self):
        """初始化策略"""
        self.turtleEngine.initStrategy(self.name)

    # ----------------------------------------------------------------------
    def start(self):
        """启动策略"""
        self.turtleEngine.startStrategy(self.name)

    # ----------------------------------------------------------------------
    def stop(self):
        """停止策略"""
        self.turtleEngine.stopStrategy(self.name)


class TurtlePortfolioManager(QtWidgets.QGroupBox):
    """海龟组合管理组件"""
    signal = QtCore.pyqtSignal(Event)

    # ----------------------------------------------------------------------
    def __init__(self, turtleEngine, eventEngine, turtleManager, parent=None):
        """Constructor"""
        super(TurtlePortfolioManager, self).__init__(parent)

        self.turtleEngine = turtleEngine
        self.eventEngine = eventEngine
        self.turtleManager = turtleManager

        self.strategyLoaded = False

        self.initUi()
        self.registerEvent()

    # ----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setTitle(u'组合管理')

        self.paramMonitor = CtaValueMonitor(self)
        self.varMonitor = CtaValueMonitor(self)

        height = 100
        self.paramMonitor.setFixedHeight(height)
        self.varMonitor.setFixedHeight(height)

        buttonLoad = QtWidgets.QPushButton('加载组合')
        buttonInit = QtWidgets.QPushButton(text.INIT)
        buttonStart = QtWidgets.QPushButton(text.START)
        buttonStop = QtWidgets.QPushButton(text.STOP)
        buttonLoad.clicked.connect(self.load)
        buttonInit.clicked.connect(self.init)
        buttonStart.clicked.connect(self.start)
        buttonStop.clicked.connect(self.stop)

        hbox1 = QtWidgets.QHBoxLayout()
        hbox1.addWidget(buttonLoad)
        hbox1.addWidget(buttonInit)
        hbox1.addWidget(buttonStart)
        hbox1.addWidget(buttonStop)
        hbox1.addStretch()

        hbox2 = QtWidgets.QHBoxLayout()
        hbox2.addWidget(self.paramMonitor)

        hbox3 = QtWidgets.QHBoxLayout()
        hbox3.addWidget(self.varMonitor)

        vbox = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)
        vbox.addLayout(hbox3)

        self.setLayout(vbox)

    # ----------------------------------------------------------------------
    def updateMonitor(self):
        """显示组合最新状态"""
        paramDict = self.turtleEngine.get_portfolio_parameters()
        if paramDict:
            self.paramMonitor.updateData(paramDict)

        varDict = self.turtleEngine.get_portfolio_variables()
        if varDict:
            self.varMonitor.updateData(varDict)

            # ----------------------------------------------------------------------

    def updateVar(self, event):
        """更新策略变量"""
        data = event.dict_['data']
        self.varMonitor.updateData(data)

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.updateVar)
        self.eventEngine.register(EVENT_TURTLE_PORTFOLIO, self.signal.emit)

    # ----------------------------------------------------------------------
    def load(self):
        """加载组合"""
        if not self.strategyLoaded:
            self.turtleEngine.init_engine()
            # 加载组合
            self.updateMonitor()
            # 加载信号
            self.turtleManager.initStrategyManager()

            self.strategyLoaded = True
            self.turtleEngine.write_log(text.STRATEGY_LOADED)

    # ----------------------------------------------------------------------
    def init(self):
        """初始化组合"""
        self.turtleEngine.initPortfolio()

    # ----------------------------------------------------------------------
    def start(self):
        """启动组合"""
        self.turtleEngine.startPortfolio()

    # ----------------------------------------------------------------------
    def stop(self):
        """停止组合"""
        self.turtleEngine.stopPortfolio()


########################################################################
class TurtleManager(QtWidgets.QWidget):
    """CTA引擎管理组件"""
    signal = QtCore.pyqtSignal(Event)

    # ----------------------------------------------------------------------
    def __init__(self, mainEngine: MainEngine, eventEngine: EventEngine, parent=None):
        """Constructor"""
        super(TurtleManager, self).__init__(parent)

        self.turtleEngine = mainEngine.get_engine(APP_NAME)
        self.eventEngine = eventEngine

        self.initUi()
        self.registerEvent()

        # 记录日志
        self.turtleEngine.write_log(text.CTA_ENGINE_STARTED)
        # ----------------------------------------------------------------------

    def initUi(self):
        """初始化界面"""
        self.setWindowTitle(u'海龟交易')

        # 海龟组合
        portfolioManager = TurtlePortfolioManager(self.turtleEngine, self.eventEngine, self)
        portfolioManager.setMaximumHeight(600)

        # 滚动区域，放置所有的CtaStrategyManager
        self.scrollArea = QtWidgets.QScrollArea()
        #self.scrollArea.setWidgetResizable(True)

        # CTA组件的日志监控
        self.ctaLogMonitor = QtWidgets.QTextEdit()
        self.ctaLogMonitor.setReadOnly(True)
        self.ctaLogMonitor.setMaximumHeight(200)

        # 设置布局
        vbox = QtWidgets.QVBoxLayout()
        vbox.addWidget(portfolioManager)
        vbox.addWidget(self.scrollArea)
        vbox.addWidget(self.ctaLogMonitor)
        self.setLayout(vbox)

    # ----------------------------------------------------------------------
    def initStrategyManager(self):
        """初始化策略管理组件界面"""
        w = QtWidgets.QWidget()
        vbox = QtWidgets.QVBoxLayout()

        l = self.turtleEngine.get_strategy_names()
        for name in l:
            strategyManager = CtaStrategyManager(self.turtleEngine, self.eventEngine, name)
            vbox.addWidget(strategyManager)

        vbox.addStretch()

        w.setLayout(vbox)
        self.scrollArea.setWidget(w)

        # ----------------------------------------------------------------------

    def initAll(self):
        """全部初始化"""
        self.turtleEngine.initAll()

        # ----------------------------------------------------------------------

    def startAll(self):
        """全部启动"""
        self.turtleEngine.startAll()

    # ----------------------------------------------------------------------
    def stopAll(self):
        """全部停止"""
        self.turtleEngine.stopAll()

    # ----------------------------------------------------------------------
    def updateCtaLog(self, event):
        """更新CTA相关日志"""
        log = event.data
        content = '\t'.join([str(log.time), log.msg])
        self.ctaLogMonitor.append(content)

    # ----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.updateCtaLog)
        self.eventEngine.register(EVENT_CTA_LOG, self.signal.emit)









