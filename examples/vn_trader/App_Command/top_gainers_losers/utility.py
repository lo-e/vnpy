from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from threading import Thread
import time
import os
from vnpy.trader.utility import DIR_SYMBOL
import pandas as pd
from datetime import datetime
from queue import Queue
import socket
from dingtalkchatbot.chatbot import DingtalkChatbot

class Chrome(object):
    def __init__(self, cta_engine) -> None:
        self.cta_engine = cta_engine

    def fetch_top_gainers_losers(self, callback = None, rest: int = 60) -> None:
        driver = None
        driver_reboot = True
        init_fetch = False
        while True:
            try:
                # 启动浏览器
                if driver_reboot:
                    print(f"Chrome启动")
                    self.quit_driver(driver)
                    driver = self.load_driver()

                driver_reboot = False
                url = "https://www.coingecko.com/en/crypto-gainers-losers?time=h1&top=100"
                if not init_fetch:
                    driver.get(url)
                    init_fetch = True
                
                else:
                    driver.refresh()

                tops = driver.find_elements(
                    By.XPATH,
                    "//tbody[@data-view-component='true']",
                )
                top_gainers = tops[0].find_elements(
                    By.XPATH,
                    "tr[@data-view-component='true']",
                )
                top_losers = tops[1].find_elements(
                    By.XPATH,
                    "tr[@data-view-component='true']",
                )
                top_gainers_losers = top_gainers + top_losers

                gainer_list = []
                loser_list = []
                for gl in top_gainers_losers:
                    token = gl.find_element(
                        By.XPATH,
                        "td/a/div/div/div[@data-view-component='true']",
                    ).text

                    try:
                        percent = gl.find_element(
                            By.XPATH,
                            "td/span[@class='gecko-up']",
                        ).text
                        percent = float(percent.split("%")[0])
                        is_gainer = True

                    except Exception as e:
                        pass

                    try:
                        percent = gl.find_element(
                            By.XPATH,
                            "td/span[@class='gecko-down']",
                        ).text
                        percent = float(percent.split("%")[0])*-1
                        is_gainer = False
                        
                    except Exception as e:
                        pass

                    data = {"token": token,
                            "percent": percent}
                    if is_gainer:
                        gainer_list.append(data)
                    
                    else:
                        loser_list.append(data)
                    
                gainer_list.sort(key=lambda x: x["percent"], reverse=True)
                loser_list.sort(key=lambda x: x["percent"], reverse=False)
                result = (gainer_list, loser_list)
                if callback:
                    callback(result)
            
            except Exception as e:
                print(str(e))
            
            time.sleep(rest)

    def fetch_rise_fall_long_short(self, callback = None, rest: int = 60) -> None:
        driver = None
        driver_reboot = True
        while True:
            try:
                # 启动浏览器
                if driver_reboot:
                    print(f"Chrome启动")
                    self.quit_driver(driver)
                    driver = self.load_driver()

                    driver_reboot = False
                    url = "https://www.coinglass.com/zh/gainers-losers"
                    driver.get(url)
                
                else:
                    driver.refresh()
                
                _ = WebDriverWait(driver, timeout=5).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@role='tab']")))
                tab_buttons = driver.find_elements(
                    By.XPATH,
                    "//button[@role='tab']",
                )
                for button in tab_buttons:
                    if "涨跌榜" in button.text:
                        button.click()
                        break

                # 筛选交易所
                exchange_button = None
                buttons = driver.find_elements(
                    By.XPATH,
                    "//div[@class='MuiBox-root cg-style-0']",
                    )
                for button in buttons:
                    if button.text == "交易所":
                        exchange_button = button
                        break

                if exchange_button:
                    exchange_button.click()
                    select_buttons = driver.find_elements(
                        By.XPATH,
                        "//ul/li/ul/li",
                    )
                    for button in select_buttons:
                        exchange = button.text.upper()
                        try_count = 1
                        while not exchange and try_count < 5:
                            time.sleep(0.2)
                            exchange = button.text.upper()
                            try_count += 1
                        if not exchange:
                            continue

                        select_need = False
                        if button.text.upper() in ["BINANCE", "BYBIT", "OKX"]:
                            select_need = True

                        select_show = button.find_elements(
                            By.XPATH,
                            "div/span/span",
                        )[0]
                        selected = "checked" in select_show.get_attribute("class")
                        try_count = 0
                        while selected != select_need and try_count < 5:
                            select_show.click()
                            selected = "checked" in select_show.get_attribute("class")
                            try_count += 1

                        if selected != select_need:
                            continue

                else:
                    continue
                exchange_button.click()

                # 选择30分钟
                select_action = False
                duration_tabs = driver.find_elements(
                    By.XPATH,
                    "//div/div/button[@role='tab']",
                    )
                target_tab = None
                for tab in duration_tabs:
                    if tab.text == "30分钟":
                        target_tab = tab
                        break

                if target_tab:
                    tab_selected = "selected" in target_tab.get_attribute("class")
                    try_count = 0
                    while not tab_selected and try_count < 5:
                        select_action = True
                        target_tab.click()
                        tab_selected = "selected" in target_tab.get_attribute("class")
                        try_count += 1

                    if not tab_selected:
                        continue
                    
                else:
                    continue

                if select_action:
                    time.sleep(1)
                
                # 获取涨跌排行榜
                rise_list = []
                fall_list = []
                row_list = driver.find_elements(
                    By.XPATH,
                    "//tr[@class='rc-table-row rc-table-row-level-0']",
                    )
                for row in row_list:
                    data = self.get_rise_fall_data(row)
                    change = data["change"]
                    if change > 0:
                        rise_list.append(data)
                    
                    elif change < 0:
                        fall_list.append(data)

                # 选择多空比
                down_up_list = []
                up_down_list = []
                for button in tab_buttons:
                    if "人数多空比" in button.text:
                        button.click()
                        break
                
                # 筛选交易所
                bybit_switch = None
                binance_switch = None
                okx_switch = None
                refresh_button = None
                _ = WebDriverWait(driver, timeout=5).until(EC.presence_of_all_elements_located((By.XPATH, "//div/ul/li")))
                switch_buttons = driver.find_elements(
                    By.XPATH,
                    "//div/ul/li",
                )
                for i in range(len(switch_buttons)):
                    button = switch_buttons[i]
                    if button.text.upper() == "BYBIT":
                        bybit_switch = button
                        binance_switch = switch_buttons[i+1]
                        okx_switch = switch_buttons[i+2]
                        refresh_button = switch_buttons[i+3]
                        break

                bybit_show = bybit_switch.find_elements(
                    By.XPATH,
                    "span/span",
                )[0]
                bybit_checked_need = True
                bybit_checked = "checked" in bybit_show.get_attribute("class")
                try_count = 0
                while bybit_checked != bybit_checked_need and try_count < 5:
                    bybit_switch.click()
                    bybit_checked = "checked" in bybit_show.get_attribute("class")
                    try_count += 1

                if bybit_checked != bybit_checked_need:
                    continue
                
                binance_show = binance_switch.find_elements(
                    By.XPATH,
                    "span/span",
                )[0]
                binance_checked_need = False
                binance_checked = "checked" in binance_show.get_attribute("class")
                try_count = 0
                while binance_checked != binance_checked_need and try_count < 5:
                    binance_switch.click()
                    binance_checked = "checked" in binance_show.get_attribute("class")
                    try_count += 1

                if binance_checked != binance_checked_need:
                    continue
                    
                okx_show = okx_switch.find_elements(
                    By.XPATH,
                    "span/span",
                )[0]
                okx_checked_need = False
                okx_checked = "checked" in okx_show.get_attribute("class")
                try_count = 0
                while okx_checked != okx_checked_need and try_count < 5:
                    okx_switch.click()
                    okx_checked = "checked" in okx_show.get_attribute("class")
                    try_count += 1

                if okx_checked != okx_checked_need:
                    continue
                
                # 按小时排序
                _ = WebDriverWait(driver, timeout=5).until(EC.presence_of_all_elements_located((By.XPATH, "//th/div[@class='ant-table-column-sorters']")))
                duration_list = driver.find_elements(
                    By.XPATH,
                    "//th/div[@class='ant-table-column-sorters']",
                )
                target_duration = None
                for duration in duration_list:
                    if "1小时" in duration.text:
                        target_duration = duration
                        break

                up_selected = False
                try_count = 0
                while not up_selected and try_count < 5:
                    try:
                        target_duration.click()
                        caret_up = target_duration.find_elements(
                            By.XPATH,
                            "span/span/span[@aria-label='caret-up']",
                        )[0]

                        caret_up_class = caret_up.get_attribute("class")
                        if "active" in caret_up_class:
                            up_selected = True
                    
                    except Exception as e:
                        pass
                    try_count += 1

                if up_selected:
                    # 获取空到多排行榜
                    row_list = driver.find_elements(
                        By.XPATH,
                        "//tr[@class='ant-table-row ant-table-row-level-0']",
                        )
                    for row in row_list:
                        data = self.get_long_short_data(row)
                        down_up_list.append(data)

                down_selected = False
                try_count = 0
                while not down_selected and try_count < 5:
                    try:
                        target_duration.click()
                        caret_down = target_duration.find_elements(
                            By.XPATH,
                            "span/span/span[@aria-label='caret-down']",
                        )[0]

                        caret_down_class = caret_down.get_attribute("class")
                        if "active" in caret_down_class:
                            down_selected = True
                    
                    except Exception as e:
                        pass
                    try_count += 1

                if down_selected:
                    # 获取多到空排行榜
                    row_list = driver.find_elements(
                        By.XPATH,
                        "//tr[@class='ant-table-row ant-table-row-level-0']",
                        )
                    for row in row_list:
                        data = self.get_long_short_data(row)
                        up_down_list.append(data)

                if callback:
                    callback((rise_list, fall_list, down_up_list, up_down_list))
            
            except Exception as e:
                print(str(e))
            
            time.sleep(rest)

    def get_long_short_data(self, item):
        # 交易所
        exchange = ""
        exchange_key = item.get_attribute("data-row-key")
        if "OKX" in exchange_key.upper():
            exchange = "OKX"

        if "BYBIT" in exchange_key.upper():
            exchange = "BYBIT"

        if "BINANCE" in exchange_key.upper():
            exchange = "BINANCE"

        # 合约
        symbol = item.find_elements(
            By.XPATH,
            "td/div/a/div/div",
            )[0].text

        # 多空比
        rate = item.find_elements(
            By.XPATH,
            "td[@class='ant-table-cell']",
            )[2].text
        rate = float(rate)
        
        # 1小时变化
        change = item.find_elements(
            By.XPATH,
            "td[@class='ant-table-cell ant-table-column-sort']",
            )[0].text
        change = float(change.split("%")[0])

        data = {"symbol": f"{symbol}.{exchange}",
                "rate": rate,
                "change": change}
        
        return data

    def get_rise_fall_data(self, item):
        # 合约
        symbol_item = item.find_elements(
            By.XPATH,
            "td/div/a/div/div",
            )[0]
        symbol = symbol_item.text

        # 涨跌幅
        change = item.find_elements(
            By.XPATH,
            "td[@class='rc-table-cell']",
            )[2].text
        change = float(change.split("%")[0])

        data = {"symbol": f"{symbol}",
                "change": change}
        return data

    def on_top_gainers_losers(self, data: tuple):
        gainers, losers = data
        for i in range(len(gainers) + len(losers)):
            if i < len(gainers):
                is_gainer = True
                data = gainers[i]
            
            else:
                is_gainer = False
                data = losers[i-len(gainers)]
            
            if i == 0:
                print(f"涨幅排行")
            
            if i == len(gainers):
                print(f"\n跌幅排行")

            token = data["token"]
            percent = data["percent"]
            print(f"{token}\t{percent}")

        # 保存到文件
        mean_gainers_percent = pd.DataFrame(gainers)["percent"].mean()
        mean_gainers_data = {"token": "mean_gainers",
                             "percent": mean_gainers_percent}
        
        mean_losers_percent = pd.DataFrame(losers)["percent"].mean()
        mean_losers_data = {"token": "mean_losers",
                            "percent": mean_losers_percent}
        
        gainers.insert(0, mean_losers_data)
        gainers.insert(0, mean_gainers_data)
        losers.insert(0, mean_losers_data)
        losers.insert(0, mean_gainers_data)
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        date = datetime.now().strftime(f"%Y-%m-%d")
        hour = datetime.now().hour
        time = datetime.now().strftime(f"%H_%M_%S")

        gainer_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}gainers{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
        os.makedirs(gainer_dir_path, exist_ok=True)
        gainer_file_path = f"{gainer_dir_path}{DIR_SYMBOL}{time}.csv"
        df = pd.DataFrame(gainers)
        df.to_csv(gainer_file_path, index=False)

        loser_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}losers{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
        os.makedirs(loser_dir_path, exist_ok=True)
        loser_file_path = f"{loser_dir_path}{DIR_SYMBOL}{time}.csv"
        df = pd.DataFrame(losers)
        df.to_csv(loser_file_path, index=False)

    def on_rise_fall_long_short(self, data: tuple):
        rise_list, fall_list, down_up_list, up_down_list = data
        print(f"上涨 {len(rise_list)} 下跌 {len(fall_list)} 多空比递增 {len(down_up_list)} 多空比递减 {len(up_down_list)}")

        if rise_list and fall_list and down_up_list and up_down_list:
            # 保存到文件
            mean_down_up_rate = pd.DataFrame(down_up_list)["rate"].mean()
            mean_down_up_change = pd.DataFrame(down_up_list)["change"].mean()
            mean_down_up_data = {"symbol": "mean_down_up",
                                 "rate": mean_down_up_rate,
                                 "change": mean_down_up_change}
            
            mean_up_down_rate = pd.DataFrame(up_down_list)["rate"].mean()
            mean_up_down_change = pd.DataFrame(up_down_list)["change"].mean()
            mean_up_down_data = {"symbol": "mean_up_down",
                                 "rate": mean_up_down_rate,
                                 "change": mean_up_down_change}
            
            # msg = f"mean_up_down {mean_up_down_change}\nmean_down_up {mean_down_up_change}\n"
            # print(msg)
            # return
            
            down_up_list.insert(0, mean_down_up_data)
            down_up_list.insert(0, mean_up_down_data)
            up_down_list.insert(0, mean_down_up_data)
            up_down_list.insert(0, mean_up_down_data)

            current_dir = os.path.dirname(os.path.abspath(__file__))
            date = datetime.now().strftime(f"%Y-%m-%d")
            hour = datetime.now().hour
            time = datetime.now().strftime(f"%H_%M_%S")

            rise_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_rise{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(rise_dir_path, exist_ok=True)
            rise_file_path = f"{rise_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(rise_list)
            df.to_csv(rise_file_path, index=False)

            fall_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}rank_fall{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(fall_dir_path, exist_ok=True)
            fall_file_path = f"{fall_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(fall_list)
            df.to_csv(fall_file_path, index=False)

            down_up_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}ls_rate_up{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(down_up_dir_path, exist_ok=True)
            down_up_file_path = f"{down_up_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(down_up_list)
            df.to_csv(down_up_file_path, index=False)

            up_down_dir_path = f"{current_dir}{DIR_SYMBOL}data{DIR_SYMBOL}ls_rate_down{DIR_SYMBOL}{date}{DIR_SYMBOL}{hour}"
            os.makedirs(up_down_dir_path, exist_ok=True)
            up_down_file_path = f"{up_down_dir_path}{DIR_SYMBOL}{time}.csv"
            df = pd.DataFrame(up_down_list)
            df.to_csv(up_down_file_path, index=False)

            if abs(mean_up_down_change) >= abs(mean_down_up_change) * 2:
                msg = f"空头过热 准备做多\n\n增幅 {mean_up_down_change}\n降幅 {mean_down_up_change}"
                dingtalk.send_ding_talk(msg)

            if abs(mean_down_up_change) >= abs(mean_up_down_change) * 2:
                msg = f"空头过热 准备做多\n\n增幅 {mean_up_down_change}\n降幅 {mean_down_up_change}"
                dingtalk.send_ding_talk(msg)

            short_msg = ""
            long_msg = ""
            for i in range(2, 12):
                data = up_down_list[i]
                symbol = data["symbol"]
                rate = data["rate"]
                change = data["change"]

                origin_rate = rate / (1 + abs(change) / 100) 
                if origin_rate < 1.0:
                    if not short_msg:
                        short_msg = "趋势做空"
                    short_msg = f"{short_msg}\n{symbol} {rate} {change}%"

                data = down_up_list[i]
                symbol = data["symbol"]
                rate = data["rate"]
                change = data["change"]
                
                origin_rate = rate / (1 + abs(change) / 100) 
                if origin_rate < 1.0:
                    if not long_msg:
                        long_msg = "趋势做多"
                    long_msg = f"{long_msg}\n{symbol} {rate} {change}%"

            if long_msg:
                dingtalk.send_ding_talk(long_msg)
            
            if short_msg:
                dingtalk.send_ding_talk(short_msg)

    def load_driver(self):
        # 加载浏览器
        # 获取当前文件所在路径
        current_dir = os.path.dirname(os.path.abspath(__file__))
        DRIVER_PATH = f"{current_dir}{DIR_SYMBOL}chromedriver/chromedriver.exe"
        if not os.path.exists(DRIVER_PATH):
            DRIVER_PATH = f"{current_dir}{DIR_SYMBOL}chromedriver/chromedriver"
        options = Options()
        # options.add_argument("--headless")
        # options.add_argument("--disable-web-security")
        # options.add_argument("--window-size=1600,1600")
        options.add_argument("--start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        # proxy = f"{LOCAL_IP}:10811"
        # options.add_argument(f'--proxy-server={proxy}')

        driver = webdriver.Chrome(options=options, service=Service(DRIVER_PATH))
        return driver

    def quit_driver(self, driver):
        try:
            driver.quit()

        except:
            pass

class DingTalkEngine(object):
    # 发送钉钉机器人消息
    def __init__(self):
        """"""
        super(DingTalkEngine, self).__init__()

        self.thread = Thread(target=self.run)
        self.queue = Queue()
        self.active = False

    def send_ding_talk(self, content):
        # 内容添加电脑名称、时间
        client = socket.gethostname()
        full_content = f'{content}\n\n【{client}】\n\n{datetime.now()}'

        # 开启线程
        if not self.active:
            self.start()

        # 消息推入队列，做流控处理
        if self.queue.qsize() < 10:
            self.queue.put(full_content)

    def run(self):
        """"""
        while self.active:
            try:
                content = self.queue.get(block=True, timeout=1)

                # 发送消息
                webhook = 'https://oapi.dingtalk.com/robot/send?access_token=c7829ba703a3e0a28fb43f40a65f68313ec3ab43324e5bad30bd2bb660f791e4'
                ding = DingtalkChatbot(webhook)
                ding.send_text(msg=content, is_at_all=True)
            except:
                pass
            time.sleep(2)

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()
        
if __name__ == "__main__":
    chrome = Chrome(cta_engine=None)
    dingtalk = DingTalkEngine()

    # Thread(target=chrome.fetch_top_gainers_losers, args=(chrome.on_top_gainers_losers, 10)).start()
    Thread(target=chrome.fetch_rise_fall_long_short, args=(chrome.on_rise_fall_long_short, 10)).start()