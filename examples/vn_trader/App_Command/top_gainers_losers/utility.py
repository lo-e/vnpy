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

if __name__ == "__main__":
    chrome = Chrome(cta_engine=None)
    Thread(target=chrome.fetch_top_gainers_losers, args=(chrome.on_top_gainers_losers, 10)).start()