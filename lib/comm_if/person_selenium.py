#!/#!/usr/bin/env python  
# -*- coding: utf-8 -*-


import pandas as pd
import json
import requests
import re

import time
import datetime


from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time, datetime
import pandas as pd
import os
import re
from bs4 import BeautifulSoup

def get_data_by_selenium(url):
    # 添加无头headlesss
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--disable-gpu')
    browser = webdriver.Chrome(chrome_options=chrome_options)

    # browser = webdriver.PhantomJS() # 会报警高提示不建议使用phantomjs，建议chrome添加无头
    browser.maximize_window()  # 最大化窗口
    wait = WebDriverWait(browser, 10)
    
    html = ''
    try: 
        browser.get(url)
        #time.sleep(10)
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()

    return html



if __name__ == '__main__':
    url='https://sh.ke.com/ershoufang/xuhui/'
    html=get_data_by_selenium(url)
    print(html)
