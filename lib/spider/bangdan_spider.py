#!/usr/bin/env python
# coding=utf-8

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

debug = 0

def get_bangdan(dist, url):
    #url = 'https://m.ke.com/sh/bangdan/ibd4'

    list_tmp = []

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
            #将滚动条移动到页面的底部
            #https://blog.csdn.net/weixin_41082042/article/details/79164046
        js="var q=document.documentElement.scrollTop=100000"  
        browser.execute_script(js)  
        time.sleep(5)
        js="var q=document.documentElement.scrollTop=100000"  
        browser.execute_script(js)  
        time.sleep(5)
        js="var q=document.documentElement.scrollTop=100000"  
        browser.execute_script(js)  
        time.sleep(5)   
        html = browser.page_source
    except:
        browser.close()
        browser.quit()
    finally:
        browser.close()
        browser.quit()
        

    soup = BeautifulSoup(html, "lxml")
    house_elements = soup.find_all('div', class_="lj-track li-item")
    print(len(house_elements))


    for house_elem in house_elements:
        house_href = house_elem.find('div', 'info').find('a').get('href')
        house_id = int(house_href[house_href.find('ershoufang') + 11: house_href.find('html') - 1])

        tips = house_elem.find('div', class_='tips')
        print(tips)
        discount = 0
        tips_name = ''
        if tips is not None:
            tips_name = tips.text
            discount = float(tips_name[ tips_name.find('型') + 1 : tips_name.find('%')])


        title_info = house_elem.find('div', class_='title')
        title_name = title_info.text
        title_name = title_name.replace(",", " ")
        title_name = title_name.replace("，", " ")
        title_name = title_name.replace("m²", "m")

        desc_info = house_elem.find('div', class_='desc')
        desc_name = desc_info.text
        desc_name = desc_name.replace(",", " ")
        room = dining = area = 0
        s=desc_name
        t1 = s[ s.find('/')+1:s.find('m²')]
        area = float(t1)
        room = int(s[s.find('室') - 1 : s.find('室')])
        dining = int(s[s.find('厅') - 1 : s.find('厅')])
        desc_name = desc_name[desc_name.rfind('/') + 1 : ]




        price_info = house_elem.find('div', class_='price')
        price_name = price_info.text
        price_name = price_name.replace(",", "")
        total_price = float(price_name[: price_name.find('万')])
        avg_price = float(price_name[price_name.find('万')+1 : price_name.find('元')])

        list_tmp.append([house_id, tips_name, discount, title_name, \
                desc_name, room, dining, area, total_price, avg_price])
        print(house_id, tips_name, discount, title_name, \
                desc_name, room, dining, area, total_price, avg_price)

    dataframe_cols = ['house_id', 'tips_name', 'discount', 'title_name', \
            'desc_name', 'room', 'dining', 'area', 'total_price', 'avg_price']

    df = pd.DataFrame(list_tmp, columns=dataframe_cols)

    nowdate=datetime.datetime.now().date()
    date_str = nowdate.strftime("%Y-%m-%d")
    df['record_date'] = date_str

    df.to_csv('./csv/house_bangdan_'+ dist + '_' + date_str+ '.csv', encoding='gbk')

    return df


if __name__ == '__main__':
    
    districts = ['sh', 'pudong', 'minhang', 'baoshan', 'xuhui', 'putuo', 'yangpu', 'changning', 'songjiang', 'jiading', 'huangpu', 'jingan', 'hongkou', 'qingpu', 'fengxian', 'jinshan', 'chongming', 'shanghaizhoubian']

    #url = 'https://m.ke.com/sh/bangdan/minhang/ibd4'
    url = 'https://m.ke.com/sh/bangdan/ibd4'
    dist_len = len(districts)
    for i in range(0, dist_len):
        dist = districts[i]
        if dist == 'sh':
            #default
            pass
        else:
            url = 'https://m.ke.com/sh/bangdan/'+ dist + '/ibd4'

        print(dist, url)
        df=get_bangdan(dist, url)
