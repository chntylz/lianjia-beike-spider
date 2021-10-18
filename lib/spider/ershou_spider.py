#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 此代码仅供学习与交流，请勿用于商业用途。
# 爬取二手房数据的爬虫派生类

import re
import threadpool
from bs4 import BeautifulSoup
from lib.item.ershou import *
from lib.zone.city import get_city
from lib.spider.base_spider import *
from lib.utility.date import *
from lib.utility.path import *
from lib.zone.area import *
from lib.utility.log import *
import lib.utility.version

from lib.comm_if.person_selenium import *

import pandas as pd


debug=0

call_times = 0

class ErShouSpider(BaseSpider):
    def collect_area_ershou_data(self, city_name, area_name, fmt="csv"):
        """
        对于每个板块,获得这个板块下所有二手房的信息
        并且将这些信息写入文件保存
        :param city_name: 城市
        :param area_name: 板块
        :param fmt: 保存文件格式
        :return: None
        """
        district_name = area_dict.get(area_name, "")
        csv_file = self.today_path + "/{0}_{1}.csv".format(district_name, area_name)
        with open(csv_file, "w", encoding='utf-8') as f:
            # 开始获得需要的板块数据
            ershous = self.get_area_ershou_info(self, city_name, area_name)
            # 锁定，多线程读写
            if self.mutex.acquire(1):
                self.total_num += len(ershous)
                # 释放
                self.mutex.release()
            if fmt == "csv":
                for ershou in ershous:
                    print(self.date_string + "," + ershou.text())
                    f.write(self.date_string + "," + ershou.text() + "\n")
        print("Finish crawl area: " + area_name + ", save data to : " + csv_file)

    @staticmethod
    def get_area_ershou_info(self, city_name, area_name):
        """
        通过爬取页面获得城市指定版块的二手房信息
        :param city_name: 城市
        :param area_name: 版块
        :return: 二手房数据列表
        """
        total_page = 1
        list_tmp = []

        global call_times 

        district_name = area_dict.get(area_name, "")
        # 中文区县
        chinese_district = get_chinese_district(district_name)
        # 中文版块
        chinese_area = chinese_area_dict.get(area_name, "")

        ershou_list = list()
        page = 'http://{0}.{1}.com/ershoufang/{2}/'.format(city_name, SPIDER_NAME, area_name)
        print(page)  # 打印版块页面地址
        html=''
        BaseSpider.random_delay()
        if not BaseSpider.is_selenium():
            headers = create_headers()
            response = requests.get(page, timeout=10, headers=headers)
            html = response.content
        else:
            html = get_data_by_selenium(page)

        soup = BeautifulSoup(html, "lxml")

        # 获得总的页数，通过查找总页码的元素信息
        try:
            page_box = soup.find_all('div', class_='page-box')[0]
            matches = re.search('.*"totalPage":(\d+),.*', str(page_box))
            total_page = int(matches.group(1))
        except Exception as e:
            print("\tWarning: only find one page for {0}".format(area_name))
            print(e)

        # 从第一页开始,一直遍历到最后一页
        for num in range(1, total_page + 1):
            page = 'http://{0}.{1}.com/ershoufang/{2}/pg{3}'.format(city_name, SPIDER_NAME, area_name, num)
            print(page)  # 打印每一页的地址
            html=''
            BaseSpider.random_delay()
            if not BaseSpider.is_selenium():
                headers = create_headers()
                response = requests.get(page, timeout=10, headers=headers)
                html = response.content
            else:
                html = get_data_by_selenium(page)

            if debug:
                print('#########################################################')
                print(html)
                print('#########################################################')

            soup = BeautifulSoup(html, "lxml")

            # 获得有小区信息的panel
            house_elements = soup.find_all('li', class_="clear")
            for house_elem in house_elements:
                price = house_elem.find('div', class_="totalPrice")
                name = house_elem.find('div', class_='title')
                desc = house_elem.find('div', class_="houseInfo")
                pic = house_elem.find('a', class_="img").find('img', class_="lj-lazy")

                # 继续清理数据
                price = price.text.strip()
                name = name.text.replace("\n", "")
                desc = desc.text.replace("\n", "").strip()
                pic = pic.get('data-original').strip()
                # print(pic)


                # 作为对象保存
                ershou = ErShou(chinese_district, chinese_area, name, price, desc, pic)
                if debug:
                    print(chinese_district, chinese_area, name, price, desc, pic)


                #title info
                title_info = house_elem.find('div', class_='title')
                title_a = title_info.find('a')
                title_name = title_a.text
                title_name = title_name.replace(',', ' ')# for iostream to db
                title_href = title_a.get('href')
                #title_href = 'https://sh.ke.com/ershoufang/107104514874.html'
                house_id = int(title_href[title_href.find('ershoufang')+11: title_href.find('html')-1])


                #positon_info
                pos_info = house_elem.find('div', class_='positionInfo')
                pos_a = pos_info.find('a')
                pos_addr = pos_a.text
                pos_addr = pos_addr.replace(',', ' ')# for iostream to db
                pos_href = pos_a.get('href')


                #house info
                house_info = house_elem.find('div', class_="houseInfo")
                house_info = house_info.text.replace("\n", "").strip()
                house_info = house_info.replace(' ', '')
                house_info = house_info.replace(',', ' ')  # for iostream to db

                room = dining = area = 0
                s = house_info
                t1 = s[ :s.find('平米')]
                area = float(t1[ t1.rfind('|') + 1:])
                room = int(s[s.find('室') - 1 : s.find('室')])
                dining = int(s[s.find('厅') - 1 : s.find('厅')])


                #years_info
                years_info = house_elem.find('div', class_="tag")
                years_info = years_info.text.replace("\n", "")

                #price_info
                price_info = house_elem.find('div', class_="priceInfo")
                price_info = price_info.text.replace("\n", "")
                price_info = price_info.replace(",", "")
                total_price = float(price_info[:price_info.find('万')])
                avg_price = float(price_info[price_info.find('万')+1: price_info.find('元')])


                print(call_times, self.date_string,  chinese_district, chinese_area, title_name, title_href, house_id, pos_addr, pos_href, \
                        house_info, area, room, dining, total_price, avg_price)
                list_tmp.append([self.date_string, chinese_district, chinese_area, title_name, title_href, house_id, pos_addr, pos_href, \
                        house_info, area, room, dining,  total_price, avg_price])

                ershou_list.append(ershou)
                
        dataframe_cols = ['record_date', 'district', 'area_part', 'title_name', 'title_href', 'house_id', 'pos_addr', 'pos_href', \
                        'house_info',  'area', 'room', 'dining', 'total_price', 'avg_price']
        df = pd.DataFrame(list_tmp, columns=dataframe_cols)

        df = df.drop_duplicates(subset=['record_date', 'house_id'], keep='first')

        if call_times:
            df.to_csv('./csv/house_info_' + self.date_string + '.csv', mode='a', encoding='utf-8', header=False)
        else:
            df.to_csv('./csv/house_info_' + self.date_string + '.csv', encoding='utf-8')

        call_times  = call_times  + 1
        
        #save to database
        try:
            BaseSpider.random_delay()  #sleep before save data 
            self.hdata_day.copy_from_stringio(df)
        except Exception as e:
            print('################################  ERROR  #################################')
            print(df)
        
        return ershou_list

    def start(self):
        city = get_city()
        self.today_path = create_date_path("{0}/ershou".format(SPIDER_NAME), city, self.date_string)

        t1 = time.time()  # 开始计时

        # 获得城市有多少区列表, district: 区县
        districts = get_districts(city)
        print('City: {0}'.format(city))
        print('Districts: {0}'.format(districts))

        # 获得每个区的板块, area: 板块
        areas = list()
        for district in districts:
            areas_of_district = get_areas(city, district)
            print('{0}: Area list:  {1}'.format(district, areas_of_district))
            # 用list的extend方法,L1.extend(L2)，该方法将参数L2的全部元素添加到L1的尾部
            areas.extend(areas_of_district)
            # 使用一个字典来存储区县和板块的对应关系, 例如{'beicai': 'pudongxinqu', }
            for area in areas_of_district:
                area_dict[area] = district

        if debug:
            print("Area:", areas)
            print("District and areas:", area_dict)

        # 准备线程池用到的参数
        nones = [None for i in range(len(areas))]
        city_list = [city for i in range(len(areas))]
        args = zip(zip(city_list, areas), nones)
        # areas = areas[0: 1]   # For debugging

        if debug:
            print('thread args:', nones, city_list, args)

        # 针对每个板块写一个文件,启动一个线程来操作
        pool_size = thread_pool_size
        pool = threadpool.ThreadPool(pool_size)
        my_requests = threadpool.makeRequests(self.collect_area_ershou_data, args)
        [pool.putRequest(req) for req in my_requests]
        pool.wait()
        pool.dismissWorkers(pool_size, do_join=True)  # 完成后退出

        # 计时结束，统计结果
        t2 = time.time()
        print("Total crawl {0} areas.".format(len(areas)))
        print("Total cost {0} second to crawl {1} data items.".format(t2 - t1, self.total_num))


if __name__ == '__main__':
    pass
