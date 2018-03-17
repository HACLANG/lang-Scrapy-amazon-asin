# -*- coding: utf-8 -*-
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import log
import re

import pymysql
import globalvar as GlobalVar

amazon_site = GlobalVar.get_site()
store_id = GlobalVar.get_store_id()

global key, title_table, keywords
key = 0
# 连接数据库
connect = pymysql.Connect(host='localhost', port=3306, user='root', passwd='0123456789', db='amazon', charset='utf8')
# 获取游标
cursor = connect.cursor()


def mysql_get_title() -> list:
    # 查询数据
    try:
        # sql = "SELECT product_title FROM `amazon`.`crawl_store` WHERE id>8 AND id<20"
        sql = "SELECT product_title FROM amazon.crawl_store_"+store_id+" where keywords is null OR keywords='None' OR keywords=''"
        # sql = "SELECT product_title FROM `amazon`.`crawl_store` WHERE id>=4016"
        cursor.execute(sql)
        amazon_title_list = []
        # 获取所有结果
        results = cursor.fetchall()
        # 去除小括号
        for (r,) in results:
            amazon_title_list.append(r)

        # print("5:amazon_title_list" + str(amazon_title_list))
        # print('共查找出', cursor.rowcount, '条数据')
    except Exception as e:
        connect.rollback()  # 事务回滚
        # print('事务处理失败', e)
    else:
        connect.commit()  # 事务提交
        # print('事务处理成功', cursor.rowcount)

    return amazon_title_list


# noinspection PyGlobalUndefined
def mysql_set(keywords, title):
    try:
        up_data = "UPDATE amazon.crawl_store_"+store_id+" SET `keywords`='%s' WHERE product_title ='%s'"
        sql_keywords = str(keywords).replace("'", "").replace("[", "").replace("]", "")
        sql_title_list = str(title).replace("'", "").replace("[", "").replace("]", "")
        # print("6：keywords:" + sql_keywords)
        # print("7：title_list:" + sql_title_list)
        data = (sql_keywords, sql_title_list)
        cursor.execute(up_data % data)
        connect.commit()
        # print('成功修改', cursor.rowcount, '条数据')
    except Exception as e:
        cursor.rollback()  # 事务回滚
        # print('事务处理失败', e)
    else:
        connect.commit()  # 事务提交
        # print('事务处理成功', cursor.rowcount)
    pass


title = str(mysql_get_title()[key])
title_table = mysql_get_title()


class AmazonSpider(Spider):
    name = 'amazon'
    domains = "google."+amazon_site
    allowed_domains = [domains]
    start_urls = ["https://www.google."+amazon_site+"/search?source=hp&q=" + str(title)]

    def parse(self, response):
        global key, keywords, title
        # count = 0
        store_info = response.xpath('//div/div/div/span/em/text()').extract()
        keyword = set(store_info)
        # 筛选关键词一般小于6个词组成的
        keywords = []
        for i in keyword:
            count = 0
            for c in i:
                if c.isspace():
                    count += 1

            if 5 > count >= 1:
                keywords.append(i)

        mysql_set(keywords, title)
        key += 1
        title = str(title_table[key])
        yield Request("https://www.google."+amazon_site+"/search?source=hp&q=" + str(title), callback=self.parse,
                      dont_filter=True)
        pass
