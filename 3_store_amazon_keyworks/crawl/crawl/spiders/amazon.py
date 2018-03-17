# -*- coding: utf-8 -*-
from asn1crypto._ffi import null
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import log
import re

import pymysql
import globalvar as GlobalVar

amazon_site = GlobalVar.get_site()
store_id = GlobalVar.get_store_id()

global key, keywords_table, keyword, keywords_gather, keywords_gather_list, mysql_data
key = 0
# 连接数据库
connect = pymysql.Connect(host='localhost', port=3306, user='root', passwd='0123456789', db='amazon', charset='utf8')
# 获取游标
cursor = connect.cursor()


def mysql_get_keywords() -> list:
    # 查询数据
    try:
        # sql = "SELECT product_title FROM `amazon`.`crawl_store` WHERE id>8 AND id<20"
        sql = "SELECT keywords FROM amazon.crawl_store_" + store_id + " where keywords is not null AND keywords!='None' AND keywords!='';"
        # sql = "SELECT product_title FROM `amazon`.`crawl_store` WHERE id>=4016"
        cursor.execute(sql)
        amazon_keywords_list = []
        # 获取所有结果
        results = cursor.fetchall()
        # 去除小括号
        for (r,) in results:
            amazon_keywords_list.append(r)

        #  #  #  #  #  #  #
        sql1 = "SELECT search_keyword from amazon.crawl_store_" + store_id + " where search_keyword is not null AND search_keyword!='None' AND search_keyword!='';"
        cursor.execute(sql1)
        search_keyword_list = []
        # 获取所有结果
        results = cursor.fetchall()
        # 去除小括号
        for (r,) in results:
            search_keyword_list.append(r)
        #  #  #  #  #  #  #  #

        amazon_keywords_list1 = list(set(amazon_keywords_list) - set(search_keyword_list))
        # print("5:amazon_title_list" + str(amazon_keywords_list))
        print('共查找出', cursor.rowcount, '条数据')
    except Exception as e:
        connect.rollback()  # 事务回滚
        # print('事务处理失败', e)
    else:
        connect.commit()  # 事务提交
        # print('事务处理成功', cursor.rowcount)

    return amazon_keywords_list1


# noinspection PyGlobalUndefined
def mysql_set(mysql_data):
    # 插入数据
    sql = "INSERT INTO amazon.crawl_store_" + store_id + " (store_name, landing_page, product_asin, product_image, product_title, product_price, product_url, search_keyword, amazon_site) "" \
    ""VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    try:
        cursor.execute(sql % mysql_data)
        connect.commit()
        # print('成功插入', cursor.rowcount, '条数据')
    except Exception as e:
        connect.rollback()  # 事务回滚
        # print('事务处理失败', e)
    else:
        connect.commit()  # 事务提交
        # print('事务处理成功', cursor.rowcount)
    pass


keywords_table = mysql_get_keywords()
lang = []
for k in keywords_table:
    lang.append(str(k))
keywords_gather = str(lang).replace("'", "").replace("\"", "").replace("[", "").replace("]", "")
keywords_gather_list = keywords_gather.split(", ")
keyword = str(keywords_gather_list[key])
mysql_data = []


class AmazonSpider(Spider):
    name = 'amazon'
    allowed_domains = ['amazon.' + amazon_site]
    start_urls = [
        "https://www.amazon." + amazon_site + "/s/ref=nb_sb_noss?me=" + store_id + "&field-keywords=" + str(keyword)]

    def parse(self, response):
        global key, keywords_table, lang, keywords_gather, keywords_gather_list, mysql_data, z, keyword
        z = 0
        # print("666lang =" + keywords_gather)
        # print("666666keywords_gather_list" + str(keywords_gather_list))
        sel = response
        store_info = sel.xpath('//*[starts-with(@class,"s-result-item")]')
        # print(store_info)
        # search_keyword = str(keyword)
        for a in store_info:
            # z = 0
            store_name = a.xpath('//*[@id="s-result-count"]/span/a/text()').extract_first()
            product_asin = a.xpath("@data-asin").extract_first()
            product_image = a.xpath("div/div[2]/div/div/a/img/@src").extract_first()
            product_title = a.xpath("div/div[3]/div[1]/a/h2/@data-attribute").extract_first()
            product_price = a.xpath("div/div/div/a[@class='a-link-normal a-text-normal']/span/text()").extract_first()
            product_url = a.xpath("div/div[3]/div[1]/a/@href").extract_first()
            landing_page = response.url
            search_keyword0 = str(landing_page)
            re1 = r"([^=]+)$"
            # pattern1 = re.compile(re1)  # 编译
            # search_keyword = re.search(pattern1, search_keyword0)  # 查询
            search_keyword00 = re.findall(re1, search_keyword0)
            search_keyword = str(search_keyword00).replace("%20", " ").replace("'", "").replace("\"", "").replace("[", "").replace("]", "")
            # print(search_keyword)

            mysql_data = (
                store_name, landing_page, product_asin, product_image,
                product_title, product_price, product_url, search_keyword, amazon_site
            )
            mysql_set(mysql_data)

            key += 1
            # print("key===" + str(key))
            keyword = str(keywords_gather_list[key])
            yield Request("https://www.amazon." + amazon_site + "/s/ref=nb_sb_noss?me=" + store_id + "&field-keywords="
                          + str(keyword), callback=self.parse, dont_filter=True)

        next_page = sel.xpath("//a[@id='pagnNextLink']/@href").extract_first()
        if next_page is not None:
            next_full_url = response.urljoin(next_page)
            # 把Scrapy的自动去重机制关掉：callback = self.parse_author, dont_filter = True
            yield Request(next_full_url, callback=self.parse, dont_filter=True)
