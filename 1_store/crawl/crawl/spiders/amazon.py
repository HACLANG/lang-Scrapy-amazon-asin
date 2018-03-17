# -*- coding: utf-8 -*-
from scrapy.spider import Spider
from scrapy.http import Request
import pymysql
import globalvar as GlobalVar


amazon_site = GlobalVar.get_site()
store_id_table = GlobalVar.get_store_id_table()
store_id = store_id_table
# print(amazon_site)
# print(store_id)

# 连接数据库
connect = pymysql.Connect(host='localhost', port=3306, user='root', passwd='0123456789', db='amazon', charset='utf8')
# 获取游标
cursor = connect.cursor()

def mysql_new_table():
    # 查询数据
    try:
        sql = "CREATE TABLE amazon.crawl_store_"+store_id+""" (
                  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                  `store_name` text COMMENT '店铺名',
                  `landing_page` text COMMENT '加载页面',
                  `product_asin` varchar(50) NOT NULL COMMENT '产品asin',
                  `product_image` text COMMENT '产品图片',
                  `product_title` text COMMENT '产品标题',
                  `product_price` text COMMENT '产品价格',
                  `product_url` text COMMENT '产品url',
                  `keywords` text COMMENT '产品关键字',
                  `search_keyword` text COMMENT '通过此关键词可搜索到',
                  `chinese` text COMMENT '关键字汉化',
                  `amazon_site` char(255) NOT NULL COMMENT '站点',
                  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `product_asin` (`product_asin`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='店铺信息'
                        """
        cursor.execute(sql)
    except Exception as e:
        connect.rollback()  # 事务回滚
        # print('事务处理失败', e)
    else:
        connect.commit()  # 事务提交
        # print('事务处理成功', cursor.rowcount)


mysql_new_table()


class AmazonSpider(Spider):
    name = 'amazon'
    # set delay time 4s
    # download_delay = 4
    allowed_domains = ['amazon.'+amazon_site]

    start_urls = [
        'https://www.amazon.'+amazon_site+'/s/ref=sr_il_ti_merchant-items?me='+store_id+'&rh=i%3Amerchant-items&ie=UTF8&lo=merchant-items'
    ]

    def parse(self, response):
        sel = response
        store_info = sel.xpath('//*[starts-with(@class,"s-result-item")]')
        # print(store_info)

        for a in store_info:
            store_name = a.xpath('//*[@id="s-result-count"]/span/span/text()').extract_first()
            product_asin = a.xpath("@data-asin").extract_first()
            product_image = a.xpath("div/div[2]/div/a/img/@src").extract_first()
            product_title = a.xpath("div/div[3]/div[1]/a/h2/@data-attribute").extract_first()
            product_price = a.xpath("div/div/div/a[@class='a-link-normal a-text-normal']/span/text()").extract_first()
            product_url = a.xpath("div/div[3]/div[1]/a/@href").extract_first()
            landing_page = response.url

            # 获取游标
            cursor = connect.cursor()

            # 插入数据
            sql = "INSERT INTO amazon.crawl_store_"+store_id+" (store_name, landing_page, product_asin, product_image, product_title, product_price, product_url, amazon_site) "" \
            ""VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')"

            try:
                mysql_data = (
                    store_name, landing_page, product_asin, product_image,
                    product_title, product_price, product_url, amazon_site
                )
                cursor.execute(sql % mysql_data)
                connect.commit()
                # print('成功插入', cursor.rowcount, '条数据')
            except Exception as e:
                connect.rollback()  # 事务回滚
                # print('事务处理失败', e)
            else:
                connect.commit()  # 事务提交
                # print('事务处理成功', cursor.rowcount)

        for url in sel.xpath("//a[@id='pagnNextLink']/@href").extract():
            yield Request("https://www.amazon."+amazon_site+url, callback=self.parse)

