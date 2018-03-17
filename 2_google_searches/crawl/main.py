# -*- coding: utf-8 -*-
__author__ = 'bobby'

from scrapy.cmdline import execute

import sys
import os

import globalvar as GlobalVar
# php_get_table_name_time = "201709"
# php_get_store_id = "a11rpb3xc6ljye"
# php_get_site = "co.uk"

php_get_store_id = sys.argv[1]
php_get_site = sys.argv[2]

GlobalVar.set_store_id(php_get_store_id)
GlobalVar.set_site(php_get_site)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
execute(["scrapy", "crawl", "amazon"])
