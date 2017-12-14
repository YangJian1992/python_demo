# -*- coding: utf-8 -*-
import scrapy
from tutorial.items import

class BaiduSpider(scrapy.Spider):
    name = 'baidu'
    allowed_domains = ['www.baidu.com']
    start_urls = ['http://www.baidu.com/']

    def parse(self, response):
        item = BaiduItem()
