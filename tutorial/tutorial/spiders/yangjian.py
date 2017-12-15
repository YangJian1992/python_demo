# -*- coding: utf-8 -*-
import scrapy
import pandas as pd

class YangjianSpider(scrapy.Spider):
    name = 'yangjian'
    allowed_domains = ['baidu.com']
    start_urls = ['http://baidu.com/']

    def parse(self, response):
        title = response.xpath('/html/head/title/text()')
        with open('a.html','wb') as file:
            file.write(response.body)
        print(title)
