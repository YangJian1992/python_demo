# -*- coding: utf-8 -*-
import scrapy

class YangjianSpider(scrapy.Spider):
    name = 'yangjian'
    allowed_domains = ['baidu.com']
    start_urls = ['http://www.baidu.com/', 'http://scrapy-chs.readthedocs.io/zh_CN/1.0/intro/tutorial.html#id2']

    def parse(self, response):
        title = response.xpath('/html/head/title/text()').extract()
        with open('1.txt','w') as file:
            file.write(title[0])
        print(title)

    
