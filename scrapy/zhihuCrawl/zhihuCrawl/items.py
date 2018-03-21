# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZhihucrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #id
    url = scrapy.Field()
    #头像
    title = scrapy.Field()
    #姓名
    name = scrapy.Field()
    #居住地
    answer = scrapy.Field()
    description = scrapy.Field()



