# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TutorialItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    name = scrapy.Field()
    email = scrapy.Field()
    age = scrapy.Field()


# yang = TutorialItem(name='yangjian', email='abc@qq.com', age=20)
# print(yang['name'])
