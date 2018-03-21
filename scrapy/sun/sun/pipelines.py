# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import  json

class SunPipeline(object):
    def __init__(self):
        self.file = open('sun.json', 'wb+')

    def process_item(self, item, spider):
        s = json.dumps(dict(item), ensure_ascii=False)+'\n'
        self.file.write(s.encode())
        return item
