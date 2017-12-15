# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


class HexunpjtPipeline(object):
    def __init__(self):
        #连接数据库
        self.conn = pymysql.connect(host='localhost', user='root')
    def process_item(self, item, spider):
        return item
