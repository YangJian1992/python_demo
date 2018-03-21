# -*- coding: utf-8 -*-
import json
from scrapy.exceptions import DropItem
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class CnblogspiderPipeline(object):
    def __init__(self):
        self.file = open('papers.json', 'w')

    def process_item(self, item, spider):
        if item['title']:
            line = json.dumps(dict(item), separators=(',', ':')) + '\n'
            self.file.write(line)
            #self.file.close() 不需要手动关闭file，否则只写入了一个item，其他的item写入不了
            return item
        else:
            raise DropItem("Missing title in %s"% item)



