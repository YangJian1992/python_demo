# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from myxml.items import MyxmlItem

class MyxmlspiderSpider(XMLFeedSpider):
    name = 'myxmlspider'
    allowed_domains = ['sina.com.cn']
    start_urls = ['http://sina.com.cn/feed.xml']
    iterator = 'iternodes' # you can change this; see the docs
    itertag = 'rss' # change it accordingly

    def parse_node(self, response, selector):
        i = MyxmlItem()
        i['title'] = selector.xpath('/rss/channel/item/title/text()')
        #i['url'] = selector.select('url').extract()
        #i['name'] = selector.select('name').extract()
        #i['description'] = selector.select('description').extract()
        return i
