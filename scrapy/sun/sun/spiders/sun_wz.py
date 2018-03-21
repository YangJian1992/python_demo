# -*- coding: utf-8 -*-
import scrapy
from sun.items import SunItem
#爬取投诉帖子的编号、帖子的url、帖子的标题，和帖子里的内容
class SunWzSpider(scrapy.Spider):
    name = 'sun_wz'
    allowed_domains = ['wz.sun0769.com']
    start_urls = ['http://wz.sun0769.com/index.php/question/questionType?type=4']

    def parse(self, response):
        article_tr = response.xpath('//div[@id="morelist"]//table[@bgcolor="#FBFEFF"]//tr')
        for tr in article_tr:
            url = tr.xpath('td[2]/a[2]/@href').extract()[0]
            time = tr.xpath('td[5]/text()').extract()[0]
            title = tr.xpath('td[2]/a[2]/@title').extract()[0]
            print(title)
            num = tr.xpath('td[1]/text()').extract()[0]
            #content = scrapy.Field()
            item = SunItem(url=url, time=time, title=title, num=num)
            request = scrapy.Request(url=url, callback=self.parse_content)
            print(item)
            request.meta['item'] = item
            yield request

    def parse_content(self, response):
        item = response.meta['item']
        content = response.xpath('//div[@class="c1 text14_2"]//text()').extract()
        content = '\n'.join(content).strip()
        item['content'] = content
        yield item

