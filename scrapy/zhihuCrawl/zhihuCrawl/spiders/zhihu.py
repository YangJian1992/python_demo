# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.linkextractor import LinkExtractor
from scrapy import Request, FormRequest
from zhihuCrawl.items import ZhihucrawlItem

class ZhihuSpider(CrawlSpider):
    name = 'zhihu'
    allowed_domains = ['baidu.com']
    start_urls = ['https://www.zhihu.com/signup?next=%2F']
    page_1 = LinkExtractor(allow=('/question/\d+#.*?',))
    page_2 = LinkExtractor(allow=('/question/\d+',))
    rules = (
        Rule(page_1, callback='parse_page', follow=True),
        Rule(page_2, callback='parse_page', follow=True)
    )
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip,deflate",
        "Accept-Language": "en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4",
        "Connection": "keep-alive",
        "Content-Type": " application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
        "Referer": "http://www.zhihu.com/"
    }

    def start_requests(self):
        return [Request("https://www.zhihu.com/signup?next=%2F",method="POST", meta={'cookiejar': 1}, callback=self.post_login)]

    def post_login(self, response):
        print('preparing loggin...')
        ##下面这句话用于抓取请求网页后返回网页中的_xsrf字段的文字, 用于成功提交表单
        #xsrf = response.xpath('//input[@name="_xsrf"]/@value').extract()[0]
        #print(xsrf)
        # FormRequeset.from_response是Scrapy提供的一个函数, 用于post表单
        # 登陆成功后, 会调用after_login回调函数
        #为什么还是get请求，FormRequest.from_response不应该是post请求么？
        return [FormRequest.from_response(response,  # "http://www.zhihu.com/login",
                                          meta={'cookiejar': response.meta['cookiejar']},
                                          headers=self.headers,  # 注意此处的headers
                                          formdata={
                                              #'_xsrf': xsrf,
                                              'username': '17600364145',
                                              'password': '19925555yj'
                                          },
                                          method='POST',
                                          callback=self.after_login,
                                          dont_filter=True
                                          )]

    def after_login(self, response):
        for url in self.start_urls:
            yield self.make_requests_from_url(url)


    def parse(self, response):
        problem = Selector(response)
        item = ZhihucrawlItem()
        item['url'] = response.url
        item['name'] = problem.xpath('//span[@class="name"]/text()').extract()
        print(item['name'])
        item['title'] = problem.xpath('//h2[@class="zm-item-title zm-editable-content"]/text()').extract()
        item['description'] = problem.xpath('//div[@class="zm-editable-content"]/text()').extract()
        item['answer'] = problem.xpath('//div[@class=" zm-editable-content clearfix"]/text()').extract()
        return item
