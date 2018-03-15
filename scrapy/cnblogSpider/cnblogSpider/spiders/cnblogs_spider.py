import scrapy
from bs4 import BeautifulSoup
import sys
from scrapy.selector import Selector
sys.path.append("D:\\资料\\githubRepository\\python_demo\\scrapy\\cnblogSpider\\cnblogSpider")
from items import CnblogspiderItem

class CnblogsSpider(scrapy.Spider):
    name = 'cnblogs'
    allowed_domains = ['cnblogs.com']
    #start_requests()读取start_url, 获取初始的Request， 并以parse为回调函数，返回Item、dict、Request或者包含前三者的迭代容器。
    start_urls = ['http://www.cnblogs.com/qiyeboy/default.html?page=1']
    def parse(self, response):
        print(type(response))
        print('***************************************')
        soup = BeautifulSoup(response.text, 'lxml', from_encoding='utf-8')
        soup_list = soup.find_all('a', class_='postTitle2')
        [print(item.string, type(soup)) for item in soup_list]
        papers = response.xpath(".//*[@class='day']")
        for paper in papers:
            url = paper.xpath(".//*[@class='postTitle']/a/@href").extract()[0]
            title = paper.xpath(".//*[@class='postTitle']/a/text()").extract()[0]
            time = paper.xpath(".//*[@class='dayTitle']/a/text()").extract()[0]
            content = paper.xpath(".//*[@class='postCon']/div/text()").extract()[0]
            #下面参数中，等号左边的url成为键key，等号右边的变量url中的内容成为value
            item = CnblogspiderItem(url=url, title=title, time=time, content=content)
            request = scrapy.Request(url=url, callback=self.parse_body)#Request返回response传给callback函数parse_body
            request.meta['item'] = item#将item暂存
            #request: <class 'scrapy.http.request.Request'>
            #response: <class 'scrapy.http.response.html.HtmlResponse'>
            yield request

        next_page = Selector(response).re('<a href="(\S*)">下一页</a>')
        if next_page:
            yield scrapy.Request(url=next_page[0], callback=self.parse)

#这里是每一篇文章的内部文档，用新的函数解析。
    def parse_body(self, response):
        item = response.meta['item']
        soup = BeautifulSoup(response.text, 'lxml', from_encoding='utf-8')
        div_list = soup.find_all('div', class_='postBody')
        image_u = []
        for x in div_list:
            for y in x.descendants:
                if y.name == "img":
                    image_u.append(y['src'])
        #body = response.xpath(".//*[@class='pastBody']")
        item['cimage_urls'] = image_u
        #item['cimage_urls'] = body.xpath('.//img//@src').extract()#提取图片链接
        yield item
