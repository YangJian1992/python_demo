import scrapy


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        urls = [
            'https://www.baidu.com',

        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        title = response.xpath('/html/head/title/text()')
        with open('a.html', 'wb') as file:
            file.write(response.body)
        print(title)