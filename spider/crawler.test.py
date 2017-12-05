from urllib import request
import socket
import sys
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import scrapy


#爬虫的小练习
def spider_test():
    URL = 'https://www.lagou.com'
    headers = {
    'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
    'Connection': 'keep-alive'
    }
    #设置超时
    timeout = 10
    socket.setdefaulttimeout(timeout)  # 设置超时
    # req_obj = request.Request(URL, headers=headers)
    page = urlopen(URL)
    #BeautifulSoup()方便你处理html文档
    bs_obj = BeautifulSoup(page)
    # page = page.decode('utf-8')
    print(bs_obj.findAll('a', {'href' : "http://www.lagou.com/"}))
    print(bs_obj.findAll('a', href=re.compile('^http://www.lagou.com')))

if __name__ == '__main__':
    spider_test()