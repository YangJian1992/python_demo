from urllib import request
import socket
import sys
from bs4 import BeautifulSoup
import urllib.request
from urllib.request import urlopen
from urllib.request import urlretrieve
import requests
import re
import scrapy

#在编程目录下运行scrapy startproject project_name，建立scrapy项目目录。
#User-Agent:Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36

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
    # print(bs_obj.find('a', {'href' : "http://www.lagou.com/"}))
    print(bs_obj.findAll(src=True))

if __name__ == '__main__':
    url = 'http://www.baidu.com/s?wd='
    keyword = urllib.request.quote('书')
    req = request.Request(url+keyword)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36')
    data = urllib.request.urlopen(req, timeout=3).read()



