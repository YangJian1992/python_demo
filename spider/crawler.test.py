from urllib import request
import socket
import sys
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.request import urlretrieve
import requests
import re
import scrapy
import random
import datetime

#在编程目录下运行scrapy startproject project_name，建立scrapy项目目录。


#爬虫的小练习
def spider_test():
    random.seed(datetime.datetime.now())
    URL = 'http://www.loupan.com/'
    URL_2 = 'https://www.baidu.com/'
    headers = {
    'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
    'Connection': 'keep-alive'
    }
    #设置超时
    timeout = 10
    # socket.setdefaulttimeout(timeout)  # 设置超时
    # req_obj = request.Request(URL, headers=headers)
    page = urlopen(URL)
    page_bj = urlopen(URL_2)
    #BeautifulSoup()方便你处理html文档
    bs_obj = BeautifulSoup(page)
    bs_obj_bj = BeautifulSoup(page_bj)
    # page = page.decode('utf-8')
    # print(bs_obj.find('a', {'href' : "http://www.lagou.com/"}))
    urlretrieve('http://public.loupan.com/public/assets_v3/images/gaba.png', 'D:\\work\\1.png')
    # for link in bs_obj.findAll(src=True):
    #     print(link.attrs['src'])
    # print(bs_obj_bj.span.get_text()
    #       )

if __name__ == '__main__':
    spider_test()