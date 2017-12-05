from urllib import request
import socket
import sys
from bs4 import BeautifulSoup


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
    req_obj = request.Request(URL, headers=headers)
    page = request.urlopen(req_obj)
    #BeautifulSoup()方便你处理html文档
    bs_obj = BeautifulSoup(page)
    # page = page.decode('utf-8')
    print(bs_obj.findAll('a', {'class':"friend-link"}))
    for i in bs_obj.findAll('a', {'class':"friend-link"}):
        print(type(i))
    print(type(bs_obj))

if __name__ == '__main__':
    spider_test()