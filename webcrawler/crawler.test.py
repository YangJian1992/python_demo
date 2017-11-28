from urllib import request
import socket


#爬虫的小练习
def spider_test():
    URL = 'https://www.baidu.com'
    headers = {
    'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
    r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
    'Connection': 'keep-alive'
    }
    #设置超时
    timeout = 10
    socket.setdefaulttimeout(timeout)  # 设置超时
    req_obj = request.Request(URL, headers=headers)
    page = request.urlopen(req_obj).read()
    page = page.decode('utf-8')
    print(page)

if __name__ == '__main__':
    spider_test()