#coding:utf-8
import os
import sys
import gc
import re
import smtplib
from pandas import Series, DataFrame
from email.mime.text import MIMEText
from email.header import Header
import pandas as pd
import time
import datetime
import threading
import webbrowser
from collections import  Iterator
import copy
from urllib import request
from urllib import parse
URL = "https://www.baidu.com"
ua_header = {"User-Agent" : "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;"}
req_obj = request.Request(URL, headers = ua_header)
page = request.urlopen(req_obj).read()
# page = page.decode('utf-8')
print(page)
# data= DataFrame({'a':[469025, 2, 3, 4],
#                  'b': [6, 7, 8, 9],
#                  'c': ['adkkda', 6, 5, 6]},index=['a', 'b', 'c', 'd'])
# b = copy.deepcopy(data)
# def time_count(fun):
#     def fun_2():
#         start = time.time()
#         fun(2, 1, 2, 3, a=4, y=3)
#         print('函数执行所需时间为：%ss'%(time.time()-start))
#     return fun_2
#
# @time_count
# def gg(x, *args, **kwargs):
#     print(type(args))
#     print(x, args, kwargs)
#     return 'python'
#
#
# class Cat():
#     def __init__(self, name, age):
#         self.__name = name
#         self.age = age
#
#     @property
#     def name(self):
#         return self.__name
#     # 使用语法糖可以把方法转化为属性，省去get()和set()方法。但是注意，方法名和属性名不能是一致的，如name和__name。
#
#     @name.setter
#     def name(self, value):
#         if isinstance(value, str):
#             self.__name = value
#         else:
#             print('姓名输入有误')
# a = 1356
# # b = 1356
# t = 'abc'
# a = 'def'
# b = re.match(r'\w{4,20}@(163|126|qq)\.com', '1aabbcc@qq.com')
# print(b.group())