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
from urllib import parse
import urllib.request
import operator
from collections import Counter
import logging
import itertools
import matplotlib.pyplot as plt
import numpy as np
import datetime
import dateutil.parser as dp
import pytz
import tkinter
import scrapy
from bs4 import BeautifulSoup
import math
import json
import requests
import pymysql
from dateutil.parser import parse
from functools import reduce
from pyspark.sql.types import *

# a='{"a":3}'
# print(type(json.loads(a)))

# print(DataFrame((data_dict['data']["transportation"][0])['origin']['total_contact_info']))
# print(DataFrame((data_dict['data']["transportation"][0])['operator_callprofile']['total_contact_info']))
# with open('C:\\Users\\QDD\\Desktop\\1.txt', 'r') as file:
#     data = file.read()
# print(type(data))
# print(data)
# print(json.loads(data))
# data = DataFrame([{"a":3, 'b':5}, {"a":3,'b':5}])
# data = DataFrame(data, columns=['a', 'c'])
# print(data
#       )
# print(data.columns)
# print('a' in data.columns)
# ])
# data = [3, 4, 5]
# for i in range(3):
#     data.append(i)
#
# print(data)
# import re
# data = "总数总数:19,银行:1,消费金融:15,P2P理财:2总数:21,银行:1,消费金融:16,P2P理财:2,其它:1"
#
# list = 'xx <aa <bbb> <bbb> aa> yy'
# a=re.findall('(?:总数){2}:', data)
# print()
# if 'abc1' in data:
#     data_list = re.findall('\d+', data)
#     print(type(data_list[0]))
# print(data_list)
# def count_call_time(time_list):
#     if len(time_list) == 3:
#         return (time_list[0]*3600+time_list[1]*60+time_list[2])
#     elif len(time_list) == 2:
#         return time_list[0]*60+time_list[1]
#     elif len(time_list) == 1:
#         return time_list[0]
#     else:
#         print("通话时间数据格式有误，请检查。")
# data = DataFrame()
# print(data.index)
# print(len(data.index))
# print(datetime.datetime.now())
# import time
# t = time.strptime('2017-09-22 08:13:50', '%Y-%m-%d %H:%M:%S')
# print(t)
# print(time.mktime(t))
# a = filter(lambda x:x>4, range(9))
# print(list(a))
# print([x for x in a])
from pyspark import SparkContext

sc = SparkContext('local')
doc = sc.parallelize([['a','b','c'],['b','d','d']])
words = doc.flatMap(lambda d:d).distinct().collect()
word_dict = {w:i for w,i in zip(words,range(len(words)))}
word_dict_b = sc.broadcast(word_dict)

def wordCountPerDoc(d):
    dict={}
    wd = word_dict_b.value
    for w in d:
        if (wd[w]) in dict:
            dict[wd[w]] +=1
        else:
            dict[wd[w]] = 1
    return dict
print(doc.map(wordCountPerDoc).collect())
print("successful!")


