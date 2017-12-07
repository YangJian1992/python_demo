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

def district():
    # 既有一级行政区又有二级行政区的数据
    # 其他

    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule"
    # 读取json文件
    with open(path + file_name + '.json', encoding='utf-8') as file:
        data = json.load(file)

    data = Series(data)
    print(data.reset_index().dtypes)
    index_new = []
    for index in data.index:
        index_new.append(int(index))
    #原地址规则文件
    data = DataFrame(data, columns=['addr'])
    print(data)
    data.reset_index(inplace=True)
    print(data)
    # data['id'] = index_new
    # data.to_csv(path + file_name + '.csv', sep='\t', encoding='utf-8', index=False)
    # writer = pd.ExcelWriter(path + file_name + '.xlsx')
    # data.to_excel(writer, 'sheet')
    # writer.save()
    # data_old = pd.read_excel(path + file_name + '_all.xlsx', sheetname=1)
    # print(data_old.dtypes)
    # print(data_old)


# 这是一个测试
if __name__ == '__main__':
    # data = DataFrame([[3, 4, 5], [5, 6, 7], [5, 5, 5]], columns=['a', 'b', 'c'])
    # a = [('john', 'A', 15), ('jane', 'C', 12), ('dave', 'B', 10)]
    # b = ['a', '2', 2, 4, 5, '2', 'b', 4, 7, 'a', 5, 'd', 'a', 'z']
    # f = DataFrame(np.array(data), index=data['a'].values, columns=['a', 'b', 'c'])
    #
    # print(data)
    # print(data.set_index('b',  drop=False))
    district()

