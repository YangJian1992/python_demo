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
from sklearn import linear_model
import tensorflow as tf
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from matplotlib.font_manager import FontProperties
font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=15)


data = DataFrame([{'t':'2018-01-08 14:12:26', 'name':'yang'},{'t':'2017-01-08 14:12:26', 'name':'jian'},
                  {'t':'2014-01-08 14:12:26', 'name':'yj'}])
a= range(10)

# with open('C:\\Users\\QDD\\Desktop\\operator\\operator\\17369735519_2018-01-08.json', encoding='utf-8') as file:
#     data = json.load(file)
# for key, item in data.items():
#     print(key, item, '\n')
#
# def w1(func):
#     def inner(a, b):
#         print('两数之积：', a*b)
#         func(a, b)
#     return inner
#
# def mysql_connection(select_string):
#     start = time.time()
#     conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
#                            passwd='qdddba@2017*', db='qiandaodao', charset='utf8',  cursorclass=pymysql.cursors.DictCursor)
#     print('已经连接到数据库，请稍候...')
#     cur = conn.cursor()
#     cur.execute(select_string)
#     data = DataFrame(cur.fetchall())
#     # print(data)
#     print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
#     # 提交
#     conn.commit()
#     # 关闭指针对象
#     cur.close()
#     # 关闭连接对象
#     conn.close()
#     return data
# data_list = []
# name_dict = {"350502":"鲤城区","130826":"丰宁满族自治县","361127": "余干县", "431321":"双峰县", "450126":"宾阳县", "350524":"安溪县",
#              "350525":"永春县", "350526":"德化县",
#              "350521":"惠安县","350581":"石狮市","350582": "晋江市","350583": "南安市",}
# area_code_list = [ "350502","350521", "350581", "350582", "350526", "350583", "130826", "361127", "431321", "450126",  "350524", "350525"]
# for area_code in  area_code_list:
#     select_string= '''
# SELECT
#     ulo.user_id,
#     max(ulo.overdue_days),
#     ulo.loan_status
# FROM
#     user_credit_profile AS ucp
#         inner JOIN
#     user_loan_orders AS ulo ON ucp.user_id = ulo.user_id
# WHERE
# 	ulo.loan_status = 2 and
#     LEFT(ucp.idcard, 6) = '{area_code}'
# GROUP BY ucp.user_id
# '''.format(area_code=area_code)
#     data = mysql_connection(select_string)
#     # data.to_csv('D:\\work\database\\特定地区的用户和身份证前六位\\{area_code}_end.csv'.format(area_code=area_code), sep=',', encoding='gbk', index=False)
#
#     data_dict = {'area_code':area_code, '区域名称':name_dict[area_code], '放款成功人数':len(data),
#                  '逾期天数大于0的人数':len(data[data['max(ulo.overdue_days)']>0]),
#                  '逾期天数大于30的人数':len(data[data['max(ulo.overdue_days)']>30])}
#     print(area_code, len(data[data['max(ulo.overdue_days)']>0]), '\n')
#     data_list.append(data_dict)
# data_2 = DataFrame(data_list)
# data_2.to_csv('D:\\work\database\\特定地区的用户和身份证前六位\\result_end_2.csv', sep=',', encoding='gbk', index=False)


plt.title("散点图练习", fontproperties=font)
plt.bar([1, 2, 3, 4, 5, 6], [2, 3, 4, 6, 9, 12])
plt.xlabel('横坐标', fontproperties=font)
plt.ylabel('纵坐标', fontproperties=font)
plt.legend()
plt.show()
print(np.cov([1, 2, 3, 4, 5, 6], [2, 3, 4, 6, 9, 12]))
print(np.cov([6, 8, 10, 14, 18], [7, 9, 13, 17.5, 18]))



