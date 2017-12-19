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
'''
这个文件是用来得到电话邦催收分的测试数据。通话详单的create_time为2017年9月
'''

# for key, value in (data_dict['data']["transportation"][0]).items():
#     print(key, value)
#     print('\n**********************************************\n')
# print("\n\n_______________________________________\n\n")
# for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin'])).items():
#     print(key, value)
#     print('\n**********************************************\n')
# for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin']['call_info'])).items():
#     print(key, value)
#     print('\n**********************************************\n')
# print(((data_dict['data']["transportation"][0]['origin']['call_info']['data'])))
# call_six_monthes = data_dict['data']["transportation"][0]['origin']['call_info']['data']
# print(call_six_monthes)

# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8',  cursorclass=pymysql.cursors.DictCursor)
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)
    data = DataFrame(cur.fetchmany(13000))
    # print(data)
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return data


#生成本地文件，供后续程序使用
def get_local_file():
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\'
    file = 'operator_info_test'
    select_string = '''
SELECT * FROM qiandaodao.operator_info
where left(create_time, 10) between '2017-09-01' and '2017-09-05'
    '''
    # columns_add = ['id', 'user_id', 'mobile', 'name', 'reg_time', 'is_valid', 'id_card', 'create_time', 'data_src', 'skip']
    data = mysql_connection(select_string)
    print('已经从数据库获得数据，正在生成本地文件，请稍候...')
    data.to_csv(path + file + '.csv', sep='\t', encoding='utf-8', index=False)




#读取本地文件
def read_analysis_file():
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\'
    file = 'operator_info_test'
    data_chunk = pd.read_csv(path+file+'.csv', sep='\t', encoding='utf-8', chunksize=1000)
    for data in data_chunk:
        #先把data['data_src']数据转换成字典格式
        fun1 = lambda x : json.dumps(x)
        fun2 = lambda x : json.loads(x)
        # data['data_src'] = data['data_src'].map(fun1)
        # data['data_src'] = data['data_src'].map(fun2)
        call_list = []
        for index in data.index:
            if data.loc[index, 'skip'] == 2:
                data_src = data.loc[index, 'data_src']
                data_src = json.loads(data_src)
                print(type(data_src))
                #该用户六个月的通话记录
                call_six_monthes = data_src['data']["transportation"][0]['origin']['call_info']['data']
                # print(call_six_monthes)
                #把每个月的通话记录，转换成df格式，并放在列表call_list中。
                for item in call_six_monthes:
                    data_item = DataFrame(item['details'])
                    call_list.append(data_item)
        data = pd.concat(call_list, ignore_index=True)
        data.to_csv(path + file + '_1.csv', sep='\t', encoding='utf-8', index=False)
        return -1

# def a():
#     with open('C:\\Users\\QDD\\Desktop\\1.txt', 'r') as file:
#         data = file.read()
#     data_dict = json.loads(data)
#     print((data_dict['data']["transportation"][0]).keys())
#     for key, value in (data_dict['data']["transportation"][0]).items():
#         print(key, value)
#         print('\n**********************************************\n')
#     print("\n\n_______________________________________\n\n")
#     # for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin'])).items():
#     #     print(key, value)
#     #     print('\n**********************************************\n')
#     # for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin']['call_info'])).items():
#     #     print(key, value)
#     #     print('\n**********************************************\n')
#     # print(((data_dict['data']["transportation"][0]['origin']['call_info']['data'])))
#     call_six_monthes = data_dict['data']["transportation"][0]['origin']['call_info']['data']
#     # print(call_six_monthes)
#     call_list = []
#     for item in call_six_monthes:
#         data_item = DataFrame(item['details'])
#         call_list.append(data_item)
#     data = pd.concat(call_list, ignore_index=True)
#
#     return data





if __name__ == '__main__':
    read_analysis_file()
