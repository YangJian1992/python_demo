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
import shutil
from sklearn.linear_model import LinearRegression
from matplotlib.font_manager import FontProperties
from dateutil.parser import parse

def fun_readdata_mysql(select_string, columns):
    """
        创建连接读取mysql数据：
        select_string:用以筛选数据库数据的语句
    """
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao', passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    print('正在对数据库进行查询，请稍候...')
    cur.execute(select_string)

    temp = DataFrame(list(cur.fetchall()),columns=columns)
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return(temp)

def fun_2():
    columns = ['user_id', 'order_id', 'overdue_days', 'apply_type', '授信长短', 'loan_rate', 'nm', '过往借款', 'overdue', 'tim', 'Y']
    select_string = '''
    select b.user_id,b.order_id,b.overdue_days,cg.apply_type,
if(timestampdiff(month, cg.grant_time,'2018-02-01')=0 ,'0',if(timestampdiff(month, cg.grant_time,'2018-02-01')<=2,'1',
if(timestampdiff(month, cg.grant_time,'2018-02-01')<=5,'2',if(timestampdiff(month, cg.grant_time,'2018-02-01')<=10,'3','4')) )) '授信长短'  ,
cg.loan_rate,
lo.nm,
if(lo.nm is null ,0, if(lo.nm<3,1,if(lo.nm<6,2,if(lo.nm<10,3,4)))) '过往借款' ,


lo.overdue,lo.tim,
if(b.overdue_days=0,'0',if(b.overdue_days>0 and b.overdue_days<=7 and b.pay_status=1,'1','2'  )) 'Y'
from qiandaodao.ecshop_user_bill b 
left join qiandaodao.user_credit_grant cg on cg.user_id=b.user_id
left join (select user_id,count(id) nm ,max(overdue_days) overdue , max(repay_time) tim  from qiandaodao.user_loan_orders where loan_status=2 group by user_id) lo on lo.user_id=b.user_id
left join users u on u.id=b.user_id
where b.create_time>'2018-01-23' and b.total_months=1 and b.pay_day<'2018-02-26' ;
    '''
    data = fun_readdata_mysql(select_string, columns)
    data_1 = data[['授信长短', '过往借款', 'Y']]
    print(data_1.dtypes)
    # data_1['过往借款'] = data_1['过往借款'].map(lambda x: int(x))
    data_1['授信长短'] = data_1['授信长短'].map(lambda x: int(x))
    data_1['Y'] = data_1['Y'].map(lambda x: int(x))
    print(data_1.corr(method='spearman'))

    for k, group in data.groupby(["授信长短"]):
        num = len(group)
        for k1, group1 in group.groupby('Y'):
            num1 = len(group1)
            print("授信长短, Y, num, num1, ratio:", (k, k1, num, num1, round(num1 / num * 100, 2)))

    for k, group in data.groupby(["过往借款"]):
        num = len(group)
        for k1, group1 in group.groupby('Y'):
            num1 = len(group1)
            print("过往借款, Y,  num, num1, ratio:", (k, k1, num, num1, round(num1 / num * 100, 2)))

if __name__ == "__main__":
    fun_2()

