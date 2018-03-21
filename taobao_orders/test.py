import os
import sys
import gc
import re
import smtplib
import xlrd
import  openpyxl
import xlsxwriter
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
import smtplib
import email.mime.multipart
import email.mime.text
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

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
    columns = ['id', 'user_id', 'gender', 'age', 'order_id', 'total_months', 'create_time', 'overdue_status', 'overdue_days', 'overdue_fee', 'pay_status', 'pay_day']
    select_string = '''
    SELECT 
    id,
    eub.user_id,
    ucp.sex as gender,
    convert((100-substring(idcard, -10, 2)+18) , signed) as age,
    eub.order_id,
    total_months,
    eub.create_time,
    overdue_status,
    overdue_days,
    overdue_fee,
    pay_status,
    pay_day
    #sum(monthly_principal)
FROM
    qiandaodao.ecshop_user_bill as eub
    left join qiandaodao.user_credit_profile as ucp on ucp.user_id = eub.user_id
WHERE
    eub.create_time > '2018-01-23'
        AND eub.total_months = 1
        AND eub.pay_day < '2018-03-09'
        and eub.overdue_days = 0;
'''
    data = fun_readdata_mysql(select_string, columns)
    print(data, data.info())
    data_int = data.select_dtypes(include=['int64'])
    for item in data_int.columns:
        data[item] = pd.to_numeric(data[item], downcast='unsigned')
    data_1 = data.groupby('gender').count()
    data_2 = data[data['pay_status']==0].groupby('gender').count()
    age_list=[]
    for age_item in [18, 21, 24, 27, 30]:
        age_list.append({age_item:len(data[(data['age']>age_item)&(data['age']<=(age_item+3))])})
    age_list.append(len(data[(data['age'] > 33) & (data['age']<=35)]))
    age_list.append(len(data[(data['age'] >35)]))

    age_list_1 = []
    data_3 = data[data['pay_status']==0]
    for age_item in [18, 21, 24, 27, 30]:
        age_list_1.append({age_item: len(data_3[(data_3['age'] > age_item) & (data_3['age'] <= (age_item + 3))])})
    age_list_1.append(len(data_3[(data_3['age'] > 33) & (data_3['age'] <= 35)]))
    age_list_1.append(len(data_3[(data_3['age'] > 35)]))

    # data_1 = data[['授信长短', '过往借款', 'Y', 'flag', 'score']]
    # print(data_1.dtypes)
    # data_1['授信长短'] = data_1['授信长短'].map(lambda x: int(x))
    # data_1['Y'] = data_1['Y'].map(lambda x: int(x))
    #
    # for name, group in data_1.groupby(['flag']):
    #     print('\nflag={name}*************************************************'.format(name=name))
    #     score_min = round(group['score'].min(), 0) - 1
    #     score_max = round(group['score'].max(),0) + 1
    #     score_interval = 20
    #     gro_num = (score_max - score_min)//score_interval+1
    #     group['score_interval']=-1
    #     group['score_range'] = "null"
    #     for i in range(1, gro_num+2):
    #         group_score = group[(group['score'] >= score_min+score_interval*(i-1)) & (group['score'] < (score_min+score_interval*i))]
    #         if len(group_score) !=0:
    #             group.ix[group_score.index, 'score_interval'] = i
    #             group.ix[group_score.index, 'score_range'] = "[{a}, {b})".format(a=score_min+score_interval*(i-1),b=score_min+score_interval*(i) )
    #     print(group.groupby(['score_range', 'Y']).count()['score'])
    #     for name_1, group_1 in group.groupby(['score_range']):
    #         num_interval = len(group_1)
    #         for name_2, group_2 in group_1.groupby('Y'):
    #             print("{name_1}, Y_{name_2}, ratio:".format(name_1=name_1, name_2=name_2),num_interval, len(group_2), round(len(group_2)/num_interval*100, 2))
    #         print('\n')


    # data_1_0 = data_1[data_1['flag'] == 0]
    # print(data_1_0)
    # data_1_1 = data_1[data_1['flag'] == 1]
    # print(data_1_1)
    # del data_1_0['flag']
    # del data_1_1['flag']
    # print("flag为0：", data_1_0.corr(method='spearman'))
    # print("\nflag为1：",data_1_1.corr(method='spearman'))

    # for k, group in data.groupby(["授信长短"]):
    #     num = len(group)
    #     for k1, group1 in group.groupby('Y'):
    #         num1 = len(group1)
    #         print("授信长短, Y, num, num1, ratio:", (k, k1, num, num1, round(num1 / num * 100, 2)))
    #
    # for k, group in data.groupby(["过往借款"]):
    #     num = len(group)
    #     for k1, group1 in group.groupby('Y'):
    #         num1 = len(group1)
    #         print("过往借款, Y,  num, num1, ratio:", (k, k1, num, num1, round(num1 / num * 100, 2)))

def fun_3():
    columns = ['id', 'user_id', 'gender', 'age', 'call_num', 'order_id', 'total_months', 'create_time', 'overdue_status', 'overdue_days', 'overdue_fee', 'pay_status', 'pay_day']
    select_string = '''
    SELECT 
    eub.id,
    eub.user_id,
    ucp.sex as gender,
    convert((100-substring(idcard, -10, 2)+18) , signed) as age,
    oabr.three_month_address_book_call_num as call_num,
    eub.order_id,
    total_months,
    eub.create_time,
    eub.overdue_status,
    eub.overdue_days,
    eub.overdue_fee,
    eub.pay_status,
    eub.pay_day
FROM
    qiandaodao.ecshop_user_bill as eub
    left join qiandaodao.user_credit_profile as ucp on ucp.user_id = eub.user_id
    left join qiandaodao.operator_address_book_rule as oabr on oabr.user_id = eub.user_id
WHERE
    eub.create_time > '2018-01-23'
        AND eub.total_months = 1
        AND eub.pay_day < '2018-03-09';
    '''
    data = fun_readdata_mysql(select_string, columns)
    print(data, data.info())
    data['call_num'] = pd.to_numeric(data['call_num'], downcast='float')
    data['cut_call_num'] = 'interval'
    data['cut_call_num'] = pd.cut(data['call_num'], [0, 90, 180, 365, 730, 1095, 20000])

    data_int = data.select_dtypes(include=['int64'])
    for item in data_int.columns:
        data[item] = pd.to_numeric(data[item], downcast='integer')
    for item in ['cut_call_num', 'pay_status', 'overdue_status', 'total_months', 'gender', 'pay_day']:
        data[item] = data[item].astype('category')

    data_cut_counts(data, 'call_num', [0, 5, 15, 30, 60, 90, 120])

#根据逾期天数、还款状态将数据分类， 并对某一规则进行面元划分，统计每一类别下的数量。
def data_cut_counts(data, rule_name, group):
    #未逾期
    data_2 = data[data['overdue_days']==0]
    data_2_cut_days = pd.cut(data_2[rule_name], group)
    count_2 = data_2_cut_days.value_counts()
    count_2.name = '未逾期'
    print('未逾期：\n', count_2)
    #逾期
    data_3 = data[data['overdue_days'] >5]
    data_3_cut_days = pd.cut(data_3[rule_name], group)
    count_3 = data_3_cut_days.value_counts()
    count_3.name = '逾期'
    print('\n逾期：\n', count_3)
    #0表示未还款
    data_4 = data[data['pay_status'] == 0]
    data_4_cut_days = pd.cut(data_4[rule_name], group)
    count_4 = data_4_cut_days.value_counts()
    count_4.name = '未还款'
    print('\n未还款：\n', count_4)
    #1表示已还款
    data_5 = data[data['pay_status'] == 1]
    data_5_cut_days = pd.cut(data_5[rule_name], group)
    count_5 = data_5_cut_days.value_counts()
    count_5.name = '已还款'
    print('\n已还款：\n', count_5)

    result = pd.concat([count_2, count_3, count_4, count_5], axis=1)
    result.insert(2, '未逾期比例', result['未逾期']/(result['未逾期'] + result['逾期']))
    result.insert(3, '逾期比例', result['逾期'] / (result['未逾期'] + result['逾期']))
    result.insert(6, '未还款比例', result['未还款'] / (result['已还款'] + result['未还款']))
    result.insert(7, '已还款比例', result['已还款'] / (result['已还款'] + result['未还款']))
    result.index = result.index.map(lambda x : str(x))
    print(result.index)

    #写入excel
    writer = pd.ExcelWriter('D:\\work\\test.xlsx')
    result.to_excel(writer, '近3个月通话记录中通讯录联系人有通话的联系人数量')
    writer.save()

def age_list(data):
    age_list = []
    for age_item in [18, 22, 26]:
        age_list.append({age_item: len(data[(data['age'] > age_item) & (data['age'] <= (age_item + 4))])})
    age_list.append({30:len(data[(data['age'] > 30) & (data['age'] <= 35)])})
    age_list.append({35:len(data[(data['age'] > 35)])})
    return age_list


if __name__ == "__main__":
    data = pd.read_table('D:\\work\\15.txt', header=0, encoding='utf-8', sep='\t', names=['借款id',	'姓名','身份证',
                        '手机号','放款时间','应还日期',	'借款金额','应还款金额','逾期天数','实还日期','实还金额'])
    data_1 = pd.read_excel('D:\\work\\数据测试公司.xlsx',sheetname= 'Sheet1', header=[0])
    data['id'] = data['姓名'] + data['身份证'].str[-3:]
    data_2 = data[['借款id', '逾期天数', '实还金额', 'id']]

    data_3 = pd.merge(data_1, data_2, on=['id'], how='left')
