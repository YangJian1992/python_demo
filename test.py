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
from itertools import combinations
from matplotlib.font_manager import FontProperties
font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=15)


# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)

    temp = cur.fetchall()
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return (temp)


def read_data():
    PATH = 'D:\\work\\2018_1_新的风控规则\\'
    file_name = '01-27and28_abnormal'
    data = pd.read_excel(PATH+file_name+'.xlsx', sheetname='data')
    print(data, type(data))
    return list(data['rule_id'].values)

def analysis(rule_tuple):
    # 日期区间是前闭后开的。
    date1 = "'2018-01-27'"
    date2 = "'2018-01-30'"
    rule_range = rule_tuple
    rule_total = 148 - len(rule_range)
    # 四条select语句
    # select_string_0_pass = '''SELECT
    #     COUNT(*)
    # FROM
    #     (SELECT
    #         credit_id
    #     FROM
    #         qiandaodao_risks_control.t_user_credit_model_record
    #     WHERE
    #         update_time BETWEEN {date1} AND {date2}
    #     GROUP BY credit_id
    #     HAVING SUM(IF(auth_result = 20, 1, 0)) = 0
    #         AND COUNT(rule_id) =  148
    #         ) AS b;'''.format(date1=date1, date2=date2)
    # select_string_0_total = '''SELECT
    #         COUNT(*)
    #     FROM
    #         (SELECT
    #             credit_id
    #         FROM
    #             qiandaodao_risks_control.t_user_credit_model_record
    #         WHERE
    #             update_time BETWEEN {date1} AND {date2}
    #         GROUP BY credit_id
    #         HAVING COUNT(rule_id) =  148
    #             ) AS b;'''.format(date1=date1, date2=date2)
    select_string_1_pass = '''SELECT 
        COUNT(*)
    FROM
        (SELECT 
            credit_id
        FROM
            qiandaodao_risks_control.t_user_credit_model_record
        WHERE
            update_time BETWEEN {date1} AND {date2} AND rule_id not in {rule_range}
        GROUP BY credit_id
        HAVING SUM(IF(auth_result = 20, 1, 0)) = 0
            AND COUNT(rule_id) =  {rule_total} 
            ) AS b;'''.format(date1=date1, date2=date2, rule_range=rule_range, rule_total=rule_total)
    select_string_1_total = '''SELECT 
            COUNT(*)
        FROM
            (SELECT 
                credit_id
            FROM
                qiandaodao_risks_control.t_user_credit_model_record
            WHERE
                update_time BETWEEN {date1} AND {date2} AND rule_id not in {rule_range}
            GROUP BY credit_id
            HAVING COUNT(rule_id) =  {rule_total} 
                ) AS b;'''.format(date1=date1, date2=date2, rule_range=rule_range, rule_total=rule_total)

    # data_0_pass表示去规则前，通过的用户数量；data_0_total表示去规则前，运行完148条规则的用户数量；data_1_pass表示去规则后，通过的用户数量；
    # data_1_total表示去规则后，运行完剩余规则的用户数量。
    # data_0_pass = mysql_connection(select_string_0_pass)[0][0]
    # data_0_total = mysql_connection(select_string_0_total)[0][0]
    data_1_pass = mysql_connection(select_string_1_pass)[0][0]
    # data_1_total = mysql_connection(select_string_1_total)[0][0]

    # result0 = data_0_pass/data_0_total
    result1 = round(data_1_pass / 1570, 6)

    star = '*' * 100
    print(star)
    # print('*     去掉规则前，通过率为{result0}, 总数为{data_0_total}, 通过的数量为{data_0_pass}'.
    #       format(result0=result0, data_0_total=data_0_total, data_0_pass=data_0_pass))
    print('*     去掉规则{rule_tuple}后，通过率为{result1}, 总数为{data_1_total}, 通过的数量为{data_1_pass}'.
          format(rule_tuple=rule_tuple,result1=result1, data_1_total=1570, data_1_pass=data_1_pass))
    print(star)
    return result1


def combinations_result(rule_list, times):
    return [x for x in combinations(rule_list, times)]


if __name__ == '__main__':
    start = time.time()
    # 要去掉的规则，放在一个列表里rule_list
    rule_list = read_data()
    result = []
    result_dict = {}
    for x in range(9, 10):
        combinations_list = combinations_result(rule_list, x)
        result1 = {}
        x_count = 0
        for rule_tuple in combinations_list:
            x_count +=1
            result1[rule_tuple] = analysis(rule_tuple)
            if x_count>2000:
                break
        sorted_result_1 = sorted(result1.items(), key=lambda item: item[1], reverse=True)
        print(sorted_result_1)
        data = DataFrame(sorted_result_1)
        for i in range(x):
            data['rule_'+str(i)] = data[0].str[i]
        data['value'] = data[1]
        data.drop([0, 1], axis=1, inplace=True)
        data.to_csv('D:\\work\\2018_1_新的风控规则\\drop_rules_results\\drop_rules_'+str(x)+'.csv', sep=',', encoding='gbk')


    # sorted_result = sorted(result.items(), key=lambda item: item[1], reverse=True)
    # for item in result:
    #     result_dict[item[0]] = item[1]
    # data = pd.Series(result_dict)
    # print(data)
    # data.to_csv('D:\\work\\2018_1_新的风控规则\\delete_rules_result.csv', sep=',', encoding='gbk' )
    # print('\n\n\n总共花费时间为：%d'%(time.time()-start))






