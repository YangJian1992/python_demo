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

def baidu():
    select_string = '''
        SELECT 
        ucp.idcard,

        IF(b.overdue_days = 0,
            '0',
            IF(b.overdue_days > 0
                    AND b.overdue_days <= 7
                    AND b.pay_status = 1,
                '1',
                '2')) 'Y'

    FROM
        qiandaodao.ecshop_user_bill b
            LEFT JOIN
        qiandaodao.user_credit_grant cg ON cg.user_id = b.user_id
            LEFT JOIN
        (SELECT 
            user_id,
                COUNT(id) nm,
                MAX(overdue_days) overdue,
                MAX(repay_time) tim
        FROM
            qiandaodao.user_loan_orders
        WHERE
            loan_status = 2
        GROUP BY user_id) lo ON lo.user_id = b.user_id
            LEFT JOIN
        users u ON u.id = b.user_id
        left join user_credit_profile as ucp on ucp.user_id = b.user_id
    WHERE
    		b.create_time > '2018-01-23'
            AND b.total_months = 1
            AND b.pay_day < '2018-02-26' 
        '''
    columns = ['idcard', 'Y']
    data_id = fun_readdata_mysql(select_string, columns)

    writer = pd.ExcelFile('D:\\work\\baidu_credit.xlsx')
    data = writer.parse(sheetname='全量数据', header=0)
    for item in ['百度黑产一级关联_关联数', '百度黑产二级关联_关联数', '百度黑产三级关联_关联数', '百度信用分_百度信用分', '百度多头身份证3个月_多头数', '百度多头身份证6个月_多头数',
                 '百度多头手机号3个月_多头数', '百度多头手机号6个月_多头数']:
        data[item] = -1
    data['百度黑产一级关联_关联数'] = data['百度信用查询结果'].str.extract('((?<=百度黑产一级关联_关联数)\s*[:：]\s*\d+)').str.extract('(\d+)').fillna(
        -1).astype('int')
    data['百度黑产二级关联_关联数'] = data['百度信用查询结果'].str.extract('((?<=百度黑产二级关联_关联数)\s*[:：]\s*\d+)').str.extract('(\d+)').fillna(
        -1).astype('int')
    data['百度黑产三级关联_关联数'] = data['百度信用查询结果'].str.extract('((?<=百度黑产三级关联_关联数)\s*[:：]\s*\d+)').str.extract('(\d+)').fillna(
        -1).astype('int')
    data['百度信用分_百度信用分'] = data['百度信用查询结果'].str.extract('((?<=百度信用分_百度信用分)\s*[:：]\s*\d+)').str.extract('(\d+)').fillna(
        -1).astype('int')
    data['百度多头身份证3个月_多头数'] = data['百度信用查询结果'].str.extract('((?<=百度多头身份证3个月_多头数)\s*[:：]\s*\d+)').str.extract(
        '(\d+)').fillna(-1).astype('int')
    data['百度多头身份证6个月_多头数'] = data['百度信用查询结果'].str.extract('((?<=百度多头身份证6个月_多头数)\s*[:：]\s*\d+)').str.extract(
        '(\d+)').fillna(-1).astype('int')
    data['百度多头手机号3个月_多头数'] = data['百度信用查询结果'].str.extract('((?<=百度多头手机号3个月_多头数)\s*[:：]\s*\d+)').str.extract(
        '(\d+)').fillna(-1).astype('int')
    data['百度多头手机号6个月_多头数'] = data['百度信用查询结果'].str.extract('((?<=百度多头手机号6个月_多头数)\s*[:：]\s*\d+)').str.extract(
        '(\d+)').fillna(-1).astype('int')
    del data['Y']
    result = pd.merge(data, data_id, left_on='身份证', right_on='idcard', how='left')
    print(result)
    data_corr = result[['百度信用分_百度信用分', "Y"]]
    data_corr = data_corr[data_corr['百度信用分_百度信用分'] != -1]
    data_corr['Y'] = data_corr['Y'].astype("int")
    data_corr['百度信用分_百度信用分'] = data_corr['百度信用分_百度信用分'].astype("int")
    print(data_corr)
    data_corr = data_corr.corr(method='spearman')
    writer = pd.ExcelWriter('D:\\work\\baidu_credit_result.xlsx')
    result.to_excel(writer, 'yj')
    data_corr.to_excel(writer, 'corr')
    writer.save()
#[手机号三天内多头申请:总数:4,持牌消费金融:1,消费金融:2,P2P理财:1][身份证三天内多头申请:总数:4,持牌消费金融:1,消费金融:2,P2P理财:1][手机号一个月内多头申请:总数:8,银行:1,持牌消费金融:1,消费金融:4,P2P理财:2][身份证一个月内多头申请:总数:8,银行:1,持牌消费金融:1,消费金融:4,P2P理财:2][身份证七天内多头申请:总数:4,持牌消费金融:1,消费金融:2,P2P理财:1][手机号七天内多头申请:总数:4,持牌消费金融:1,消费金融:2,P2P理财:1][身份证三个月内多头申请:总数:21,银行:1,持牌消费金融:2,消费金融:10,P2P理财:7,助贷机构:1][手机号三个月内多头申请:总数:23,银行:1,持牌消费金融:3,消费金融:11,P2P理财:7,助贷机构:1]

def duotou():
    select_string = '''
            SELECT 
            ucp.idcard,

            IF(b.overdue_days = 0,
                '0',
                IF(b.overdue_days > 0
                        AND b.overdue_days <= 7
                        AND b.pay_status = 1,
                    '1',
                    '2')) 'Y'

        FROM
            qiandaodao.ecshop_user_bill b
                LEFT JOIN
            qiandaodao.user_credit_grant cg ON cg.user_id = b.user_id
                LEFT JOIN
            (SELECT 
                user_id,
                    COUNT(id) nm,
                    MAX(overdue_days) overdue,
                    MAX(repay_time) tim
            FROM
                qiandaodao.user_loan_orders
            WHERE
                loan_status = 2
            GROUP BY user_id) lo ON lo.user_id = b.user_id
                LEFT JOIN
            users u ON u.id = b.user_id
            left join user_credit_profile as ucp on ucp.user_id = b.user_id
        WHERE
        		b.create_time > '2018-01-23'
                AND b.total_months = 1
                AND b.pay_day < '2018-02-26' 
            '''
    columns = ['idcard', 'Y']
    data_id = fun_readdata_mysql(select_string, columns)

    writer = pd.ExcelFile('D:\\work\\多头名单查询结果.xlsx')
    data = writer.parse(sheetname='全量数据', header=0)
    for item in [ '身份证三天内多头申请', '手机号三天内多头申请',  '身份证七天内多头申请', '手机号七天内多头申请','身份证一个月内多头申请','手机号一个月内多头申请',
                 '身份证三个月内多头申请', '手机号三个月内多头申请']:
        data[item] = -1
    data['身份证三天内多头申请'] = data['明细'].str.extract('((?<=身份证三天内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['手机号三天内多头申请'] = data['明细'].str.extract('((?<=手机号三天内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['身份证七天内多头申请'] = data['明细'].str.extract('((?<=身份证七天内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['手机号七天内多头申请'] = data['明细'].str.extract('((?<=手机号七天内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['身份证一个月内多头申请'] = data['明细'].str.extract('((?<=身份证一个月内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['手机号一个月内多头申请'] = data['明细'].str.extract('((?<=手机号一个月内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['身份证三个月内多头申请'] = data['明细'].str.extract('((?<=身份证三个月内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    data['手机号三个月内多头申请'] = data['明细'].str.extract('((?<=手机号三个月内多头申请:总数:)\s*\d+)').fillna(-1).astype('int')
    del data['Y']
    result = pd.merge(data, data_id, left_on='身份证', right_on='idcard', how='left')
    print(result)
    writer = pd.ExcelWriter('D:\\work\\多头名单查询结果_result.xlsx')
    result.to_excel(writer, 'yj')
    writer.save()
if __name__ == "__main__":
    duotou()