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
import smtplib
import email.mime.multipart
import email.mime.text
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

#mysql函数，得到数据库的数据
def mysql_data(select_string, columns):
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

#发邮件
def send_email(smtpHost,port, sendAddr, password, recipientAddrs, subject='', content=''):
    msg = email.mime.multipart.MIMEMultipart()
    msg['from'] = sendAddr
    msg['to'] = recipientAddrs#多个收件人的邮箱应该放在字符串中,用字符分隔, 然后用split()分开,不能放在列表中, 因为要使用encode属性
    msg['subject'] = subject
    content = content
    txt = email.mime.text.MIMEText(content, 'plain', 'utf-8')
    msg.attach(txt)
    print("准备添加附件...")
    # 添加附件，从本地路径读取。如果添加多个附件，可以定义part_2,part_3等，然后使用part_2.add_header()和msg.attach(part_2)即可。
    part = MIMEApplication(open('C:\\Users\\Administrator\\Desktop\\test.xlsx','rb').read())
    part.add_header('Content-Disposition', 'attachment', filename="test.xlsx")#给附件重命名,一般和原文件名一样,改错了可能无法打开.
    msg.attach(part)
    smtp = smtplib.SMTP_SSL(smtpHost, port)#需要一个安全的连接，用SSL的方式去登录得用SMTP_SSL，之前用的是SMTP（）.端口号465或587
    smtp.login(sendAddr, password)#发送方的邮箱，和授权码（不是邮箱登录密码）
    smtp.sendmail(sendAddr, recipientAddrs.split(";"), str(msg))#注意, 这里的收件方可以是多个邮箱,用";"分开, 也可以用其他符号
    print("发送成功！")
    smtp.quit()


def data_analysis():
    PATH = 'D:\\work\\loan_rules'
    # 此刻时间点和七天前的时间点，按自然日计算
    time_stamp = time.gmtime(time.time() + 8 * 3600)#按日期处理，今天是2018-03-05， 应该小于等于2018-03-06
    time_stamp_latest = time.gmtime(time.time() + 8*3600 + 24*3600)
    time_stamp_2 = time.gmtime(time.time() + 8 * 3600- 6 * 24 * 3600)#不是七，是六，按自然日计算
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time_stamp)[:10]
    time_latest = time.strftime('%Y-%m-%d %H:%M:%S', time_stamp_latest)[:10]#select语句中要用的条件
    time_7days = time.strftime('%Y-%m-%d %H:%M:%S', time_stamp_2)[:10]
    # 查询语句和字段名
    select_string = '''
            select left(r.create_time,10) d , sum(if(r.type=11,1,0)) '贷前总量'  , sum(if(r.type=21,1,0)) '贷中总量'  ,   
        round(sum(if(r.type=11 and r.is_pass=0,1,0 ))/sum(if(r.type=11,1,0)),2) '贷前等待' ,
        round(sum(if(r.type=21 and r.is_pass=0,1,0 ))/sum(if(r.type=21,1,0)),2) '贷中等待',
        sum(if(r.type=11 and r.is_pass in (1,2),1,0 ))/sum(if(r.type=11,1,0)) '贷前通过率' ,
        sum(if(r.type=21 and r.is_pass in (1,2),1,0 ))/sum(if(r.type=21,1,0)) '贷中通过率',
        sum(if(r.type=11 and r.is_pass=3 and a.yi>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '内部准入',
        sum(if(r.type=11 and r.is_pass=3 and a.er>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '外部欺诈',
        sum(if(r.type=11 and r.is_pass=3 and a.san>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '外黑',
        sum(if(r.type=11 and r.is_pass=3 and a.si>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '外部逾期',
        sum(if(r.type=11 and r.is_pass=3 and a.wu>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '内黑',
        sum(if(r.type=11 and r.is_pass=3 and a.liu>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '内部逾期',
        sum(if(r.type=11 and r.is_pass=3 and a.qi>0,1,0 ))/sum(if(r.type=11 and r.is_pass=3,1,0)) '外部多头',

        sum(if(r.type=21 and r.is_pass=3 and a.yi>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '内部准入',
        sum(if(r.type=21 and r.is_pass=3 and a.er>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '外部欺诈',
        sum(if(r.type=21 and r.is_pass=3 and a.san>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '外黑',
        sum(if(r.type=21 and r.is_pass=3 and a.si>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '外部逾期',
        sum(if(r.type=21 and r.is_pass=3 and a.wu>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '内黑',
        sum(if(r.type=21 and r.is_pass=3 and a.liu>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '内部逾期',
        sum(if(r.type=21 and r.is_pass=3 and a.qi>0,1,0 ))/sum(if(r.type=21 and r.is_pass=3,1,0)) '外部多头'


        from qiandaodao_risks_control.t_user_credit_record r 
        left join (

        select m.credit_id,r.is_pass,sum(if(m.auth_result=20,1,0)),
        sum(if(m.auth_result=20 and left(c.rule_name,4)='内部准入',1,0 )) as yi ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)='外部欺诈'   ,1,0 )) as er ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)='外部黑名'  ,1,0 )) as san ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)='外部逾期'   ,1,0 )) as si ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)!='外部逾期' and left(c.rule_name,4)='内部黑名'   ,1,0 )) as wu ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)!='外部逾期' and left(c.rule_name,4)!='内部黑名' and left(c.rule_name,4)='内部逾期'   ,1,0 )) as liu ,
        sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
        left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)!='外部逾期' and left(c.rule_name,4)!='内部黑名' and left(c.rule_name,4)!='内部逾期'  and left(c.rule_name,4)='外部多头'   ,1,0 )) as qi
        from qiandaodao_risks_control.t_user_credit_model_record m 
        left join qiandaodao_risks_control.t_credit_rule c on c.rule_id=m.rule_id
        left join qiandaodao_risks_control.t_user_credit_record r  on r.credit_id=m.credit_id
        group by m.credit_id

        ) a on  a.credit_id=r.credit_id
        where r.create_time>'{time_1}' and r.create_time<='{time_2}' group by d;
            '''.format(time_1=time_7days, time_2=time_latest)
    columns = ['d', '贷前总量', '贷中总量', '贷前等待', '贷中等待', '贷前通过率', '贷中通过率', '内部准入', '外部欺诈', '外黑', '外部逾期', '内黑', '内部逾期',
               '外部多头', '内部准入', '外部欺诈', '外黑', '外部逾期', '内黑', '内部逾期', '外部多头']
    data = mysql_data(select_string, columns)
    print(data)

    #创建文件夹
    PATH_2 = PATH + '\\rules_loan_{date_name}'.format(date_name=time_now)
    if os.path.exists(PATH_2):
        print(PATH+"已存在！文件夹内容已覆盖")
    else:
        os.mkdir(PATH_2)
    writer = pd.ExcelWriter(PATH_2+'\\{date_name}.xlsx'.format(date_name=time_now))
    data.to_excel(writer, 'data')
    writer.save()
    #建立画布1，同一规则，贷前七天的拆线图
    fig_1 = plt.figure(figsize=(8,8), dpi=80)
    fig_1_ax = [fig_1.add_subplot(4, 2, i) for i in range(1, 8)]
    #建立画布2，同一规则，贷中七天的拆线图
    fig_2 = plt.figure(figsize=(8, 8), dpi=80)
    fig_2_ax = [fig_2.add_subplot(4, 2, i) for i in range(1, 8)]
    # 建立画布3，同一天，贷前规则的柱状图
    fig_3 = plt.figure(figsize=(8, 8), dpi=80)
    fig_3_ax = [fig_3.add_subplot(4, 2, i) for i in range(1, 8)]
    # 建立画布4，同一天，贷中规则的柱状图
    fig_4 = plt.figure(figsize=(8, 8), dpi=80)
    fig_4_ax = [fig_4.add_subplot(4, 2, i) for i in range(1, 8)]

if __name__ == "__main__":
    pass
