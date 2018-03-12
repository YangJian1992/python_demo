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
import PIL #注意，不是pillow
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
    PATH = 'D:\\work\\loan_rules'
    # 此刻时间点和七天前的时间点，按自然日计算
    time_stamp = time.gmtime(time.time() + 8 * 3600)  # 按日期处理，今天是2018-03-05， 应该小于等于2018-03-06
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time_stamp)[:10]
    PATH_2 = PATH + '\\rules_loan_{date_name}'.format(date_name=time_now)
    msg = email.mime.multipart.MIMEMultipart()
    msg['from'] = sendAddr
    msg['to'] = recipientAddrs#多个收件人的邮箱应该放在字符串中,用字符分隔, 然后用split()分开,不能放在列表中, 因为要使用encode属性
    msg['subject'] = subject
    content = content
    txt = email.mime.text.MIMEText(content, 'plain', 'utf-8')
    msg.attach(txt)
    print("准备添加附件...")
    if os.path.exists(PATH_2):
        # 添加附件，从本地路径读取。如果添加多个附件，可以定义part_2,part_3等，然后使用part_2.add_header()和msg.attach(part_2)即可。
        for part_name in os.listdir(PATH_2):
            if 'xlsx' in part_name:
                part = MIMEApplication(open(PATH_2 + '\\' + part_name,'rb').read())
                part.add_header('Content-Disposition', 'attachment', filename=part_name)#给附件重命名,一般和原文件名一样,改错了可能无法打开.
                msg.attach(part)
    else:
        print(PATH_2+'不存在，请先处理数据得到结果后再发邮件。')
        return
    smtp = smtplib.SMTP_SSL(smtpHost, port)#需要一个安全的连接，用SSL的方式去登录得用SMTP_SSL，之前用的是SMTP（）.端口号465或587
    smtp.login(sendAddr, password)#发送方的邮箱，和授权码（不是邮箱登录密码）
    smtp.sendmail(sendAddr, recipientAddrs.split(";"), str(msg))#注意, 这里的收件方可以是多个邮箱,用";"分开, 也可以用其他符号
    print("发送成功！")
    smtp.quit()


#处理数据
def data_analysis():
    PATH = 'D:\\work\\loan_rules'
    # 此刻时间点和七天前的时间点，按自然日计算
    time_stamp = time.gmtime(time.time() + 8 * 3600)#按日期处理，今天是2018-03-05， 应该小于等于2018-03-06
    time_stamp_latest = time.gmtime(time.time() + 8*3600 + 24*3600)
    time_stamp_2 = time.gmtime(time.time() + 8 * 3600- 13 * 24 * 3600)#不是14，是13，求14天之前的时间点
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
sum(if(r.type=11 and r.is_pass=3 and a.yi>0,1,0 ))/sum(if(r.type=11,1,0)) '内部准入',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er>0,1,0 ))/sum(if(r.type=11,1,0)) '外部欺诈',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san>0,1,0 ))/sum(if(r.type=11,1,0)) '外黑',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si>0,1,0 ))/sum(if(r.type=11,1,0)) '外部逾期',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu>0,1,0 ))/sum(if(r.type=11,1,0)) '内黑',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu>0,1,0 ))/sum(if(r.type=11,1,0)) '内部逾期',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu=0 and a.qi>0,1,0 ))/sum(if(r.type=11,1,0)) '外部多头',
sum(if(r.type=11 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu=0 and a.qi=0 and a.ba>0,1,0 ))/sum(if(r.type=11,1,0)) '通话电商',

sum(if(r.type=21 and r.is_pass=3 and a.yi>0,1,0 ))/sum(if(r.type=21,1,0)) '内部准入',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er>0,1,0 ))/sum(if(r.type=21,1,0)) '外部欺诈',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san>0,1,0 ))/sum(if(r.type=21,1,0)) '外黑',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si>0,1,0 ))/sum(if(r.type=21,1,0)) '外部逾期',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu>0,1,0 ))/sum(if(r.type=21,1,0)) '内黑',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu>0,1,0 ))/sum(if(r.type=21,1,0)) '内部逾期',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu=0 and a.qi>0,1,0 ))/sum(if(r.type=21,1,0)) '外部多头',
sum(if(r.type=21 and r.is_pass=3 and a.yi=0 and a.er=0 and a.san=0 and a.si=0 and a.wu=0 and a.liu=0 and a.qi=0 and a.ba>0,1,0 ))/sum(if(r.type=21,1,0)) '通话电商'
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
left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)!='外部逾期' and left(c.rule_name,4)!='内部黑名' and left(c.rule_name,4)!='内部逾期'  and left(c.rule_name,4)='外部多头'   ,1,0 )) as qi,
sum(if(m.auth_result=20 and left(c.rule_name,4)!='内部准入'  and 
left(c.rule_name,4)!='外部欺诈' and left(c.rule_name,4)!='外部黑名' and left(c.rule_name,4)!='外部逾期' and left(c.rule_name,4)!='内部黑名' and left(c.rule_name,4)!='内部逾期'  and left(c.rule_name,4)!='外部多头' and left(c.rule_name,4)='外部资质'  ,1,0 )) as ba
from qiandaodao_risks_control.t_user_credit_model_record m 
left join qiandaodao_risks_control.t_credit_rule c on c.rule_id=m.rule_id
left join qiandaodao_risks_control.t_user_credit_record r  on r.credit_id=m.credit_id
group by m.credit_id
) a on  a.credit_id=r.credit_id
where r.create_time>'{time_1}' and r.create_time<='{time_2}' group by d;
            '''.format(time_1=time_7days, time_2=time_latest)
    columns = ['日期', '贷前总量', '贷中总量','贷前等待,%', '贷中等待,%', '贷前通过率,%', '贷中通过率,%', '贷前_内部准入,%', '贷前_外部欺诈,%',
               '贷前_外黑,%', '贷前_外部逾期,%', '贷前_内黑,%', '贷前_内部逾期,%', '贷前_外部多头,%', '贷前_通话电商,%', '贷中_内部准入,%',
               '贷中_外部欺诈,%', '贷中_外黑,%', '贷中_外部逾期,%', '贷中_内黑,%', '贷中_内部逾期,%', '贷中_外部多头,%', '贷中_通话电商,%']
    data = mysql_data(select_string, columns)
    print(data)
    for col_item in data.columns:
        if col_item != '日期':
            if col_item in ['贷前总量', '贷中总量']:
                data[col_item] = data[col_item].astype('int')
            else:
                data[col_item] = data[col_item].astype('float')
                data[col_item] = data[col_item].map(lambda x: round(x*100, 2))
    #创建文件夹
    PATH_2 = PATH + '\\rules_loan_{date_name}'.format(date_name=time_now)
    if os.path.exists(PATH_2):
        print(PATH+"已存在！文件夹内容将被覆盖")
    else:
        os.mkdir(PATH_2)
    #生成工作簿
    writer = pd.ExcelWriter(PATH_2+'\\{date_name}.xlsx'.format(date_name=time_now))
    data.to_excel(writer, '数据')
    writer.save()

    #建立时间序列
    data['日期'] = data['日期'].str[5:]
    data.set_index('日期', inplace=True)
    data_sum = data[['贷前总量', '贷中总量','贷前等待,%', '贷中等待,%', '贷前通过率,%', '贷中通过率,%']]
    data_before = data[['贷前_内部准入,%', '贷前_外部欺诈,%','贷前_外黑,%', '贷前_外部逾期,%', '贷前_内黑,%', '贷前_内部逾期,%', '贷前_外部多头,%', '贷前_通话电商,%']]
    data_ing = data[['贷中_内部准入,%','贷中_外部欺诈,%', '贷中_外黑,%', '贷中_外部逾期,%', '贷中_内黑,%', '贷中_内部逾期,%', '贷中_外部多头,%', '贷中_通话电商,%']]
    #画图，中文字体
    font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=10)
    font_1 = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=15)#柱状图的标题大一点
    #建立画布1，同一规则，贷前七天的拆线图
    rule_name_1 = data_before.columns
    #为每个字段画拆线图
    for key, item_rule in enumerate(data.columns):
        fig = plt.figure(figsize=(7, 4), dpi=100)
        fig_ax = fig.add_subplot(1, 1, 1)
        data_s = data[item_rule]
        fig_ax.plot(data_s.index, data_s.values, color='blue', marker='o', linestyle='dashed')
        fig_ax.set_title(item_rule, fontproperties=font_1)
        fig.subplots_adjust(left=0.08, bottom=0.1, right=0.95, top=0.9, wspace=0.1, hspace=0.5)#间隔百分比
        fig.savefig(PATH_2+'\\' + item_rule+'.png')
        plt.close()#关闭当前画图,可以传入参数'all'，删除所有

    # 建立画布3，同一天，贷前规则的柱状图
    fig_3 = plt.figure(figsize=(12, 6), dpi=100)
    ax_3= fig_3.add_subplot(111)
    ax_3.set_title('贷前规则柱状图', fontproperties=font_1)
    ax_3.set_xlabel('日期', fontproperties=font)
    ax_3.set_ylabel('百分比', fontproperties=font)
    ax_3_plot = data_before[7:].plot(kind='bar', ax=ax_3,  alpha=0.8, title='贷前规则柱状图')#df调用plot()返回的是ax对象
    labels = ax_3_plot.get_xticklabels() + ax_3_plot.legend().texts
    for label_3 in labels:
        label_3.set_fontproperties(font)
    fig_3.subplots_adjust(left=0.05, bottom=0.15, right=0.95, top=0.95)  # 间隔百分比调整每幅图的的间隔，左右间隔以整个图的左边界为准，上下间隔是以整个图的下边界为准。wspace是子两图之间宽度间隔，hspace是两子图之间的高度间隔
    fig_3.show()
    fig_3.savefig(PATH_2 + '\\贷前规则柱状图.png')
    plt.close()  # 关闭当前画图,可以传入参数'all'，删除所有

    # 建立画布4，同一天，贷中规则的柱状图
    fig_4 = plt.figure(figsize=(12, 6), dpi=100)
    ax_4 = fig_4.add_subplot(111)
    ax_4.set_title('贷中规则柱状图', fontproperties=font_1)
    ax_4.set_xlabel('日期', fontproperties=font)
    ax_4.set_ylabel('百分比', fontproperties=font)
    ax_4_plot = data_ing[7:].plot(kind='bar', ax=ax_4, alpha=0.8, title='贷中规则柱状图')  # df调用plot()返回的是ax对象
    labels = ax_4_plot.get_xticklabels() + ax_4_plot.legend().texts
    for label_4 in labels:
        label_4.set_fontproperties(font)
    fig_4.subplots_adjust(left=0.05, bottom=0.15, right=0.95, top=0.95)  # 间隔百分比
    fig_4.show()
    fig_4.savefig(PATH_2 + '\\贷中规则柱状图.png')
    plt.close()  # 关闭当前画图,可以传入参数'all'，删除所有

    #打开保存有图片的工作簿，追加数据
    wb_path = PATH_2+'\\{date_name}.xlsx'.format(date_name=time_now)
    work_book = openpyxl.load_workbook(wb_path)  # 这是现有的工作簿，只有sheet1表
    #建立多个工作表用于存放图片
    for sheet_name in ['总体变化图', '贷前规则变化拆线图', '贷中规则变化拆线图', '贷前规则柱状图', '贷中规则柱状图']:
        sheet = work_book.create_sheet(title=sheet_name)
        if sheet_name == '总体变化图':
            for key, image_name in enumerate(map(lambda x:x+'.png', data_sum.columns)):
                image_path = PATH_2 + '\\'+ image_name
                img = openpyxl.drawing.image.Image(image_path)
                #一行两张图片，不能重叠
                if key <= 2:
                    sheet.add_image(img, 'B'+ str(key*20+1))
                else:
                    sheet.add_image(img, 'M' + str((key-3) * 20 + 1))
        elif sheet_name == '贷前规则变化拆线图':
            for key, image_name in enumerate(map(lambda x:x + '.png', data_before.columns)):
                image_path = PATH_2 + '\\'+ image_name
                img = openpyxl.drawing.image.Image(image_path)
                if key <= 3:
                    sheet.add_image(img, 'B'+ str(key*20+1))
                else:
                    sheet.add_image(img, 'M' + str((key-4) * 20 + 1))
        elif sheet_name == '贷中规则变化拆线图':
            for key, image_name in enumerate(map(lambda x:x+'.png', data_before.columns)):
                image_path = PATH_2 + '\\'+ image_name
                img = openpyxl.drawing.image.Image(image_path)
                if key <= 3:
                    sheet.add_image(img, 'B'+ str(key*20+1))
                else:
                    sheet.add_image(img, 'M' + str((key-4) * 20 + 1))
        else:
            image_path = PATH_2 + '\\' + sheet_name +'.png'
            img = openpyxl.drawing.image.Image(image_path)
            sheet.add_image(img, 'A1')
    work_book.save(wb_path)
    #图片插入到excel后，再删除文件夹中的所有png图片
    image_list = list(filter(lambda x : 'png' in x, os.listdir(PATH_2)))#需要用list转换一下
    [os.remove(PATH_2+'\\'+x) for x in image_list]#列表推导式中的元素可以为空，只执行删除操作。

if __name__ == "__main__":
    time_stamp = time.gmtime(time.time() + 8 * 3600)
    time_now = time.strftime('%Y-%m-%d %H:%M:%S', time_stamp)[:10]
    data_analysis()
    try:
        # 设置好邮箱信息
        smtpHost = 'smtp.exmail.qq.com'  # 这是QQ邮箱服务器。如果是腾讯企业邮箱，其服务器为smtp.exmail.qq.com。其他邮箱需要查询服务器地址和端口号。
        port = 465  # 端口号
        sendAddr = 'yangj@qiandaodao.com'  # 发送方地址
        password = 'ZEWEbUPUo25pkTLD'#input("请输入邮箱授权码：")  # 手动输入授权码更安全.授权码的获取:打开qq邮箱->设置->账户->开启IMAP/SMTP服务->发送短信->授权码
        recipientAddrs = 'liuzhibo@qiandaodao.com'  # 接收方可以是多个账户, 用分号分开,send_email()函数中手动设置
        subject = '最近14天的贷前贷中数据_{d}'.format(d=time_now)  # 主题
        content = '''
        你好，
            附件中包括数据和相应的图片，请查收。
        ''' # 正文内容
        send_email(smtpHost, port, sendAddr, password, recipientAddrs, subject, content)
    except Exception as err:
        print(err)
