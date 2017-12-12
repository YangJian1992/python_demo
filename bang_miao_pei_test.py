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
# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string, columns_add):
        start = time.time()
        conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                               passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
        print('已经连接到数据库，请稍候...')
        cur = conn.cursor()
        cur.execute(select_string)

        temp = DataFrame(list(cur.fetchall()), columns=columns_add)
        print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
        # 提交
        conn.commit()
        # 关闭指针对象
        cur.close()
        # 关闭连接对象
        conn.close()
        return (temp)
    # for t in range(10):
    #     print(t)
    #     print('还有{t}s就要关机'.format(t=(10-t)))
    #     time.sleep(1)
    # os.system("shutdown -s -t 0")


#对call_history中的receiver进行去重
def receiver_drop_duplicates(data):
    data_list = []
    for name, group in data.groupby('mobile'):
        group = group.drop_duplicates('receiver')
        data_list.append(group)
    data = pd.concat(data_list, axis=0)
    return data



if __name__ == '__main__':
    path = 'D:\\work\\bang_miao_pei\\'
    file = 'overdue'
    select_string = '''
    SELECT
	ca.mobile,
    ch.receiver,
    ca.id,
	ca.user_id,
	ca.create_time,
	ca.first_loan,
	ca.loan_status,
	ca.overdue_days,
	ca.overdue_status
FROM
    (SELECT
        ulo.id,
		ulo.user_id,
		ulo.create_time,
		ulo.first_loan,
		ulo.loan_status,
		ulo.overdue_days,
		ulo.overdue_status,
		users.mobile
    FROM
        user_loan_orders AS ulo
    INNER JOIN users ON users.id = ulo.user_id
    WHERE
        ulo.first_loan = 1
            AND ulo.loan_status = 2
            AND ulo.overdue_days > 0
            AND LEFT(ulo.create_time, 7) = '2017-09'
    LIMIT 2000) AS ca
        LEFT JOIN
    call_history AS ch ON ch.mobile = ca.mobile
WHERE
    LEFT(ch.call_time, 7) BETWEEN '2017-09' AND '2017-11';
    '''
    columns_add = ['mobile', 'receiver', 'id', 'user_id', 'create_time', 'first_loan', 'loan_status', 'overdue_days', 'overdue_status']
    data = mysql_connection(select_string, columns_add)
    print(data.dtypes)
    data['mobile'] = data['mobile'].astype('str', errors='raise')
    data['receiver'] = data['receiver'].astype('str', errors='raise')
    data = receiver_drop_duplicates(data)

    data.to_csv(path + file + '.csv', encoding='utf-8', sep='\t')

    writer = pd.ExcelWriter(path + file + '.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()





