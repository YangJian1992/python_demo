#coding:utf-8
import os
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
import PyPDF2
data= DataFrame({'a':[469025, 2, 3, 4],
                 'b': [6, 7, 8, 9],
                 'c': ['adkkda', 6, 5, 6]},index=['a', 'b', 'c', 'd'])

start = time.time()
def dfun2():
    start2 = time.time()
    df2 = pd.read_table(path + filename_2)
    print(df2)
    print('df2的时间是%d----------------------------------'%(time.time()-start2))
path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
filename = 'call_history_three_months.csv'
filename_2 = 'call_history_new_1.csv'
# thread_obj = threading.Thread(target=dfun2)
# thread_obj.start()
# start3 = time.time()
# df1 = pd.read_table(path + filename)
# print(df1)
# print('df1时间为%d'%(time.time()-start3))
# print('累计时间为%d'%(time.time()-start))
dfun2()
# subject = 'python mail'
# message['Subject'] = Header(subject, 'utf-8')
# print(smtp_obj.sendmail('920892845@qq.com', '1556492839@qq.com', message.as_string()))