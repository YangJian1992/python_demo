#coding:utf-8
import os
import sys
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
data= DataFrame({'a':[469025, 2, 3, 4],
                 'b': [6, 7, 8, 9],
                 'c': ['adkkda', 6, 5, 6]},index=['a', 'b', 'c', 'd'])

def abc(fun):
    def de():
        start = time.time()
        fun(2, 1, 2, 3, x=4, y=3)
        print('函数执行所需时间为：%s'%(time.time()-start))
    return de

@abc
def gg(x, *args, **kwargs):
    print(type(args))
    print(x, args, kwargs)
    return 'python'

print(gg(2, 1, 2, 3, x=4, y=3))

# thread_obj = threading.Thread(target=dfun2)
# thread_obj.start()
# start3 = time.time()
# df1 = pd.read_table(path + filename)
# print(df1)
# print('df1时间为%d'%(time.time()-start3))
# print('累计时间为%d'%(time.time()-start))
# subject = 'python mail'
# message['Subject'] = Header(subject, 'utf-8')
# print(smtp_obj.sendmail('920892845@qq.com', '1556492839@qq.com', message.as_string()))