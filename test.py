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
from matplotlib.font_manager import FontProperties
font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=15)


filename = 'C:\\Users\\QDD\Desktop\\15857729339_2018-01-25'
with open(filename+'.json', encoding='utf-8') as file_json:
    data = json.load(file_json)
print(type(data), '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
print(data.keys())
# for key, item in data.items():
#     print(key, item, '\n***********\n')
data_list = []
for item in data['calls']:
    call_data = DataFrame(item['items'])
    data_list.append(call_data)
data_result = pd.concat(data_list, ignore_index=True)
data_result['mobile'] = str(data['mobile'])
data_result['idcard'] = str(data['idcard'])
s1 = data_result.pop('mobile')
print(data_result)
data_result.insert(0, 'mobile', s1)
s2 = data_result.pop('idcard')
data_result.insert(1, 'idcard', s2)
# print(data_result.info)
data_result['time_hour'] = data_result['time'].str.slice(-8,-6)
print(data_result['time_hour'])
# print('\n**********************************\n', data_result)
writer = pd.ExcelWriter(filename+'.xlsx')
data_result.to_excel(writer, 'sheet1')
writer.save()
# data2 = writer.parse('sheet1')
# print(data2)
# data_result.to_csv(filename+'.csv', sep=',', encoding='gbk', index=False)







