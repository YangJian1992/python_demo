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

def read_analysis_data(filename):
    PATH = 'D:\\work\\dian_shang\\dianshangfile\\dianshangfile\\'
    with open(PATH+filename, encoding='utf-8') as file:
        data_dict = json.load(file)

    sub = data_dict['tradedetails']['tradedetails']
    for item in sub:
        print(item['sub_orders'][0]['item_name'])

    dict_1 = data_dict['userinfo']
    dict_2 = data_dict['alipaywealth']
    dict_1.update(dict_2)
    # dict_1外面需要放在一个列表中，列表中每个字典都df中的一行，如果不是放在列表中，则无法转换成df
    data_user = DataFrame([dict_1])
    data_trade = DataFrame(data_dict['tradedetails']['tradedetails'])
    data_trade['real_name'] = dict_1['real_name']
    data = pd.merge(data_user, data_trade, on='real_name')
    if len(data_dict['tradedetails']['tradedetails'])!=0:
        data['sub_orders'] = data['sub_orders'].map(lambda x: [item['item_name'] for item in x] if len(x)>0 else x)
        data.to_csv('D:\\work\\dian_shang\\dianshangfile\\result\\'+filename[:-4]+'csv', encoding='gbk', sep=',', index=False)


    # print(data_user)
    # print(data_trade)
    # print(data)


if __name__ == '__main__':
    PATH = 'D:\\work\\dian_shang\\dianshangfile\\dianshangfile\\'
    for filename in os.listdir(PATH):
        if 'taobao' in filename:
            data = read_analysis_data(filename)
        # break

