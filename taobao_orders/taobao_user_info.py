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

def read_analysis_taobao_data(filename):
    PATH = 'D:\\work\\2018_1_新的风控规则\\01-27and28-dianshang\\'
    with open(PATH+filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key,item)
    #收货地址不全，并不是和商品订单一一对应，用left join
    data_delever = DataFrame(data_dict['recentdeliveraddress'])
    if 'actual_fee' in data_delever.columns:
        del data_delever['actual_fee']
    if 'trade_createtime' in data_delever.columns:
        del data_delever['trade_createtime']
    print(data_delever)

    sub = data_dict['tradedetails']['tradedetails']
    for item in sub:
        print(item['sub_orders'][0]['item_name'])

    dict_1 = data_dict['userinfo']
    dict_2 = data_dict['alipaywealth']
    dict_1.update(dict_2)
    del dict_1['mapping_id']
    # dict_1外面需要放在一个列表中，列表中每个字典都df中的一行，如果不是放在列表中，则无法转换成df
    data_user = DataFrame([dict_1])
    data_trade = DataFrame(data_dict['tradedetails']['tradedetails'])
    print(data_trade)
    data_trade['real_name'] = dict_1['real_name']
    data = pd.merge(data_user, data_trade, on='real_name')
    if len(data_dict['tradedetails']['tradedetails'])!=0:
        data['sub_orders'] = data['sub_orders'].map(lambda x: [item['item_name'] for item in x] if len(x)>0 else x)
        for money_column in ['actual_fee', 'huabei_totalcreditamount','huabei_creditamount', 'total_profit', 'balance', 'total_quotient']:
            data[money_column] = data[money_column] * 0.01

        if 'trade_id' in data_delever.columns:
            data_result = pd.merge(data, data_delever, how='left', on='trade_id')
            for item in ['nick', 'pic', 'tao_score', 'seller_id', 'seller_nick']:
                if item in data_result.columns:
                    data_result.drop([item], axis=1, inplace=True)
            writer = pd.ExcelWriter(PATH+'excel\\'+filename[:-4]+'xlsx')
            data_result.to_excel(writer, 'yangjian')
            writer.save()

def read_analysis_jd_data(filename):
    PATH = 'D:\\work\\2018_1_新的风控规则\\01-27and28-dianshang\\'
    with open(PATH + filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key, item , '\n************************\n')
    sub = data_dict['tradedetails']['tradedetails']
    dict_info = data_dict['userinfo']
    del dict_info['mapping_id']
    dict_info.update(data_dict['wealth'])
    data_info = DataFrame([dict_info])
    print(np.array(data_info.columns))
    for item in ['touravailable_limit', 'tourcredit_limit', 'tourcredit_waitpay','tourdelinquency_balance',
                    'vip_count', 'vip_level','vip_count', 'nick']:
        if item in data_info.columns:
            data_info.drop([item], axis=1, inplace=True)
    print('*******************',data_info.columns, '*******************')

    data = DataFrame(sub)
    data['user_name'] = data_dict['userinfo']['user_name']
    if len(data) != 0:
        data['products'] = data['products'].map(lambda x: [item['name'] if 'name' in item else 'name_null' for item in x] if len(x) > 0 else x)
        data_result = pd.merge(data_info, data, on='user_name')
        writer = pd.ExcelWriter(PATH+'excel\\'+filename[:-4]+'.xlsx')
        data_result.to_excel(writer, 'yangjian')
        writer.save()
        print(data_result)


if __name__ == '__main__':
    PATH = 'D:\\work\\2018_1_新的风控规则\\01-27and28-dianshang\\'
    for filename in os.listdir(PATH):
        if 'json' in filename:
            if 'taobao' in filename:
                read_analysis_taobao_data(filename)
            else:
                read_analysis_jd_data(filename)
                #     break


