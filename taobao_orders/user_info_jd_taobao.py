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


def read_analysis_taobao_data(filename, PATH):
    # PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\all_json\\'
    with open(PATH+filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key,item)
    print('****************************\n')
    result_dict = {'file_name':filename}
    list_user_info = data_dict['userinfo']
    list_name = ['real_name', 'phone_number', 'email', 'alipay_account','first_ordertime', 'register_time', 'account_auth', 'weibo_account', 'weibo_nick']
    for item_name in list_name:
        result_dict[item_name] = list_user_info.get(item_name, '未找到该值_yj')
    print(result_dict)

    data_alipay = data_dict['alipaywealth']
    list_name_2 = ['balance', 'total_profit', 'total_quotient', 'huabei_creditamount','huabei_totalcreditamount']
    for item_name_2  in list_name_2:
        #阿里的货币单位是分
        result_dict[item_name_2] = data_alipay.get(item_name_2, '未找到该值_yj')/100

    data_deliveraddress = data_dict['deliveraddress']
    list_name_3 = ['province', 'city', 'name', 'phone_no']
    deliver_flag = 1
    if len(data_deliveraddress) >0:
        for deliver_item in data_deliveraddress:
            if deliver_item['default'] == True:
                for item_name_3 in list_name_3:
                    result_dict[item_name_3] = deliver_item.get(item_name_3, '未找到该值_yj')
                break
            else:
                deliver_flag = 0
    else:
        deliver_flag = 0
    if deliver_flag == 0:
        for item_name_3 in list_name_3:
            result_dict[item_name_3] = '用户没有设置默认地址_yj'
    print(result_dict)

    columns_list = ['real_name', 'phone_number', 'email', 'alipay_account','first_ordertime', 'register_time', 'account_auth', 'weibo_account', 'weibo_nick',
                    'balance', 'total_profit', 'total_quotient', 'huabei_creditamount', 'huabei_totalcreditamount', 'province', 'city', 'name', 'phone_no']
    return result_dict


def read_analysis_jd_data(filename):
    PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\only_dianshang\\'
    with open(PATH + filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key, item , '\n************************\n')

    # sub = data_dict['tradedetails']['tradedetails']
    # dict_info = data_dict['userinfo']
    # del dict_info['mapping_id']
    # dict_info.update(data_dict['wealth'])
    # data_info = DataFrame([dict_info])
    # print(np.array(data_info.columns))
    # for item in ['touravailable_limit', 'tourcredit_limit', 'tourcredit_waitpay','tourdelinquency_balance',
    #                 'vip_count', 'vip_level','vip_count', 'nick']:
    #     if item in data_info.columns:
    #         data_info.drop([item], axis=1, inplace=True)
    # print('*******************',data_info.columns, '*******************')
    #
    # data = DataFrame(sub)
    # data['user_name'] = data_dict['userinfo']['user_name']
    # if len(data) != 0:
    #     data['products'] = data['products'].map(lambda x: [item['name'] if 'name' in item else 'name_null' for item in x] if len(x) > 0 else x)
    #     data_result = pd.merge(data_info, data, on='user_name')
    #     writer = pd.ExcelWriter(PATH+'excel\\'+filename[:-4]+'.xlsx')
    #     data_result.to_excel(writer, 'yangjian')
    #     writer.save()
    #     print(data_result)





if __name__ == '__main__':
    PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\all_json\\'
    file_list = os.listdir(PATH)
    result_list = []
    for item_name in file_list:
        if 'json' in item_name and ('taobao' in item_name):
            taobao_dict = read_analysis_taobao_data(item_name, PATH)
            result_list.append(taobao_dict)

    data = DataFrame(result_list)
    writer = pd.ExcelWriter(PATH+'info_taobao.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()