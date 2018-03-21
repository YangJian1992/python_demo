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
        result_dict[item_name] = list_user_info.get(item_name, '!!!!!!未找到该值_yj')
    print(result_dict)

    data_alipay = data_dict['alipaywealth']
    list_name_2 = ['balance', 'total_profit', 'total_quotient', 'huabei_creditamount','huabei_totalcreditamount']
    for item_name_2  in list_name_2:
        #阿里的货币单位是分
        result_dict[item_name_2] = float(data_alipay.get(item_name_2, '!!!!!!未找到该值_yj'))/100

    data_recentdeliveraddress = data_dict['recentdeliveraddress']
    data_recent_list = []
    if len(data_recentdeliveraddress) != 0:
        for data_recent_item in data_recentdeliveraddress:
            new_data_recent = {k:v for k,v in data_recent_item.items() if k in ['deliver_name', 'deliver_mobilephone', 'deliver_address']}
            # new_data_recent = dict(filter(lambda x:x[0] in ['deliver_name', 'deliver_mobilephone', 'deliver_address'], data_recent_item))
            print("new_data_recent:", new_data_recent)
            data_recent_list.append(new_data_recent)
        print(DataFrame(data_recent_list))
        result_dict["recentdeliveraddress"] = np.array(DataFrame(data_recent_list).drop_duplicates()).tolist()
    else:
        result_dict["recentdeliveraddress"] = []
    # print(result_dict["recentdeliveraddress"])

    #交易订单明细
    data_tradedetails = data_dict['tradedetails']['tradedetails']
    for item_3 in data_tradedetails:
        print(item_3, '\n*************************************************************************')
    if len(data_tradedetails) != 0:
        data_tradedetails = DataFrame(data_tradedetails)
        #报错了，有缺失值Na
        data_tradedetails['trade_text'].fillna('null_yj', inplace=True)
        data_tradedetails = data_tradedetails[data_tradedetails['trade_text'].str.contains("成功")]
        if len(data_tradedetails) != 0:
            data_tradedetails['actual_fee'] = data_tradedetails['actual_fee'].map(lambda x: round(x*0.01, 2))
            print(data_tradedetails['actual_fee'])
            sub_class_list = []
            for sub_item in data_tradedetails['sub_orders']:
                for class_item in sub_item:
                    if 'cname_level1' in class_item.keys():
                        sub_class_list.append({v:k for v,k in class_item.items() if v in ['cname_level1', 'cid_level1']})
                    else:
                        sub_class_list.append({'cname_level1':'!!!未找到_yj', 'cid_level1':'!!!未找到_yj'})
            data_sub_class = DataFrame(sub_class_list)
            a = data_sub_class.groupby('cname_level1').count()
            goods_class_num = dict(Series(a['cid_level1']))
            goods_class_num = dict(sorted(goods_class_num.items(), key=lambda x:x[1],  reverse=True))
            print(goods_class_num)
            #不同类别的商品数量
            result_dict['goods_class_num'] = goods_class_num

            #交易的最近和最早时间。又报错了，因为'trade_createtime'字段中含有float格式的数据。
            data_tradedetails['trade_createtime'] = data_tradedetails['trade_createtime'].astype('str')
            result_dict['trade_createtime_range'] = [data_tradedetails['trade_createtime'].min(), data_tradedetails['trade_createtime'].max()]
            #交易成功的订单数量，及订单额度的统计量（总和，最大值，平均值，标准差）
            result_dict['success_order_num'] = len(data_tradedetails)
            result_dict['success_order_fee'] = [round(data_tradedetails['actual_fee'].sum(), 2), round(data_tradedetails['actual_fee'].max(),2), round(data_tradedetails['actual_fee'].mean(), 2), round(data_tradedetails['actual_fee'].std(), 2)]
            # 充值成功的订单数量，及订单额度的统计量
            data_tradedetails_topup = data_tradedetails[data_tradedetails['trade_text'].str.contains("充值成功")]
            result_dict['top_up_num'] = len(data_tradedetails_topup)
            result_dict['top_up_fee'] = [round(data_tradedetails_topup['actual_fee'].sum(),2), round(data_tradedetails_topup['actual_fee'].max(),2), round(data_tradedetails_topup['actual_fee'].mean(), 2), round(data_tradedetails_topup['actual_fee'].std(), 2)]
            print(result_dict['success_order_fee'], result_dict['top_up_fee'])
        else:
            result_dict['goods_class_num'] = {}
            result_dict['trade_createtime_range'] = []
            result_dict['success_order_num'] = 0
            result_dict['success_order_fee'] = []
            result_dict['top_up_num'] = 0
            result_dict['top_up_fee'] = []
    else:
        result_dict['goods_class_num'] = {}
        result_dict['trade_createtime_range'] = []
        result_dict['success_order_num'] = 0
        result_dict['success_order_fee'] = []
        result_dict['top_up_num'] = 0
        result_dict['top_up_fee'] = []


    data_deliveraddress = data_dict['deliveraddress']
    list_name_3 = ['name', 'phone_no', 'address', 'full_address']
    deliver_flag = 1
    if len(data_deliveraddress) > 0:
        for deliver_item in data_deliveraddress:
            if deliver_item['default'] == True:
                for item_name_3 in list_name_3:
                    result_dict[item_name_3] = deliver_item.get(item_name_3, '!!!!!!未找到该值_yj')
                break
            else:
                deliver_flag = 0
    else:
        deliver_flag = 0
    if deliver_flag == 0:
        for item_name_3 in list_name_3:
            result_dict[item_name_3] = '!!!!!!用户没有设置默认地址_yj'
    print(result_dict.keys())

    # columns_list = ['real_name', 'phone_number', 'email', 'alipay_account','first_ordertime', 'register_time', 'account_auth', 'weibo_account', 'weibo_nick',
    #                 'balance', 'total_profit', 'total_quotient', 'huabei_creditamount', 'huabei_totalcreditamount', 'province', 'city', 'name', 'phone_no']
    return result_dict


def read_analysis_jd_data(filename):
    PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\only_dianshang\\'
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
    PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\all_json\\'
    columns = ['file_name', 'real_name', 'phone_number', 'email', 'first_ordertime', 'register_time', 'account_auth', 'weibo_account', 'weibo_nick','alipay_account',
               'balance', 'total_profit', 'total_quotient', 'huabei_creditamount', 'huabei_totalcreditamount',
                'name', 'phone_no', 'address', 'full_address',
               'recentdeliveraddress', 'goods_class_num', 'trade_createtime_range',
               'success_order_num', 'success_order_fee', 'top_up_num', 'top_up_fee']
    columns_1 = ["userinfo", "userinfo", "userinfo", "userinfo", "userinfo", "userinfo", "userinfo", "userinfo", "userinfo","userinfo",
                 "alipaywealth", "alipaywealth", "alipaywealth", "alipaywealth", "alipaywealth",
                 "deliveraddress", "deliveraddress", "deliveraddress", "deliveraddress",
                 "tradedetails", "tradedetails", "tradedetails", "tradedetails", "tradedetails", "tradedetails", "tradedetails"
                 ]
    read_analysis_taobao_data('13855230268_taobao_2018-01-24.json', PATH)
    # file_list = os.listdir(PATH)
    # result_list = []
    # for item_name in file_list:
    #     if 'json' in item_name and ('taobao' in item_name):
    #         taobao_dict = read_analysis_taobao_data(item_name, PATH)
    #         result_list.append(taobao_dict)
    #
    # data = DataFrame(result_list, columns=columns).values
    # data = DataFrame(data, columns=[columns_1, columns])
    # writer = pd.ExcelWriter(PATH+'info_taobao.xlsx')
    # data.to_excel(writer, 'sheet1')
    # writer.save()