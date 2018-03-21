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

#求过去距离time_str(日期到天，固定格式)相距days天的时间点
def time_ago(time_str, days):
    tt = time.strptime(time_str, '%Y-%m-%d')
    ts = time.mktime(tt)
    s = ts-days*24*3600+8*3600
    tt_2 = time.gmtime(s)
    return time.strftime('%Y-%m-%d', tt_2)

#不同时间区间内的订单总数、总金额和平均值
def trade_analysis(time_str, data_trade):
    trade_time_min = data_trade['trade_time'].min()
    if time_str > trade_time_min:
        data = data_trade[data_trade['trade_time']>time_str]
        return [len(data), round(data['amount'].sum(), 2), round(data['amount'].mean(), 2)]
    else:
        return []

#主要函数
def read_analysis_jd_data(filename, PATH):
    with open(PATH + filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key, item , '\n************************\n')
    #文件的爬取时间
    get_time = filename[-15:-5]
    time_7days = time_ago(get_time, 7)
    time_30days = time_ago(get_time, 30)
    time_90days = time_ago(get_time, 90)
    #个人信息
    result_dict = {'file_name': filename}
    list_user_info = data_dict['userinfo']
    list_name = ['email', 'gender', 'mapping_id', 'real_name', 'xb_credit', 'phone_number','card_number']
    for item_name in list_name:
        result_dict[item_name] = list_user_info.get(item_name, '!!!未找到该值_yj')
    #京东金融
    data_alipay = data_dict['wealth']
    list_name_2 = ['total_money','available_limit','credit_limit','credit_waitpay','delinquency_balance','tourcredit_limit', 'tourdelinquency_balance']
    for item_name_2 in list_name_2:
        # 阿里的货币单位是分
        result_dict[item_name_2] = data_alipay.get(item_name_2, '!!!未找到该值_yj')

    #默认收货地址
    data_deliveraddress = data_dict['deliveraddresses']
    list_name_3 = ['province', 'city', 'name', 'phone_no']
    deliver_flag = 1
    if len(data_deliveraddress) > 0:
        for deliver_item in data_deliveraddress:
            if deliver_item['default'] == True:
                for item_name_3 in list_name_3:
                    result_dict[item_name_3] = deliver_item.get(item_name_3, '!!!未找到该值_yj')
                break
            else:
                deliver_flag = 0
    else:
        deliver_flag = 0
    if deliver_flag == 0:
        for item_name_3 in list_name_3:
            result_dict[item_name_3] = '!!!用户没有设置默认收货地址_yj'

    #交易明细

    data_trade = DataFrame(data_dict['tradedetails']['tradedetails'])
    if len(data_trade) != 0 and 'trade_status' in data_trade.columns:
        data_trade['trade_status'].fillna('null', inplace=True)
        data_trade = data_trade[data_trade['trade_status'].str.contains("已完成")]
        if len(data_trade)!=0:
            print(data_trade['trade_time'], '********************************************')
            #订单总数量、总金额、平均值
            result_dict['trade_amount_num'] = len(data_trade)
            result_dict['trade_amount_sum'] = data_trade['amount'].sum()
            result_dict['trade_amount_mean'] = round(data_trade['amount'].mean(), 2)
            trade_time_min = data_trade['trade_time'].min()
            trade_time_max =data_trade['trade_time'].max()
            time_span = time.mktime(time.strptime(trade_time_max[0:10], '%Y-%m-%d'))-time.mktime(time.strptime(trade_time_min[0:10], '%Y-%m-%d'))
            result_dict['trade_time_range'] = [trade_time_min, trade_time_max]
            result_dict['trade_time_days'] = round(time_span/3600/24, 0)
            #不同时间区间内订单统计
            trade_data_7days = trade_analysis(time_7days, data_trade)
            trade_data_30days = trade_analysis(time_30days, data_trade)
            trade_data_90days = trade_analysis(time_90days, data_trade)
            result_dict['amount_7days'] = trade_data_7days
            result_dict['amount_30days'] = trade_data_30days
            result_dict['amount_90days'] = trade_data_90days
            #商品名称中的关键信息：母婴，汽车，房产，宠物
            child_set = set()
            car_set = set()
            apartment_set = set()
            pet_set = set()
            for index in data_trade.index:
                item_list = data_trade.ix[index,'products']
                product_amount = data_trade.ix[index, 'amount']
                if len(item_list) !=0:
                    for item_dict in item_list:
                        product_name = item_dict.get('name', '!!!not_find_name_yj')
                        for key_word in ['亲子', '儿童', '婴', '孩', '孕妇']:
                            if key_word in product_name:
                                child_set.add(product_name)
                        for key_word in ['汽车', '车用']:
                            if key_word in product_name and product_amount>50:#车用大于50元
                                car_set.add(product_name)
                        for key_word in ['宠物', '猫/狗', '狗粮', '猫粮']:
                            if key_word in product_name:
                                pet_set.add(product_name)
                        for key_word in ['家电', '装修','冰箱', '空调', '彩电', '电视', '沙发', '床垫']:#有些插座、配件的名称中也含有“空调”等字，所以金额大于500
                            if key_word in product_name and product_amount > 500:
                                apartment_set.add(product_name)
            result_dict['child_product'] = child_set
            result_dict['car_product'] = car_set
            result_dict['apartment_product'] = apartment_set
            result_dict['pet_product'] = pet_set
            #四类关键商品的数量
            result_dict['child_car_apartment_pet'] = [len(child_set), len(car_set), len(apartment_set), len(pet_set)]
        else:
            result_dict['trade_time_range'] = []
            result_dict['trade_time_days'] = 0
            result_dict['trade_amount_num'] = 0
            result_dict['trade_amount_sum'] = 0
            result_dict['trade_amount_mean'] = 0
            result_dict['amount_7days'] = []
            result_dict['amount_30days'] = []
            result_dict['amount_90days'] = []
            result_dict['child_product'] = set()
            result_dict['car_product'] = set()
            result_dict['apartment_product'] = set()
            result_dict['pet_product'] = set()
            result_dict['child_car_apartment_pet'] = []
    else:
        result_dict['trade_time_range'] = []
        result_dict['trade_time_days'] = 0
        result_dict['trade_amount_num'] = 0
        result_dict['trade_amount_sum'] = 0
        result_dict['trade_amount_mean'] = 0
        result_dict['amount_7days'] = []
        result_dict['amount_30days'] = []
        result_dict['amount_90days'] = []
        result_dict['child_product'] = set()
        result_dict['car_product'] = set()
        result_dict['apartment_product'] = set()
        result_dict['pet_product'] = set()
        result_dict['child_car_apartment_pet'] = []
    #绑定的卡
    data_bindcards = data_dict['bindcards']
    list_name_4 = ['card_name', 'phone_num', 'deposit_card_num', 'credit_card_num']
    deposit_card_num = 0
    credit_card_num = 0
    card_name = set()
    phone_num = set()
    for item_card in data_bindcards:
        if item_card['card_type'] == '储蓄卡':
            deposit_card_num += 1
        elif item_card['card_type'] == '信用卡':
            credit_card_num += 1
        card_name.add(item_card['card_name'])
        phone_num.add(item_card['phone_num'])
    result_dict['card_name'] = list(card_name)
    result_dict['phone_num'] = list(phone_num)
    result_dict['deposit_card_num'] = deposit_card_num
    result_dict['credit_card_num'] = credit_card_num

    #btbills
    data_bibills = data_dict['btbills']
    overdue_bill = []
    for item_btbills in data_bibills:
        if 'status' in item_btbills.keys():
            if item_btbills['status'] in [0, 3]:
                overdue_bill.append(item_btbills)
    result_dict['overdue_bill'] = overdue_bill
    print('************************')
    return (result_dict)

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


#userinfo: 'email', 'gender', 'mapping_id', 'real_name', 'xb_credit', 'phone_number','card_number', 'name', 'address','province','city',
#'phone_no','total_money','available_limit','credit_limit','credit_waitpay','delinquency_balance','tourcredit_limit', 'tourdelinquency_balance'
#'card_name':set, 'phone_num':set, 'deposit_card_num', 'credit_card_num','btbills_overdue'中的异常status==3或者0
if __name__ == '__main__':
    PATH = 'D:\\work\\2018_1_新的风控规则\\dianshang_json\\all_json\\'


    file_list = os.listdir(PATH)
    result_list = []
    columns = ['file_name', 'email', 'gender', 'mapping_id', 'real_name', 'xb_credit', 'phone_number','card_number','total_money',
               'available_limit','credit_limit','credit_waitpay','delinquency_balance','tourcredit_limit', 'tourdelinquency_balance',
               'trade_time_range','trade_time_days','trade_amount_num','trade_amount_sum','trade_amount_mean','amount_7days','amount_30days','amount_90days'
               ,'child_product','apartment_product','pet_product','car_product','child_car_apartment_pet',
               'province', 'city', 'name', 'phone_no',
               'card_name', 'phone_num', 'deposit_card_num', 'credit_card_num','overdue_bill']
    columns_1 = ['userinfo','userinfo','userinfo','userinfo','userinfo','userinfo','userinfo','userinfo',
                                     'wealth','wealth','wealth','wealth','wealth','wealth','wealth',
                 'tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails','tradedetails',
                                     'deliveraddresses','deliveraddresses','deliveraddresses','deliveraddresses',
                                     'bindcards','bindcards','bindcards','bindcards','btbills']

    for item_name in file_list:
        if 'json' in item_name and ('taobao' not in item_name):
            jd_dict = read_analysis_jd_data(item_name, PATH)
            result_list.append(jd_dict)

    data = DataFrame(result_list, columns=columns).values
    data = DataFrame(data, columns=[columns_1, columns])
    writer = pd.ExcelWriter(PATH + 'info_jd.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()
