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


def read_analysis_jd_data(filename, PATH):
    with open(PATH + filename, encoding='utf-8') as file:
        data_dict = json.load(file)
    for key, item in data_dict.items():
        print(key, item , '\n************************\n')
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
               'province', 'city', 'name', 'phone_no',
               'card_name', 'phone_num', 'deposit_card_num', 'credit_card_num','overdue_bill']
    columns_1 = ['filename','userinfo','userinfo','userinfo','userinfo','userinfo','userinfo','userinfo',
                                     'wealth','wealth','wealth','wealth','wealth','wealth','wealth',
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
