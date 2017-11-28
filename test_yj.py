#coding:utf-8
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
from urllib import request
from urllib import parse

def address_book_remove(data):
    print('输入的数据共%d行'%len(data))
    print('正在对数据进行筛选，请稍候...')
    start2 = time.time()
    #手机号的字段,通话记录中是'receiver，联系人中是mobile
    mobile = 'mobile'
    data[mobile] = data[mobile].astype('str', errors='raise')
    pattern = re.compile(r'^1[3-8][0-9]{9}$')
    data_filter = data[data[mobile].str.match(pattern)]
    print('通讯录11位筛选，正在生成数据，请稍候...累计花费时间为%ds。' % (time.time() - start2))

    print('得到数据共%d行' % len(data_filter))
    return data_filter



def address_book_remove_2(data, mobile_num=9):
    start = time.time()
    print('本次处理的数据一共%d行。address_book_remove_2()程序正在执行，请稍候...'%len(data))
    #为data增加一列，用于判断手机号前9位重复情况
    data['name'] = data['name'].astype('str', errors='raise' )
    columns_add = 'mobile_%d'%mobile_num
    data['mobile_9'] = 'NULL'
    word_list = ['加粉','粉丝', '提醒', '专线', '易信', '微会', 'poo', 'mimicall', 'Skype', '来电', '阿里通', '有信', '触宝', '微话',
                 '钉钉', '酷宝', '微信']
    # word_list = ['加粉','专线', '易信', '微会', 'poo']
    for word in word_list:
        word_index = data[data['name'].str.contains(word)].index
        if len(word_index)!=0:
            data.drop(word_index, axis=0, inplace=True)
            #取前多少位str.slice， 找了好长时间才找到，好好记住。
            data[columns_add] = data['mobile'].str.slice(0, mobile_num)
    print('address_book_remove_2()函数运行结束，请稍候...花费时间为%ds。\t' % (time.time() - start))
    return data


def address_book_remove_3(data, mobile_num=9):
    start = time.time()
    columns_add = 'mobile_%s'%mobile_num
    data[columns_add] = data[columns_add].astype('str', errors='raise')
    #用来存放用户名和有效的手机号
    user_remove_list = []
    print('数据一共%d行。\naddress_book_remove_3()函数正在执行，请稍候...' % len(data))
    for user_id, group in data.groupby('user_id'):
        user_id = str(user_id)

        # for item in risky_mobile:
        #     # 如果含有号码item，则计数变量risky_num加一
        #     risky_item = mobile_only[mobile_only['mobile'] == item]
        #     if len(risky_item):
        #         risky_num += 1

        # mobile_data为DataFrame，索引为mobile_9,值为mobile_9的数量,但字段中去除了columns_add字段,所以用'mobile'字段表示>10
        mobile_data = group.groupby(columns_add).count()
        # mobile_remove为一个列表，表示要删除的九位数字的手机号
        #mobile_need为一个列表，表示要保留的九位数字的手机号
        mobile_remove = mobile_data[mobile_data['mobile'] > 20].index.values
        if len(mobile_remove)> 0:
            print(user_id)
            user_remove_list.append([user_id, list(mobile_remove)])
            print('删除的九位手机号：--------------------------------------------------------------------', mobile_remove)
    print('-------------------address_book_remove_3()函数执行结束,共花费%s，请稍候...------------------\n'%(time.time()-start))
    return user_remove_list


def address_book_remove_4(data):
    start = time.time()
    columns_add = 'name'
    data[columns_add] = data[columns_add].astype('str', errors='raise')
    #用来存放用户名和有效的手机号
    user_remove_list = []
    print('数据一共%d行。\naddress_book_remove_3()函数正在执行，请稍候...' % len(data))
    for user_id, group in data.groupby('user_id'):
        user_id = str(user_id)

        # for item in risky_mobile:
        #     # 如果含有号码item，则计数变量risky_num加一
        #     risky_item = mobile_only[mobile_only['mobile'] == item]
        #     if len(risky_item):
        #         risky_num += 1

        # mobile_data为DataFrame，索引为mobile_9,值为mobile_9的数量,但字段中去除了columns_add字段,所以用'mobile'字段表示>10
        mobile_data = group.groupby(columns_add).count()
        # mobile_remove为一个列表，表示要删除的九位数字的手机号
        #mobile_need为一个列表，表示要保留的九位数字的手机号
        mobile_remove = mobile_data[mobile_data['mobile'] > 10].index.values
        if len(mobile_remove)> 0:
            print(user_id)
            user_remove_list.append([user_id, list(mobile_remove)])
            print('重复十次以上的姓名：--------------------------------------------------------------------', mobile_remove)
    print('-------------------address_book_remove_4()函数执行结束,共花费%s，请稍候...------------------\n'%(time.time()-start))
    return user_remove_list


def get_result(mobile_num=9):
    user_list = []
    PATH = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
    FILE = 'address_book_old.csv'
    data = pd.read_table(PATH + FILE, sep='\t', encoding='gbk', chunksize=1000000)
    for data_item in data:
        print('\n****************************************************temp start***************************************')
        data_remove_1 =address_book_remove(data_item)
        data_remove_2 = address_book_remove_2(data_remove_1, mobile_num)
        user_list.extend(address_book_remove_4(data_remove_2))
        print(user_list)
        print('***************************************************************temp end**************************************\n\n')
    DataFrame(user_list, columns=['user_id', 'target']).to_csv(PATH + 'user_list%d'%mobile_num+'.csv', encoding='utf-8', sep = '\t', index=False)
    print("\nall end")

if __name__ == "__main__":
    get_result(9)
    get_result(8)
    get_result(7)