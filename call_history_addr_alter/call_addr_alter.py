#coding: utf-8
import re
import os
# import sys
import time
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
import json
# from datetime import datetime
# import pymysql
import mysql_connection as my
'''
1.利用mysql过滤：
call_addr为“未知”，“-”，“������������”及null

2.原数据形式：
“郑州”、“抚顺；沈阳；铁岭”（可能不在同一个省）、“海南省海口市”、“海南省.海口市”、“湖南永州(冷水滩)”、“上海市.上海市”、“西安.咸阳”、“美国/加拿大”、"其他”

3.统一格式：
“河南省,郑州市”、“北京市”、“广西壮族自治区,钦州市”、
生成新的表

一级行政区，33个
二级行政区，334个（地级市293个，自治州30个，地区8个，盟3个）
三级行政区，2856个（市辖区958个，县1367个，县级市360个，自治县117个，特区1个，林区1个，旗49个，自治旗3个）

问题：
县级市需要考虑么
同一个"mobile"对应多个"call_addr"
最终提供一个csv的本地数据文件
'''


#读取省市规则的json文件，并转换成Series格式
def read_addr_json():
    print('read_addr_json()函数开始执行，请稍候...')
    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"

    with open(path + file_name,'r') as json_file:
        data = json_file.read()
    #data应该为字典格式
    print(type(data))
    print(data)
    data_s = Series(data)
    print(data_s)
    return data_s


#使用mysql语句将数据库中文件生成本地csv文件，方便后续操作，无返回值。
def get_local_data():
    #计算时间
    start_1 = time.time()
    #路径
    path = 'D:\\work\\database\\province-city\\'
    file_name = 'call_addr_old.csv'
    file_name_test = 'call_addr_alter_test.csv'

    print('get_local_data()函数正在执行，请稍候...')
    select_string = '''
select call_addr from call_history 
group by call_addr
    '''
    columns = ['call_addr']
    data = my.mysql_connection(select_string, columns)
    print('正在生成本地文件，请稍候...')
    data.to_csv(path + file_name, index=False, sep='\t', encoding='utf-8')
    #生成excel
    writer = pd.ExcelWriter(path + file_name[:-4] + '.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()
    print('本地文件已生成，获取的数据一共%d行\n'%len(data))
    print('函数已经结束，共花费时间%d'%(time.time()-start_1))


#读取本地的通话记录文件，并增加新列
def read_local_data():
    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"
    file_name_2 = 'call_addr_old.csv'

    file = open(path + file_name, encoding='utf-8')
    #读取json文件
    data = json.load(file)
    data = Series(data)
    index_new = []
    list = []
    for index in data.index:
        index_new.append(int(index))
    data = DataFrame(data, columns=['addr'])
    data['id'] = index_new

    province = data[data['id']%10000==0]
    province['addr_new'] = 'NULL'
    index_item = province[(province['addr'].str.contains('省'))| (province['addr'].str.contains('市')) ].index
    for item_1 in index_item:
        province.ix[item_1, 'addr_new'] = province.ix[item_1,'addr'][:-1]
    province.ix['150000', 'addr_new'] = '内蒙'
    province.ix['450000', 'addr_new'] = '广西'
    province.ix['540000', 'addr_new'] = '西藏'
    province.ix['640000', 'addr_new'] = '宁夏'
    province.ix['650000', 'addr_new'] = '新疆'
    province.ix['810000', 'addr_new'] = '香港'
    province.ix['820000', 'addr_new'] = '澳门'
    # print(province)
    # city = data[(data['id']%100==0) & (data['id']%10000!=0)]


    #data_2为原地址表，是处理的数据
    data_old = pd.read_table(path + file_name_2, encoding='utf-8', sep='\t')
    data_old['call_addr_new'] = "NULL"
    #flag判断地址字符中是否含有多个省名,如果只有一个，就确定它属于哪个省
    data_old['flag'] = 0
    for item_3 in province.index:
        province_word = province.ix[item_3, 'addr_new']
        #找到含有该省名的地址，比如 海南
        item_3_province = data_old[data_old["call_addr"].str.contains(province_word)].index
        data_old.ix[index, 'flag'] = data_old.ix[index, 'flag'] + 1
        for item_4 in item_3_province:
            if data_old.ix[item_4 , 'flag'] == 1:
                data_old.ix[item_4, 'call_addr']
            elif data_old.ix[item_4 , 'flag'] > 1:
                pass
            else: 
                pass


        data_old.ix[item_3_province, ['call_addr_new']] = province.ix[item_3, 'addr']

    print(data_old)

    for item_2 in data_old.index:
        addr_old = data_old.ix[item_2, 'call_addr']
        #先看看地址中是否含有中文，不含有中文，新地址为“未知”
        if re.search(r'[\u4e00-\u9fa5]', addr_old):
            pass
        else:
            data_old.ix[item_2, 'call_addr_new'] = '未知'

    # print(data_old)
    #增加新列"call_addr_new"
    data_old['call_addr_new'] = "NULL"


    # print(data_old)

#修改原通话记录中的call_addr
def alter_addr():
    print('alter_addr()函数正执行，请稍候...')
    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"

    addr_rule = read_addr_json()
    iterator = pd.read_table(path + file_name, encoding='utf-8', sep='\t', chunksize=500000)

    for data in iterator:
        data['call_addr_new'] = 'NULL'
        for mobile, group in data.groupby('mobile'):
            group['call_addr'].drop_duplicates(index=False)


read_local_data()



