#coding: utf-8
import json
import re
# import sys
import time

import pandas as pd
from pandas import DataFrame, Series

# from datetime import datetime
# import pymysql
from call_history_addr_alter import mysql_connection as my

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
    #不含中文的数据，或者含有国外名称的数据
    #既有一级行政区又有二级行政区的数据
    #只有二级行政区的数据
    #只有三级行政区的数据
    #其他

    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"
    file_name_2 = 'call_addr_old.csv'

    file = open(path + file_name, encoding='utf-8')
    #读取json文件
    data = json.load(file)
    data = Series(data)
    index_new = []
    for index in data.index:
        index_new.append(int(index))
    #原地址规则文件
    data = DataFrame(data, columns=['addr'])
    data['id'] = index_new
    #得到data表，含有addr，id字段————————————————————————————-----------------————
    # print(data)
    province = data[data['id']%10000==0]
    province['addr_new'] = 'NULL'

    #第一部分：原数据中含有省名和市名。
    print('找出原数据中的省名和市名，请稍候.............................................................................')
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
    # city = data[(data['id']%100==0) & (data['id']%10000!=0)]
    # 建立一个数据表province，只存放省名，含有addr，id ，addr_new----------------------------------------------------

    #data_2为原地址表，是处理的数据
    data_old = pd.read_table(path + file_name_2, encoding='utf-8', sep='\t')
    data_old['call_addr_new'] = "NULL"
    #flag判断地址字符中是否含有多个省名,如果只有一个，就确定它属于哪个省，如果有多个省名，则给出提示。
    data_old['flag_province'] = 0
    data_old['flag_city'] = 0
    data_old['flag_county'] = 0

    #找出既有省名又有市名的数据------------------------------------------------------------------------------------------------------------
    #遍历每一个省名
    for index_province in province.index:
        province_word = province.ix[index_province, 'addr_new']
        if province_word not in ['北京', '天津', '上海', '重庆', '香港', '澳门']:
            # print(province_word, '***********************************************************')
            #找到含有该省名的地址，比如 海南
            data_province_index = data_old[data_old["call_addr"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1
            # print(data_old)
            #遍历该省下的每一个市名，考虑直辖县，第三位为9的是直辖县，如429004，仙桃市。"469024":"临高县"。市的编码需要大于本省的编码，小于下一省的编码。普通城市编码小于
            #9000，且被100整除，或者大于9000的直辖县。
            for city_index in data[(data['id'] > int(index_province)) & (((data['id'] < int(index_province) + 9000) & (data['id'] % 100 == 0)
                                                                        |(data['id']>int(index_province)+8999)&(data['id']<int(index_province)+10000)))].index:
                city_word = data.ix[city_index, 'addr'][:-1]
                # print(city_word, '_______________')
                #找到地址字符中含有该城市数据索引
                #该省的data_old数据记录中，寻找对应市的记录
                data_old_province = data_old.ix[data_province_index]
                if city_word != '吉林':
                    #data_city_index为data_old中含有特定省名和市名的数据索引，
                    data_city_index = data_old_province[data_old_province['call_addr'].str.contains(city_word)].index
                    data_old.ix[data_city_index, 'flag_city'] = data_old.ix[data_city_index, 'flag_city'] + 1
                    # print(data_old.ix[data_city_index, 'call_addr'], '____________________________________')

                    #遍历每一个包含该城市名的原数据

                    for data_item in data_city_index:
                        if data_old.ix[data_item, 'flag_city'] == 1:
                            #让标准文件中的城市名作为新的地址
                            data_old.ix[data_item, 'call_addr_new'] = province.ix[index_province, 'addr'] +','+ data.ix[city_index, 'addr']
                        elif data_old.ix[data_item, 'flag_city'] > 1:
                            data_old.ix[data_item, 'call_addr_new'] = "多个城市"
                        else:
                            pass
                else:
                    #如果是吉林市，先不考虑
                    pass
        elif province_word in ['北京', '天津', '上海', '重庆']:
            #如果是这四个直辖市
            data_province_index = data_old[data_old["call_addr"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'call_addr_new'] = province_word + '市,' + province_word + '市'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1
        #香港、澳门
        else:
            data_province_index = data_old[data_old["call_addr"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'call_addr_new'] = province_word + '特别行政区'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1

    # 第二部分：原数据中是否含有中文和"未知"。
    print('正在判断原数据是否含有非中文和“未知”，请稍候.............................................................................')
    # print(data_old)
    #-------------------------------------------------------------------------------------------------------------------------------
    for item_2 in data_old.index:
        addr_old = data_old.ix[item_2, 'call_addr']
        #先看看地址中是否含有中文，不含有中文，新地址为“未知”；如果含有国外信息，新地址也为未知。
        if re.search(r'[\u4e00-\u9fa5]', addr_old):
            if re.search(r'[未知]', addr_old):
                data_old.ix[item_2, 'call_addr_new'] = '未知'
        else:
            data_old.ix[item_2, 'call_addr_new'] = '未知'
    #
    # # print(data_old)
    # #增加新列"call_addr_new"
    # data_old['call_addr_new'] = "NULL"

    #原数据中只有二级市名的情况------------------------------------------------------------------------------------------------------------------------------
    #目前中国的二级行政区由市和盟，自治州，地区组成，分别用data_2, data_3, data_4表示。另外还要考虑省辖县。
    data_2 = data[(data['addr'].str.contains('市')|(data['addr'].str.contains('盟'))|(data['id']%10000//1000==9))]
    for data_2_item in data_2.index:
        data_2.ix[data_2_item, 'addr'] = data_2.ix[data_2_item, 'addr'][:-1]
    # print(data_2)
    #自治州名字较长，取前两位，如临夏回族自治州
    data_3 = data[data['addr'].str.contains('自治州')]
    for data_3_item in data_3.index:
        data_3.ix[data_3_item, 'addr'] = data_3.ix[data_3_item, 'addr'][:2]

    data_4 = data[data['addr'].str.contains('地区')]
    for data_4_item in data_4.index:
        data_4.ix[data_4_item, 'addr'] = data_4.ix[data_4_item, 'addr'][:-2]
    #data_old是要处理的数据，data_2,data_3,data_4是要遍历的二级行政区，吉林省含有吉林市单独考虑

    data_other = data_old[(data_old['call_addr_new'] == 'NULL')&(~(data_old['call_addr'].str.contains('吉林')))]
    # print(data_other,'__________________________________________________')
    for city_2 in data_2.index:
        city_2_word = data_2.ix[city_2, 'addr']
        # print(city_2_word, '*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-')
        city_2_index = data_other[data_other['call_addr'].str.contains(city_2_word)].index
        # print(data_other[data_other['call_addr'].str.contains(city_2_word)], '------------------------------------------')
        #该市必须被100整除或者第三位为9(省辖县)
        if int(city_2)%100==0 or int(city_2)%10000//1000==9:
            province_word_2 = data.ix[str(data.ix[city_2, 'id']//10000*10000), 'addr']
            # print(province_word_2, '*********************************************************')
            data_old.ix[city_2_index, ['call_addr_new']] = province_word_2 + ',' + data.ix[city_2, 'addr']
            data_old.ix[city_2_index, 'flag_city'] = data_old.ix[city_2_index, 'flag_city'] + 1

    for city_3 in data_3.index:
        city_3_word = data_3.ix[city_3, 'addr']
        city_3_index = data_other[data_other['call_addr'].str.contains(city_3_word)].index
        # 该市必须被100整除或者第三位为9(省辖县)
        if int(city_3) % 100 == 0 or int(city_3)%10000//1000 == 9:
            province_word_3= data.ix[str(data.ix[city_3, 'id'] // 10000 * 10000), 'addr']
            # print(province_word_3, '*********************************************************')
            data_old.ix[city_3_index, ['call_addr_new']] = province_word_3 + ',' + data.ix[city_3, 'addr']
            data_old.ix[city_3_index, 'flag_city'] = data_old.ix[city_3_index, 'flag_city'] + 1

    for city_4 in data_4.index:
        city_4_word = data_4.ix[city_4, 'addr']
        city_4_index = data_other[data_other['call_addr'].str.contains(city_4_word)].index
        # 该市必须被100整除或者第三位为9(省辖县)
        if int(city_4) % 100 == 0 or int(city_4) %10000//1000 == 9:
            province_word_4 = data.ix[str(data.ix[city_4, 'id'] // 10000 * 10000), 'addr']
            # print(province_word_3, '*********************************************************')
            data_old.ix[city_4_index, ['call_addr_new']] = province_word_4 + ',' + data.ix[city_4, 'addr']
            data_old.ix[city_4_index, 'flag_city'] = data_old.ix[city_4_index, 'flag_city'] + 1


    #现在还有一部分数据只有三级行政区的名字， 如海拉尔,但不考虑直辖县，用data_county表示----------------------------------------------------------------
    #直辖市--------------------------------------------------
    # 第三部分：是否只含有三级行政区的名字。
    print('判断是否只含有三级行政区的名字，请稍候.............................................................................')
    data_bj_tj = data["110000": "120119"]
    data_sh = data['310000':"310151"]
    data_cq = data['500000':'500243']
    data_am_xg = data['810000':"820109"]
    #得到包括四个直辖市和两个特别行政区的数据，行索引不变
    data_unique = pd.concat([data_bj_tj, data_sh, data_cq])
    data_old_unique = data_old[(data_old['call_addr_new'] == 'NULL') & (~(data_old['call_addr'].str.contains('吉林')))]
    for index_unique in data_unique.index:
        unique_name = data_unique.ix[index_unique, 'addr']
        if len(unique_name)> 5:
            #地址太长，只取前两个
            unique_name = unique_name[:2]
        elif len(unique_name)<=2:
            #地址太长，只取前两个
            pass
        else:
            unique_name = unique_name[:-1]
        unique_old_index = data_old_unique[data_old_unique['call_addr'].str.contains(unique_name)].index
        data_old.ix[unique_old_index, 'call_addr_new'] = data.ix[str(int(index_unique)//10000*10000), 'addr'] + ',' + data.ix[str(int(index_unique)//10000*10000), 'addr']
        data_old.ix[unique_old_index, 'flag_county'] += 1

    #香港和澳门命名不一样
    data_old_am_xg = data_old[(data_old['call_addr_new'] == 'NULL') & (~(data_old['call_addr'].str.contains('吉林')))]
    for index_am_xg in data_am_xg.index:
        am_xg_name = data_am_xg.ix[index_am_xg, 'addr']
        if len(am_xg_name) > 5:
            # 地址太长，只取前两个
            am_xg_name = am_xg_name[:3]
        elif len(am_xg_name)<=2:
            am_xg_name = am_xg_name[:2]
        else:
            am_xg_name = am_xg_name[:-1]
        am_xg_old_index = data_old_am_xg[data_old_am_xg['call_addr'].str.contains(am_xg_name)].index
        data_old.ix[am_xg_old_index, 'call_addr_new'] = data.ix[str(int(index_am_xg) // 10000 * 10000), 'addr']
        data_old.ix[am_xg_old_index, 'flag_county'] += 1

    data_county = data[(data['id']%100 != 0)&(data['id']%10000//1000!=9)]
    list_county_drop = list(data_unique['id'])
    #extend没有返回值和append一样，所以直接修改
    list_county_drop.extend(list(data_am_xg['id']))
    #data_county不能包括直辖市和香港澳门，因为它们的二级行政区命名不一样。
    data_county = data_county[~data_county['id'].isin(list_county_drop)]
    data_county_1 = data_county[(data_county['addr'].str.contains('县'))|(data_county['addr'].str.contains('区'))|(data_county['addr'].str.contains('市'))]
    data_county_2 = data_county[data_county['addr'].str.contains('自治县')]
    #将data_old中处理的数据选进来，还有的县是属于直辖市和特别行政区，得去掉。前两位是11，31，50，12，81，82
    data_old_other_2 = data_old[(data_old['call_addr_new'] == 'NULL')&(~(data_old['call_addr'].str.contains('吉林')))]
    for county_index_1 in data_county_1.index:
        county_word_1 = data_county_1.ix[county_index_1, 'addr']
        if len(county_word_1) > 2:
            county_word_1 = county_word_1[:-1]
        county_old_index = data_old_other_2[data_old_other_2['call_addr'].str.contains(county_word_1)].index
        #找到data中找到省名和市名，并在data_old中修改“call_addr_new”
        province_word_5 = data.ix[str(data.ix[county_index_1, 'id'] // 10000 * 10000), 'addr']
        city_word_5 = data.ix[str(data.ix[county_index_1, 'id'] // 100 * 100), 'addr']
        data_old.ix[county_old_index, ['call_addr_new']] = province_word_5 + ',' + city_word_5
        data_old.ix[county_old_index,'flag_county'] += 1

    for county_index_2 in data_county_2.index:
        #对于自治县，只取前两个字符
        county_word_2 = data_county_2.ix[county_index_2, 'addr'][:2]
        county_old_index_2 = data_old_other_2[data_old_other_2['call_addr'].str.contains(county_word_2)].index
        #找到data中找到省名和市名，并在data_old中修改“call_addr_new”
        province_word_6 = data.ix[str(data.ix[county_index_2, 'id'] // 10000 * 10000), 'addr']
        city_word_6 = data.ix[str(data.ix[county_index_2, 'id'] // 100 * 100), 'addr']
        data_old.ix[county_old_index_2, ['call_addr_new']] = province_word_6 + ',' + city_word_6
        data_old.ix[county_old_index_2, 'flag_county'] += 1

    #生成excel-----------------------------------------------------------------------------------------------------------------------------------------
    writer = pd.ExcelWriter(path + file_name_2[:-4] + '_analysis.xlsx' )
    data_old.to_excel(writer, 'sheet1')
    writer.save()






read_local_data()



