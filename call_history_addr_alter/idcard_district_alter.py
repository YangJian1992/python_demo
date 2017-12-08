import json
import re
# import sys
import time
import pandas as pd
from pandas import DataFrame, Series
from call_history_addr_alter import mysql_connection as my
"""
数据库idcard_district中的地址转换成统一格式，参考call_addr_alter.py文件。
1.根据地区编号来查找。先改写直辖市，特别行政区，再考虑省辖县，
"""

#如果只是根据地址编号添加统一格式，可以用下面这个函数
def district():
    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule"
    # 读取json文件
    with open(path + file_name + '.json', encoding='utf-8') as file:
        data = json.load(file)

    data = Series(data)
    print(data.reset_index().dtypes)
    index_new = []
    for index in data.index:
        index_new.append(int(index))
    #原地址规则文件
    data = DataFrame(data, columns=['addr'])

    # data.reset_index(inplace=True)
    # print(data)
    data['id'] = index_new
    print(data.dtypes)
    # # data.to_csv(path + file_name + '.csv', sep='\t', encoding='utf-8', index=False)
    # # writer = pd.ExcelWriter(path + file_name + '.xlsx')
    # # data.to_excel(writer, 'sheet')
    # # writer.save()
    # # data_old = pd.read_excel(path + file_name + '_all.xlsx', sheetname=1)
    # # print(data_old.dtypes)
    # # print(data_old)
    data['new_addr'] = 'null'
    data['province_code'] = data['id']//10000
#北京、天津、上海，重庆，澳门、香港
    # print(data[data['id']//10000==11])
    for num in [11, 12, 31, 50]:
        index_zhixia = data[data['id']//10000==num].index
        item_zhixia = data[data['id']==num*10000]['addr'].values
        data.ix[index_zhixia, ['new_addr']] = item_zhixia + ',' + item_zhixia
        # print( data.ix[index_zhixia, ['new_addr']])
    data.ix["810000":"810118", 'new_addr'] = '香港特别行政区'
    data.ix["820000":"820109", 'new_addr'] = '澳门特别行政区'
#直辖县，第三位为9， 如469001	五指山市
    county_direct_index = data[data['id']%10000//1000==9].index
    # print(data.ix[county_direct_index])
    for index in county_direct_index:
        province = data.ix[index[0:2] + '0000', 'addr']
        data.ix[index, ['new_addr']] = province + ',' + data.ix[index, 'addr']
    print(data.ix['429004'])
#普通的二级行政区
    for item, group in data.groupby('province_code'):
        province = group.ix[str(item) + '0000', 'addr']
        for index_2 in group.index:
            if group.ix[index_2, 'new_addr'] == 'null':
                #不是省名
                if data.ix[index_2, 'id']%10000!=0 and data.ix[index_2, 'id']%10000//1000!=9:
                    city_2 = group.ix[index_2[:4] + '00', 'addr']
                    data.ix[index_2, 'new_addr'] = province + ',' + city_2
    print(data)

    writer = pd.ExcelWriter(path + file_name + '_alter.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()

#使用mysql语句将数据库中文件生成本地csv文件，方便后续操作，无返回值。
def get_local_data():
    #计算时间
    start_1 = time.time()
    #路径
    path = 'D:\\work\\database\\province-city\\'
    file_name = 'idcard_district_old'

    print('get_local_data()函数正在执行，请稍候...')
    select_string = '''
select * from idcard_district
    '''
    columns = ['id', 'province', 'city', 'county', 'scale', 'type']
    data = my.mysql_connection(select_string, columns)
    print('正在生成本地文件，请稍候...')
    data.to_csv(path + file_name + '.csv', index=False, sep='\t', encoding='utf-8')
    #生成excel
    writer = pd.ExcelWriter(path + file_name + '.xlsx')
    data.to_excel(writer, 'sheet1')
    writer.save()
    print('本地文件已生成，获取的数据一共%d行\n'%len(data))
    print('函数已经结束，共花费时间%d'%(time.time()-start_1))


def read_local_data():
    #既有一级行政区又有二级行政区的数据
    #其他

    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"
    file_name_2 = 'idcard_district_old.csv'

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
    data_old['idcard_addr_new'] = "null"
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
            data_province_index = data_old[data_old["city"].str.contains(province_word)].index
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
                    data_city_index = data_old_province[data_old_province['city'].str.contains(city_word)].index
                    data_old.ix[data_city_index, 'flag_city'] = data_old.ix[data_city_index, 'flag_city'] + 1
                    # print(data_old.ix[data_city_index, 'city'], '____________________________________')

                    #遍历每一个包含该城市名的原数据

                    for data_item in data_city_index:
                        if data_old.ix[data_item, 'flag_city'] == 1:
                            #让标准文件中的城市名作为新的地址
                            data_old.ix[data_item, 'idcard_addr_new'] = province.ix[index_province, 'addr'] +','+ data.ix[city_index, 'addr']
                        elif data_old.ix[data_item, 'flag_city'] > 1:
                            data_old.ix[data_item, 'idcard_addr_new'] = "多个城市"
                        else:
                            pass
                else:
                    #如果是吉林市，先不考虑
                    pass
        elif province_word in ['北京', '天津', '上海', '重庆']:
            #如果是这四个直辖市
            data_province_index = data_old[data_old["city"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'idcard_addr_new'] = province_word + '市,' + province_word + '市'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1
        #香港、澳门
        else:
            data_province_index = data_old[data_old["city"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'idcard_addr_new'] = province_word + '特别行政区'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1

    writer = pd.ExcelWriter(path + file_name_2[:-4] + '_analysis.xlsx')
    data_old.to_excel(writer, 'sheet1')
    writer.save()


if __name__ == '__main__':
    # get_local_data()
    # district()
    read_local_data()