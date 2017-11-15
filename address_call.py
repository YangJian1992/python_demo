#coding: utf-8
'''
功能：
    近1个月通话记录中通讯录联系人通话次数小于等于3次
    近3个月通话记录中通讯录联系人通话次数小于等于8次
    近3个月通话记录中通讯录联系人有通话的联系人数量小于等于2个
    近1个月top10联系人无通讯录联系人

write by yang_jian

导入pymysql模块，可以直接地把数据读取到内在中。这里没用到，先备着，以后可能会用。
conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao', passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
cur = conn.cursor()
'''

import pandas as pd
from pandas import Series, DataFrame

path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
file_name_1 = 'one_month_call.csv'
file_name_2 = 'three_month_call.csv'
file_name_3 = 'address_book.csv'

one_month = pd.read_csv(path + file_name_1)
three_month = pd.read_csv(path + file_name_2)
address_book = pd.read_csv(path + file_name_3)


def call_frequency(FRE_NUM= 3, data = one_month):
    # print(one_month)
    #call_num_list列表用来存放mobile和对应的top10的receiver
    call_num_list = []

    #先根据mobile分类，再和通讯录联系起来，但通讯录和通话记录没有联系
    for name, group in data.groupby('mobile'):
        mobile = str(name)
        # print(str(mobile))
        pass

        #通话记录top10联系人
        #groupby().count()返回一个DataFrame，但索引值已不再是data中的索引，而是‘receiver’的值,数据为对应的统计数量，但字段中去除了'receiver',所以排序用'mobile'字段
        #ascending=False表示降序排列，取前十位，如果数量不到10，会自动取最大行数，还是得到一个DataFrame.
        # 取index对象即为'receiver'的值，.values为存放top10联系人手机号的列表
        list = group.groupby('receiver').count().sort_values('mobile', ascending=False)[:10].index.values
        #将mobile用户和对应的top10联系人手机号列表，放在一个列表中，并都添加到call_num_list中
        call_num_list.append([str(name), list])

