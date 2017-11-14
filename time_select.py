#coding: utf-8
import re
import os
import sys
import time
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime

path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
file_name_1 = 'addressBookTest.xlsx'
file_name_2 = 'callHistroyTest.xlsx'
file_name_3 = 'operator_info_demo.xlsx'
# file_name_3 = 'operator_info_demo.csv'

# #本地数据是csv格式的，是用\t分隔operator_info_demo，每行后面有\n，空值为"NULL'。
# file_open = open(path + file_name,'r').readlines()
# #为什么 re.findall(r'^[^\t]+\t$', row[:-1]+'\t'))就不行啊？
# data_temp = [[temp[:-1] for temp in (re.findall(r'[^\t]+\t', row[:-1]+'\t'))] for row in file_open]
# data = DataFrame(data_temp[1:],columns=data_temp[0])
# print(data)

#读取xls或者xlsx的文件
file_open_1 = pd.ExcelFile(path + file_name_1)
address_book_test = file_open_1.parse('Table1')

file_open_2 = pd.ExcelFile(path + file_name_2)
call_history_test = file_open_2.parse('Table1')


#有问题，read_csv和read_table读取，会报错。
# file_open_3 = pd.read_table(path + file_name_3)
# operator_info_demo = file_name_3
file_open_3 = pd.ExcelFile(path + file_name_3)
operator_info = file_open_3.parse('Table1')


#定义dat_time()函数，输入数字，如1，3，得到最近1和3个月的通话记录如：[data, data_2]。
def data_time_select(month_num, month_num_2, data):
    #调用sort_data()函数，对operator_info数据表进行排序
    sort_operator_info = sort_data(operator_info, 'mobile')
    #根据手机号去重，得到新的表
    # data_mobile = data.drop_duplicates['mobile']
    #按照mobile分组，并迭代
    data_2 = data.copy()
    # month_2 = 3
    # data_3 = data.copy()
    for name, group in data.groupby('mobile'):
        # print("手机号%s的类型为%s" % (name, type(name)))
        #
        #
        #不知道groupyby()分组后，为什么call_history_test中name的类型为numpy.int64，但对于data_call_history,name的类型为str。
        #
        #所以加了个str，但对于其他数据，不一定要加
        #
        target_mobile = str(name)
        #date为create_time，调用函数查询得到
        date = lookup_date(target_mobile, sort_operator_info)
        print(date)
        if len(date) == 19:
            # 先把日期转换成时间数组，再转换成时间戳。
            time_array = time.strptime(date, '%Y-%m-%d %H:%M:%S')
            time_stamp = time.mktime(time_array)
            # 把函数的输入参数，由月转换成秒，得到新的时间戳。
            time_stamp_pre = time_stamp - month_num * 30 * 24 * 3600
            time_stamp_pre_2 = time_stamp - month_num_2 * 30 * 24 * 3600
            #再把计算最近几个月后的时间戳转换成普通的日期格式
            target_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_stamp_pre))
            target_date_2 = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_stamp_pre_2))
            #把不在时间范围内的数据的索引找到，并用drop删除，inplace为true,可以改变原data

            #不但要在date之前的一个月和三个月，也不超过date。注意用&符号，不能用and,&两侧用（）分开，这个bug找了好长时间，然而，找出来的索引是我要删除的，
            #所以不能用and，应该把我不想要的索引找出来，再删掉，所以用或运算 。
            # &  group['call_time'] <date
            index_tem = group[(group['call_time'] < target_date) | (group['call_time'] > date)].index
            index_tem_2 = group[(group['call_time'] < target_date_2) | (group['call_time'] > date)].index

            data.drop(index_tem, axis=0, inplace=True)
            data_2.drop(index_tem_2, axis=0, inplace=True)



    # # for index in data_mobile.index:
    #     # ，选择索引对应的手机号，而lookup_date()返回日期
    #     target_mobile = data_mobile.ix[index, ['mobile']][0]
    #
    #     #将排序后的opreator_info作为参数传入，在表里找到相应的日期。
    #     date = lookup_date(target_mobile, sort_operator_info)
    #     if len(date) == 19:
    #         # 先把日期转换成时间数组，再转换成时间戳。
    #         time_array = time.strptime(date, '%Y-%m-%d %H:%M:%S')
    #         time_stamp = time.mktime(time_array)
    #         # 把函数的输入参数，由月转换成秒，得到新的时间戳。
    #         time_stamp_pre = time_stamp - month_num * 30 * 24 * 3600
    #         # time.strftime("%b %d %Y %H:%M:%S", time.gmtime(time_stamp_pre))
    #     #将call_time的数据转换成秒，再减掉要求的时间间隔，和表中的时间戳。
    #         data_stamp = time.mktime(time.strptime(call_date, '%Y-%m-%d %H:%M:%S'))
    #         if data_stamp <= time_stamp_pre:
    #             #不在日期范围内的删除。
    #             # data.drop(index, axis=0, inplace=True)
    #             #去掉不符合日期条件的数据，把返回结果赋值给data
    #             data = data[data['mobile'] != target_mobile]
    #     elif date == "0":
    #         #表示找不到这个手机号对应的create_time
    #          print("找不到手机号%s对应的日期\n"%target_mobile)
    #          continue
    #     else:
    #         print("手机号%s的格式不正确\n"%target_mobile)
    #         continue
    return [data, data_2]



#返回一个排序的DataFrame，按第二个参数排序。data为DataFrame，column_0为排序的字段，index_flag为None时，返回的数据索引值已变化。
def sort_data(data, column_0, index_flag  = None):
    #如果只有前两个参数，返回的表不但排序，而且每一行的数值和相应的索引值已经不再和data一致。对索引重新排序。
    if index_flag == None:
        # 先用sort_values对手机号排序，才能用二分法查找。但是，按mobile_0排序后的索引值不是按序排列的，需要建立新的索引。
        # 先把数据按column_0排序，但需要重新设置索引值。所以把排序后结果转换成数组，再转换成DataFrame，这样就把排序后的索引的值改变了。
        return DataFrame(np.array(data.sort_values(by=column_0)), columns=data.columns)
    else:
        return data.sort_values(by=column_0)

#data要求是排序过的，按target_number所在的列
def lookup_date(target_number_0, data):
    # data = operator_info

    #如果手机号码符合规则，则执行查找程序。否则返回的列表中，的省和市都为'0'。
    #需要在正则表达式中添加^$表示开始和结束的位置，保证只有11位。
    if re.findall(r"^1[3-8]\d{9}$", target_number_0):
        target_number = int(target_number_0)
        #先用sort_values对手机号排序，才能用二分法查找。
        # mobile_series = operator_info['mobile']
        mobile_series = data['mobile']
        low = 0
        high = len(mobile_series) - 1
        # print('------正在进行第%d次查找，  手机号：%s  ，请稍候......------' % (lookup_count, target_number_0))
        #二分法查找，当找不到数据时，会跳出while的循环。
        while (low <= high):
            # 用//取整数
            mid = (low + high) // 2
            midval = int(mobile_series[mid])
            if midval < target_number:
                low = mid + 1
            elif midval > target_number:
                high = mid - 1
            elif midval == target_number:
                # print('已找到 手机号:%s， \n归属地为：%s,%s' % (
                # target_number_0, data_home_location.ix[mid, ['province', 'city']].values[0],
                # data_home_location.ix[mid, ['province', 'city']].values[1]))
                # print('------手机号在data_home_location中的索引为%d------\n' %mid)
                return data.ix[mid, ['create_time']].values[0]
        # print('找不到手机号%s'%target_number_0)
        return '0'
    else:
        # print('手机号不符合规则%s' % target_number_0)
        return '-1'


# print(call_history_test)
print(data_time_select(1, 3, call_history_test))
# print(type((data_time_select(1, 3, call_history_test))))