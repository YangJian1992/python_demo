#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
#本模块含有三个函数:
# add_locations(data):data数据表中必须包含主叫"mobile"和被叫"receiver"字段,将归属地作为新的字段添加到表中。
# lookup_location(target_number_0),输入字符串格式的手机号，得到一个列表，表示号码的所属的省名和市名。
# program_time(fun_1, a,fun_2=None, fun_3=None)：计算函数fun_的运行时间，a为fun_的参数。


lookup_count =1
#data_home_location 总数量为:343150
data_home_location = pd.read_pickle('D:\\work\\database\\home_location.pkl')
#data_call_history 总数量为:102994
data_call_history = pd.read_pickle('D:\\work\\database\\call_history.pkl')

# print('data_home_location 总数量为:%d'%len(data_home_location))
b = data_call_history.copy()

# print(data_call_history)
print(data_call_history[(data_call_history['call_time']<'2017-07-01 18:41:47' )& (data_call_history['call_time']>'2017-07-01 07:40:48')])
# for name, group in data_call_history.groupby('mobile'):
#     # index_tem = group[group['call_time']<"2017-12-30 02:09:34"].index
#     # data_call_history.drop(index_tem, axis=0, inplace=True)
#     print(type(name))
#
# print(data_call_history)
# print('data_call_history 总数量为:%d'%len(data_call_history))
# date = "2017-12-30 02:09:34"
# time_array = time.strptime(date, '%Y-%m-%d %H:%M:%S')
# time_stamp = time.mktime(time_array)
# # 把函数的输入参数，由月转换成秒，得到新的时间戳。
# time_stamp_pre = time_stamp - 3 * 30 * 24 * 3600
# target_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_stamp_pre))
# print(target_date)


#将全局变量重置
lookup_count =1