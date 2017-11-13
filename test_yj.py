#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
from home_location import lookup_location
#本模块含有三个函数:
# add_locations(data):data数据表中必须包含主叫"mobile"和被叫"receiver"字段,将归属地作为新的字段添加到表中。
# lookup_location(target_number_0),输入字符串格式的手机号，得到一个列表，表示号码的所属的省名和市名。
# program_time(fun_1, a,fun_2=None, fun_3=None)：计算函数fun_的运行时间，a为fun_的参数。

print(lookup_location('13000005030'))

lookup_count =1
#data_home_location 总数量为:343150
data_home_location = pd.read_pickle('D:\\work\\database\\home_location.pkl')
#data_call_history 总数量为:102994
data_call_history = pd.read_pickle('D:\\work\\database\\call_history.pkl')

print('data_home_location 总数量为:%d'%len(data_home_location))
print(len(data_call_history.ix[20,['mobile']][0]))
print('data_call_history 总数量为:%d'%len(data_call_history))


#将全局变量重置
lookup_count =1