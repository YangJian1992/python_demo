#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
import numpy as np
#本模块含有三个函数:
# add_locations(data):data数据表中必须包含"mobile"和联系人"receiver"字段,将归属地作为新的字段添加到表中。
# lookup_location(target_number_0),输入字符串格式的手机号，得到一个列表，表示号码的所属的省名和市名。
# program_time(fun_1, a,fun_2=None, fun_3=None)：计算函数fun_的运行时间，a为fun_的参数。


#根据手机号找到归属地。data为包含手机号的DataFrame数据
def add_locations(data):
        print('输入参数的类型为%s'%type(data))
        print('输入参数的长度%d'%len(data))
        data = DataFrame(data, columns=["id", 'mobile', 'mobile_province', 'mobile_city', 'mobile_addr',  'receiver',
                                        'receiver_province', 'receiver_city', 'receiver_addr','call_time', 'call_addr','call_type'])
        column_tem1 = 'mobile'
        column_tem2 = 'receiver'
        print('\n正在处理数据，请稍候......')
        for index_data in data.index:
            # data.index是一个序列，所以index_data是一个整数int
            print('正在查找第%d条数据'%index_data)
            target_1 = (data.ix[index_data, [column_tem1]].values[0])
            target_2 = (data.ix[index_data, [column_tem2]].values[0])
            #lookup_location函数的返回结果为一个列表
            result_list_1 = lookup_location(target_1)
            result_list_2 = lookup_location(target_2)
            #data.ix[index,['a'] = xx。改变index索引对应的若干行或一行，'a'列的值
            # if len(target_1)==11:
            data.ix[index_data, ['%s_province'%column_tem1]] = result_list_1[0]
            data.ix[index_data, ['%s_city'%column_tem1]] = result_list_1[1]
            data.ix[index_data, ['%s_addr'%column_tem1]] = result_list_1[0] +','+ result_list_1[1]
            #
            # else:
            #     data.ix[index_data, ['%s_province'%column_tem1]] = '0'
            #     data.ix[index_data, ['%s_city'%column_tem1]] = '0'
            #     data.ix[index_data, ['%s_addr'%column_tem1]] = '0'
            # if len(target_2)==11:
            data.ix[index_data, ['%s_province'%column_tem2]] = result_list_2[0]
            data.ix[index_data, ['%s_city'%column_tem2]] = result_list_2[1]
            data.ix[index_data, ['%s_addr'%column_tem2]] = result_list_2[0] +','+ result_list_2[1]
            # else:
            # data.ix[index_data, ['%s_province'%column_tem2]] = '0'
            # data.ix[index_data, ['%s_city'%column_tem2]] = '0'
            # data.ix[index_data, ['%s_addr'%column_tem2]] = '0'

        print('------返回新的数据表：------')
        print(data)
        print('\n------函数add_locations运行结束------')
        return data


def lookup_location(target_number_0):
    #如果手机号码符合规则，则执行查找程序。否则返回的列表中，的省和市都为'0'。
    #需要在正则表达式中添加^$表示开始和结束的位置，保证只有11位。
    if re.findall(r"^1[3-8]\d{9}$",target_number_0):
        target_number = int(target_number_0[:7])
        #先用sort_values对手机号排序，才能用二分法查找。但是，按mobile排序后的索引值不是按序排列的，需要建立新的索引。
        #先把数据按"mobile"排序，但需要重新设置索引值。所以把排序后结果转换成数组，再转换成DataFrame，这样就把排序后的索引的值改变了。
        #DataFrame的index和columns都是从0开始的数字序列，没有mobile这样的字段名
        mobile_series = data_home_location_order['mobile']
        global lookup_count
        low = 0
        high = len(mobile_series) - 1
        # print('------正在进行第%d次查找，  手机号：%s  ，请稍候......------' % (lookup_count, target_number_0))
        lookup_count += 1
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
                return [data_home_location.ix[mid, ['province', 'city']].values[0],
                          data_home_location.ix[mid, ['province', 'city']].values[1]]
        # print('找不到手机号%s'%target_number_0)
        return ['0','0']
    else:
        # print('手机号不符合规则%s' % target_number_0)
        return ['-1', '-1']
    # print('不好意思，没有找到 手机号：%d。\n'%target_number)


#判断程序运行时间，不知道是否合适。如果只有两个参数，第二个参数会给fun_2么？
def program_time(fun_1, a,fun_2=None, fun_3=None):
    start = time.time()
    #文件存在时会一直报错
    while True:
        #生成的目标文件名
        file_path = 'D:\\work\\database\\home_location_new2.txt'
        if os.path.exists(file_path):
            print('错误：%s文件已经存在了'%file_path)
        else:
            break
    data = fun_1(a)
    #index为False，则生成的文本中不显示index。to_csv可以生成txt的文本
    data.to_csv(file_path, index=False, sep='\t')
    # file_data = open(file_path, 'w')
    # file_data.close()
    if fun_2 != None:
        fun_2()
        print('fun_2执行结束')
        if fun_3 != None:
            fun_3()
            print('fun_3执行结束')
    print('-------------------------------------------程序运行结束！-------------------------------------------\n')
    end = time.time()
    print('\n运行时间为:%d秒'%(end-start))

#注意这些都是全局变量
lookup_count =1
#data_home_location 总数量为:343150
data_home_location = pd.read_pickle('D:\\work\\database\\home_location.pkl')
#data_call_history 总数量为:102994
data_call_history = pd.read_pickle('D:\\work\\database\\call_history.pkl')
#
# print('data_home_location 总数量为:%d'%len(data_home_location))
# print(len(data_call_history.ix[20,['mobile']][0]))
# print('data_call_history 总数量为:%d'%len(data_call_history))

#对data_home_location排序，并改变索引值，让索引从0开始。放在函数外面，提高效率。
data_home_location_order = DataFrame(np.array(data_home_location.sort_values(by='mobile')),columns=data_home_location.columns)

program_time(add_locations, data_call_history)

#将全局变量重置
lookup_count =1