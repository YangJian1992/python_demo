# coding:utf-8
import re
import os
import sys
import time
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime
import pymysql
'''
功能：对通话记录进行筛选
函数：
data_time_select(month_num, month_num_2, data)：根据时间筛选，如：进件日期近一个月和进件日期最近三个月

'''

#从数据库中读取通话记录
def fun_readdata_mysql(select_string, columns_add):
    """
        创建连接读取mysql数据：
        select_string:用以筛选数据库数据的语句
    """
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao', passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    print('正在对数据库进行查询，请稍候...')
    cur.execute(select_string)

    temp = DataFrame(list(cur.fetchall()), columns=columns_add)
    print('已经查询到数据，正在处理，请稍候...累计花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return(temp)


#定义dat_time()函数，输入数字，如1，3，得到最近1和3个月的通话记录如：[data, data_2]。
def data_time_select(month_num, month_num_2, data):
    print('函数data_select开始执行，请稍候...')
    #调用sort_data()函数，对operator_info数据表进行排序
    # sort_operator_info = sort_data(operator_info, 'mobile')
    #根据手机号去重，得到新的表
    # data_mobile = data.drop_duplicates['mobile']
    #按照mobile分组，并迭代、
    #把data_1和data_2中不符合条件的去掉，复制一份就行，减少内存
    # data_1 = data.copy()
    data_2 = data.copy()
    # month_2 = 3
    # data_3 = data.copy()
    for name, group in data.groupby('user_id'):
        # print("手机号%s的类型为%s" % (name, type(name)))
        #
        #
        #不知道groupyby()分组后，为什么call_history_test中name的类型为numpy.int64，但对于data_call_history,name的类型为str。
        #
        #所以加了个str，但对于其他数据，不一定要加
        #同一个用户的auth_time是一样的，取一个就行
        date = group['auth_time'].values[0]
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
    print('函数data_select结束执行，请稍候...')
    return [data, data_2]


#返回一个排序的DataFrame，按第二个参数排序。data为DataFrame，column_0为排序的字段，index_flag为None时，返回的数据索引值已变化。
# def sort_data(data, column_0, index_flag  = None):
#     #如果只有前两个参数，返回的表不但排序，而且每一行的数值和相应的索引值已经不再和data一致。对索引重新排序。
#     if index_flag == None:
#         # 先用sort_values对手机号排序，才能用二分法查找。但是，按mobile_0排序后的索引值不是按序排列的，需要建立新的索引。
#         # 先把数据按column_0排序，但需要重新设置索引值。所以把排序后结果转换成数组，再转换成DataFrame，这样就把排序后的索引的值改变了。
#         return DataFrame(np.array(data.sort_values(by=column_0)), columns=data.columns)
#     else:
#         return data.sort_values(by=column_0)


#data要求是排序过的，按target_number所在的列
# def lookup_date(target_number_0, data):
#     # data = operator_info
#
#     #如果手机号码符合规则，则执行查找程序。否则返回的列表中，的省和市都为'0'。
#     #需要在正则表达式中添加^$表示开始和结束的位置，保证只有11位。
#     if re.findall(r"^1[3-8]\d{9}$", target_number_0):
#         target_number = int(target_number_0)
#         #先用sort_values对手机号排序，才能用二分法查找。
#         # mobile_series = operator_info['mobile']
#         mobile_series = data['mobile']
#         low = 0
#         high = len(mobile_series) - 1
#         # print('------正在进行第%d次查找，  手机号：%s  ，请稍候......------' % (lookup_count, target_number_0))
#         #二分法查找，当找不到数据时，会跳出while的循环。
#         while (low <= high):
#             # 用//取整数
#             mid = (low + high) // 2
#             midval = int(mobile_series[mid])
#             if midval < target_number:
#                 low = mid + 1
#             elif midval > target_number:
#                 high = mid - 1
#             elif midval == target_number:
#                 # print('已找到 手机号:%s， \n归属地为：%s,%s' % (
#                 # target_number_0, data_home_location.ix[mid, ['province', 'city']].values[0],
#                 # data_home_location.ix[mid, ['province', 'city']].values[1]))
#                 # print('------手机号在data_home_location中的索引为%d------\n' %mid)
#                 return data.ix[mid, ['create_time']].values[0]
#         # print('找不到手机号%s'%target_number_0)
#         return '0'
#     else:
#         # print('手机号不符合规则%s' % target_number_0)
#         return '-1'


#统计用户数量
def count_user(data):
    print('即将对数据的用户数量进行统计，累计花费%ds'%(time.time() - start))
    print('数量：%d'%len(data.drop_duplicates('user_id')))
    print('已完成对数据的用户数量进行统计，累计花费%ds' % (time.time() - start))


def others():

    start = time.time()
    # call_history = fun_readdata_mysql(
    #     '''select u.id as user_id,ao.auth_time ,c.* from call_history c
    # left join users u on u.mobile=c.mobile
    # left join (select * from credit_apply_orders where auth_status=2 ) ao on ao.user_id=u.id
    # where u.id in (select ao.user_id
    # from
    # credit_apply_orders ao
    # left join (select * from user_loan_orders where first_loan=1 and loan_status=2 and loan_time>='2017-06-01' and loan_time <'2017-09-01' group by user_id ) lo on lo.user_id=ao.user_id
    # left join users u on u.id=ao.user_id
    # left join (select user_id,count(id) num , sum(if(overdue_days>0,1,0)) yqbs,max(overdue_days) overdue from user_loan_orders where first_loan=0 and loan_time>='2017-06-01'  and loan_status=2 group by user_id ) lo1 on lo1.user_id=ao.user_id
    # where ao.create_time>='2017-06-01' and ao.create_time<'2017-09-01' and ao.auth_status='2'  and ao.auth_time<'2017-09-01' and lo.user_id is not null and lo.user_id>=910000 and lo.user_id<950000) ;
    #     ''')

    path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
    file_name_1 = 'call_history_old_3.csv'
    file_name_2 = 'call_history_one.csv'
    file_name_3 = 'call_history_three.csv'
    call_history.to_csv(path + file_name_1, sep='\t',encoding='utf-8')
    #注意encoding='gbk'，不是utf-8
    # call_history_old = pd.read_table(path+file_name_1, encoding='gbk')
    #取前多少行，设iterator为true，用get_chunk()来调用
    call_history_old = pd.read_table(path+file_name_1, encoding='utf-8')
    start_2 = time.time()
    print('读取数据花费%ds'%(start_2 -start))
    count_user(call_history_old)
    # print(call_history_old[:1000])

    # call_list_month = data_time_select(1, 3, call_history_old)
    # call_history_one = call_list_month[0]
    # call_history_three = call_list_month[1]


    #需要encoding='utf-8'
    # call_history_one.to_csv(path + file_name_2, sep = '\t', encoding='utf-8')
    # call_history_three.to_csv(path + file_name_3, sep = '\t', encoding='utf-8')
    #
    # print('data_time_select数据花费%ds'%(time.time()-start_2))
    # count_user(call_history_one)
    # count_user(call_history_three)
    end = time.time()
    print('总花费时间：%ds'%(end-start))



# file_name_3 = 'operator_info_demo.csv'

# #本地数据是csv格式的，是用\t分隔operator_info_demo，每行后面有\n，空值为"NULL'。
# file_open = open(path + file_name,'r').readlines()
# #为什么 re.findall(r'^[^\t]+\t$', row[:-1]+'\t'))就不行啊？
# data_temp = [[temp[:-1] for temp in (re.findall(r'[^\t]+\t', row[:-1]+'\t'))] for row in file_open]
# data = DataFrame(data_temp[1:],columns=data_temp[0])
# print(data)

# 读取xls或者xlsx的文件
# file_open_1 = pd.ExcelFile(path + file_name_1)
# address_book_test = file_open_1.parse('Table1')
#
# file_open_2 = pd.ExcelFile(path + file_name_2)
# call_history_test = file_open_2.parse('Table1')

# 有问题，read_csv和read_table读取，会报错。
# file_open_3 = pd.read_table(path + file_name_3)
# operator_info_demo = file_name_3
# file_open_3 = pd.ExcelFile(path + file_name_3)
# operator_info = file_open_3.parse('Table1')


# print(data_time_select(1, 3, call_history_test))
# call_history_list = data_time_select(1, 3, call_history_test)
# one_month = call_history_list[0]
# three_month =call_history_list[1]

#保存一个月和三个月的通话记录
# one_month.to_csv(path+'one_month_call.csv', na_rep = 'NULL')
# three_month.to_csv(path+'three_month_call.csv', na_rep = 'NULL')