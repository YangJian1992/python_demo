#codeing: uft-8
import re
# import os
# import sys
import time
# from datetime import datetime
import pymysql
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
import address_call as ac
import unique_user_id as uui

'''
办公电脑无法一次性处理大的文件，两三百兆的文件都不行。所以要分块读取文件。
由于分块读取大的文件，使得同一个user_id的数据被分割成两块，对于这种user_id的数据需要重新分析。
本模块要处理这个问题。
'''
def unique_user_id(data_flag):
    if data_flag==1:
        #读取一个月通话记录的分析结果，找到重复的user_id。
        filen_name = 'one_month_analysis.csv'
        filen_name_2 = 'call_history_one_month.csv'

    elif data_flag==3:
        filen_name = 'three_months_analysis.csv'
        filen_name_2 = 'call_history_three_months.csv'
    else:
        print('unique_user_id()函数的参数有误，请重新输入。')

    path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'

    #读取分析结果
    unique_data = pd.read_table(path + filen_name, encoding='utf-8', sep='\t')
    # print(unique_data)
    #找重复的user_id和相应的用户名列表
    group_count = unique_data.groupby('user_id').count()
    user_list = group_count[group_count['top10_address_num']>1].index.values
    print(user_list)
    #找到unique_data中的重复user_id的索引，并把对应的行数据删掉
    unique_data_remove = unique_data[unique_data['user_id'].isin(user_list)]
    unique_data.drop(unique_data_remove.index, axis=0, inplace=True)

    #晚点读取
    call_history = pd.read_table(path + 'result_excel\\' + filen_name_2, encoding='utf-8', sep='\t')
    call_history_2 = call_history[call_history['user_id'].isin(user_list)]
    #能释放内存？
    call_history = None
    print(call_history_2)

    #被删掉的user_id的通话记录，重新计算
    result_list = ac.user_call_info(call_history_2)

    #生成新的DataFrame
    unique_date_2 = DataFrame(result_list, columns=['user_id', 'address_call_times', 'top10_address_num','address_call_person', 'contacts_three_times'])
    #最终结果unique_date_new
    unique_date_new = pd.concat([unique_data, unique_date_2])

    # 1,建立一个ExcelWriter;2.写入;3,save
    writer = pd.ExcelWriter(path + filen_name_2[0:-3]+'xlsx')
    unique_date_new.to_excel(writer, 'yangjian')
    writer.save()

    return result_list