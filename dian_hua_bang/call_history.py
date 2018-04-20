import os
from pandas import Series, DataFrame
import json
import pandas as pd
import time
import pymysql
from datetime import datetime
import re

path = 'D:\\work\\dian_hua_bang\\2018-4-10\\'
file = 'test_1.csv'
data = pd.read_csv(path+file, sep=',', encoding='gbk')
pattern = r'(?:86|\+86|0)?1[3-9]\d{9}'
data = data[data['other_tel'].notnull()]
data['other_tel'] = data['other_tel'].astype('str')
data['uid'] = data['uid'].astype('str').astype('category')
data['call_type'] = data['call_type'].astype('category')
data['search_time'] = data['search_time'].astype('category')
data_mobile = data[data['other_tel'].str.match(pattern)]
result_list = []
i = 0
u_sum = len(data_mobile['uid'].drop_duplicates())
for uid, group in data_mobile.groupby('uid'):
    i += 1
    if i % 500 == 0:
        print("\n\n************************************************\n正在处理第{i}个用户，共{u_sum}个用户，请稍候。。。".format(i=i, u_sum=u_sum))
    uid_dict = {}
    data_dict = {}
    group['start_time'] = group['start_time'].map(lambda x : datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    search_time = datetime.strptime(group['search_time'].iloc[0], '%Y-%m-%d %H:%M:%S')
    data_dict['所有时期'] = group
    group['delta_days'] = (search_time - group['start_time']).map(lambda x : x.days)
    data_dict['近一周'] = group[group['delta_days']<=7]
    data_dict['近两周'] = group[(group['delta_days']<=14) & (group['delta_days']>7)]
    data_dict['近三周'] =group[(group['delta_days']<=21) & (group['delta_days']>14)]
    data_dict['0日30日'] = group[ (group['delta_days']<=30)]
    data_dict['30日60日'] = group[(group['delta_days']<=60) & (group['delta_days']>30)]
    data_dict['60日90日'] = group[(group['delta_days']<=90) & (group['delta_days']>60)]
    data_dict['90日180日'] = group[(group['delta_days']<=180) & (group['delta_days']>90)]
    # data_dict['180日以上'] = group[(search_time - group['start_time']).map(lambda x: ( x.days > 180))]
    uid_dict['uid'] = uid
    for data_k, data_v in data_dict.items():
        uid_dict[data_k+'_通话次数'] = len(data_v)
        uid_dict[data_k + '_通话次数_主叫'] = len(data_v[data_v['call_type'] == '主叫'])
        uid_dict[data_k + '_通话次数_被叫'] = len(data_v[data_v['call_type'] == '被叫'])

        uid_dict[data_k + '_号码数量'] = len(data_v['other_tel'].drop_duplicates())

        uid_dict[data_k + '_通话总时长'] = data_v['call_duration'].sum()
        uid_dict[data_k + '_通话总时长_被叫'] = data_v[data_v['call_type'] == '被叫']['call_duration'].sum()
        uid_dict[data_k + '_通话总时长_主叫'] = data_v[data_v['call_type'] == '主叫']['call_duration'].sum()
        if uid_dict[data_k+'_通话次数'] != 0:
            uid_dict[data_k + '_平均通话时长'] = uid_dict[data_k + '_通话总时长'] / uid_dict[data_k+'_通话次数']
        else:
            uid_dict[data_k + '_平均通话时长'] = 0

    result_list.append(uid_dict)

data_cui =pd.read_excel(path+'cuishoufen.xlsx', sheetname='cuishoufen', encoding='utf-8')
data_cui['uid'] = data_cui['uid'].astype('str')
result = DataFrame(result_list)
result_final = pd.merge(data_cui, result, how='left', on='uid')
week_co = [co for co in result_final.columns if '周' in co]
week_co.extend(['uid', '催收综合分'])
month_co = [co for co in result_final.columns if '日' in co]
month_co .extend(['uid', '催收综合分'])
all_co = [co for co in result_final.columns if '所有时期' in co]
all_co.extend(['uid', '催收综合分'])
writer = pd.ExcelWriter(path + '对比结果.xlsx')
result_final.to_excel(writer, sheet_name='汇总')
result_final.loc[:, all_co].to_excel(writer, sheet_name='所有时期')
result_final.loc[:, week_co].to_excel(writer, sheet_name='按周划分')
result_final.loc[:, month_co].to_excel(writer, sheet_name='按月划分')
writer.save()
print(result)