import pandas as pd
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime
from datetime import timedelta
import pymysql
import json
import time

def analysis_data():
    path = 'D:\\work\\华道征信\\华道借贷详情测试\\'
    filename = '北京钱到到金服科技有限公司信贷详情-华道2018.4.20'
    data_1 = pd.read_excel(path+filename+'.xlsx', sheet_name='信贷详情', header=0)
    data_2 = pd.read_excel(path+filename+'.xlsx', sheet_name='Table1', header=0)
    # print(data_1)
    data_1['手机号'] = data_1['手机号'].astype('str').astype('category')
    data_2['mobile'] = data_2['mobile'].astype('str').astype('category')
    data = pd.merge(data_1, data_2, left_on='手机号', right_on='mobile', how='left')
    print(data.info())
    data_1=None
    data_2=None
    for co_name in [i for i in data.columns if ('类型' in i or '金额' in i)]:
        data[co_name] = data[co_name].astype('category')
    print('**************************', data.info())
    data['create_time'] = data['create_time'].map(lambda x:datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    #data_0是所有的手机号，用来左连后面的结果DF
    data_0 = data.loc[:,['手机号']].drop_duplicates(['手机号'])

    data_dict = {}
    data_dict['注册'] = data[data['注册时间'].notnull()].loc[:, ['手机号','create_time', '注册平台','注册时间']]
    data_dict['申请'] = data[data['申请时间'].notnull()].loc[:, ['手机号','create_time', '申请平台','申请金额', '申请时间']]
    data_dict['放款'] = data[data['放款时间'].notnull()].loc[:, ['手机号','create_time', '放款平台','放款金额', '放款时间']]
    data_dict['驳回'] = data[data['驳回时间'].notnull()].loc[:, ['手机号','create_time', '驳回平台', '驳回时间']]
    data_dict['逾期'] = data[data['逾期时间'].notnull()].loc[:, ['手机号','create_time', '逾期平台','逾期金额', '逾期时间']]
    data_dict['还款'] = data[data['还款时间'].notnull()].loc[:, ['手机号','create_time', '还款平台','还款金额', '还款时间']]

    writer = pd.ExcelWriter(path + '华道信贷结果划分_区间划分表.xlsx')
    df_list = []
    for k, v_data in data_dict.items():
        v_data['delta_days'] = (v_data['create_time']-v_data[k+'时间']).map(lambda x : x.days)
        result_list = []
        for mobile, group in v_data.groupby('手机号'):
            user_dict = {}
            result_dict = {}
            result_dict['手机号'] = mobile
            data_7 = group[(group['delta_days'] < 7) & (group['delta_days'] >= 0)]
            data_14 = group[(group['delta_days'] < 14) & (group['delta_days'] >= 7)]
            data_21 = group[(group['delta_days'] < 21) & (group['delta_days'] >= 14)]

            data_30 = group[(group['delta_days'] < 30) & (group['delta_days'] >= 0)]
            data_60 = group[(group['delta_days'] < 60) & (group['delta_days'] >= 30)]
            data_90 = group[(group['delta_days'] < 90) & (group['delta_days'] >= 60)]
            data_0_90 = group[(group['delta_days'] < 90) & (group['delta_days'] >=0)]
            data_180 = group[(group['delta_days'] < 180) & (group['delta_days'] >= 90)]
            user_dict['所有日期'] = group
            user_dict['0-6天'] = data_7
            user_dict['7-13天'] = data_14
            user_dict['14-20天'] = data_21
            user_dict['0-29天'] = data_30
            user_dict['30-59天'] = data_60
            user_dict['60-89天'] = data_90
            user_dict['0-89天'] = data_0_90
            user_dict['90-179天'] = data_180
            for k_2, v_2 in user_dict.items():
                result_dict[k_2 + '_' + k + '次数'] = len(v_2[k + '平台'].notnull())
            result_list.append(result_dict)
        result = DataFrame(result_list, columns=['手机号','所有日期_'+k+'次数', '0-6天_'+k+'次数', '7-13天_'+k+'次数', '14-20天_'+k+'次数',
                                                 '0-29天_'+k+'次数', '30-59天_'+k+'次数','60-89天_'+k+'次数','0-89天_'+k+'次数','90-179天_'+k+'次数'])
        result = pd.merge(data_0, result, how='left', on='手机号')
        df_list.append(result)
        result.to_excel(writer, sheet_name=k+'结果', encoding='utf-8')
    # sheet_data= sheet_list[0]
    # for k_3, v_sheet in enumerate(sheet_list):
    #     if k_3 < len(sheet_list)-1:
    #         sheet_data = pd.merge(sheet_data, sheet_list[k_3+1], on='手机号', how='left')
    # sheet_data.to_excel(writer, sheet_name='汇总结果', encoding='utf-8')
    writer.save()
    writer_2 = pd.ExcelWriter(path + '华道信贷结果划分_区间汇总表.xlsx')
    sheet_data = df_list[0]
    for k_3, v_sheet in enumerate(df_list):
            if k_3 < len(df_list)-1:
                sheet_data = pd.merge(sheet_data, df_list[k_3+1], on='手机号', how='left')
            print(k_3, sheet_data.columns,'*************************************************')
    sheet_data.to_excel(writer_2, sheet_name='汇总')
    writer_2.save()

analysis_data()


