import pandas as pd
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime
from datetime import timedelta
import pymysql
import json
import time

#连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string, columns_add):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)

    temp = DataFrame(list(cur.fetchall()), columns=columns_add)
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return (temp)


def read_data():
    path = 'D:\\work\\华道征信-测试数据\\信贷详情\\qiandaodao_new_data20180412-信贷详情(完成)2018.4.13.xlsx'
    data = pd.read_excel(path, sheetname='Sheet1', header=0)
    data['手机号'] = data['手机号'].astype('str')
    data.rename(columns={'平台类型':'注册平台类型', '平台类型.1':'申请平台类型', '平台类型.2':'放款平台类型', '平台类型.3':'驳回平台类型',
                         '平台类型.4':'逾期平台类型',  '平台类型.5': '还款平台类型'}, inplace=True)
    data_test = data[data['loan_time'].notnull()]
    data_test['loan_time'] = data_test['loan_time'].str[:10].map(lambda x : datetime.strptime(x, '%Y-%m-%d'))
    print(data_test.info())
    # print(data)

    data_dict = {}
    data_dict['注册'] = data_test[data_test['注册时间'].notnull()].iloc[:, [0, 1, 2, 3, 4]]
    data_dict['申请'] = data_test[data_test['申请时间'].notnull()].iloc[:, [0, 1, 5, 6, 7, 8]]
    data_dict['放款'] = data_test[data_test['放款时间'].notnull()].iloc[:, [0, 1, 9, 10, 11, 12]]
    data_dict['驳回'] = data_test[data_test['驳回时间'].notnull()].iloc[:, [0, 1, 13, 14, 15]]
    data_dict['逾期'] = data_test[data_test['逾期时间'].notnull()].iloc[:, [0, 1, 16, 17, 18, 19]]
    data_dict['还款'] = data_test[data_test['还款时间'].notnull()].iloc[:, [0, 1, 20, 21, 22, 23]]

    return([data_dict, data_test])


def analysis_data(k, data):
    '''
    :param k: data_dict中的key，如'注册', '申请'..
    :param data: data_dict中的值，为DataFrame

    :return: result: 每个用户的统计结果，为DataFrame
    '''
    data_dict = {}
    result_list = []
    data['delta_days'] = (data['loan_time']-data[k+'时间']).map(lambda x:x.days)
    # print(data)

    for mobile, group in data.groupby('手机号'):
        result_dict = {}#存放每个用户的结果
        result_dict['手机号'] = mobile
        data_30 = group[(group['delta_days'] <= 30) & (group['delta_days'] >= 0)]
        data_90 = group[(group['delta_days'] <= 90) & (group['delta_days'] >= 0)]
        data_180 = group[(group['delta_days'] <= 180) & (group['delta_days'] >= 0)]
        data_dict['所有日期'] = group
        data_dict['近30天'] = data_30
        data_dict['近90天'] = data_90
        data_dict['近180天'] = data_180
        # for key, value in data_dict.items():
        #     result_dict[key + '_银行_' + k] = len(value[value[k+'平台类型']=='银行'])
        #     result_dict[key + '_非银行_' + k] = len(value[value[k+'平台类型'] == '非银行'])
        #     result_dict[key + '_总计_' + k] = len(value[value[k+ '平台'].notnull()])
        for key, value in data_dict.items():
            result_dict[key + '_机构_' + k] = len(value[k + '平台'].drop_duplicates())
        result_list.append(result_dict)
    print(result_list)
    result = DataFrame(result_list)
    print('\n*********************************************')
    print(result)
    print('{k}手机号总数：'.format(k=k), len(result))
    return result

if __name__=="__main__":
    data_dict = read_data()[0]
    data_test = read_data()[1]
    writer = pd.ExcelWriter('D:\\work\\信贷详情_机构数量.xlsx')
    for k, v in data_dict.items():
        data_0 = analysis_data(k, v)
        data = DataFrame(columns=['手机号'])
        data['手机号'] = data_test['手机号'].drop_duplicates()
        result = pd.merge(data, data_0, how='left', on='手机号')
        print(result)
        print('*****************\n', data_0)
        result.fillna(0, inplace=True)
        result.insert(0, '手机号', result.pop('手机号'))
        result.to_excel(writer, k)
    writer.save()





