#在credit_apply_orders这张表中，找到审核通过的，auth_status=2的用户，并把他们按照省和市分类，统计相应的数量。
import numpy as np
import pandas as pd
from pandas import Series
from pandas import DataFrame
import pymysql
import os
import time


#连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string, columns):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)
    temp = DataFrame(list(cur.fetchall()), columns=columns)
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return (temp)


def get_analysis_data():
    path = 'D:\\work\\database\\cao.provivce_city\\'
    file = 'test_data'
    columns = ['user_id', 'location']
    select_string = '''
    select user_id, location from credit_apply_orders where auth_status = 2 limit 100
    '''
    data = mysql_connection(select_string, columns)
    print(data.info(), '\n')
    print(type(data.ix[18, 'location']))
    data.to_csv(path + file + '.csv', sep=',', encoding='gbk')
    print('原数据共{num}行：'.format(num=len(data)))
    data['province'] = 'null'
    data['city'] = 'null'
    province = data['location'].str.extract('((?<="province":")\w+)')
    print(type(province[18]))
    # print(province)
    # city = data['location'].str.extract('((?<="city":")\w+?")').str[:-1].fillna('null')
    # #(?<=\bprovince:)\w+?(?=,)
    # del data['location']
    # data['province'] = province
    # data['city'] = city
    # print('_________________________________')
    # data = data[(data['province']!='null') & (data['city']!='null')]
    # print('处理后的数据共{num}行：'.format(num=len(data)))
    # result = data.groupby(['province', 'city']).count()
    # result.to_csv(path+file+'.csv', sep=',', encoding='gbk')


if __name__ == '__main__':
    get_analysis_data()
