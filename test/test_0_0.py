import re
import os
import sys
import time
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime
from datetime import timedelta
import pymysql
import json

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




def addr(select_string, name):
    columns_add = ['id', 'amount', 'location']
    data = mysql_connection(select_string, columns_add)
    data['province'] = 'NULL'
    data['city'] = 'NULL'
    print(data.info())
    #字段不为空，分为含有‘location’、不含‘location'但有地址、不含’location'但无地址(如’{}')
    data_1 = data[(data['location'].notnull()) & (data['location'].str.contains('location'))]
    pattern_1 = r'.*location":"(.*?(?:自治区|省|市))(.*?(?:盟|自治州|地区|市|区|县|自治县)).*'
    data_pc_1 = data_1['location'].str.extract(pattern_1)
    pattern_2 = r'.*?(.*?(?:自治区|省|市))\|?(.*?(?:盟|自治州|地区|市|区|县|自治县)).*'
    data_2 = data[(data['location'].notnull()) ^ ((data['location'].str.contains('location')))]
    data_pc_2 = data_2['location'].str.extract(pattern_2)
    data_pc = pd.concat([data_pc_1, data_pc_2], ignore_index=False)
    data_pc = data_pc.fillna('NULL')
    data['province'] = data_pc[0]
    data['city'] = data_pc[1]
    data = data.fillna('NULL')
    in_city = data[data['province'].map(lambda x : '市' in x)].index
    data.ix[in_city, 'city'] = data.ix[in_city, 'province']


    data_list = []
    for (k_1, k_2), group in data.groupby(['province', 'city']):
        data_dic = {}
        data_dic['province'] = k_1
        data_dic['city'] = k_2
        data_dic['amount'] = float(group['amount'].sum())
        data_dic['orders_count'] = len(group)
        data_dic['aver_amount'] = round(float(group['amount'].sum()) / len(group), 2)
        data_list.append(data_dic)
    a = pd.DataFrame(data_list, columns=['province', 'city', 'amount', 'orders_count', 'aver_amount'])
    writer = pd.ExcelWriter('D:\\work\\' + name)
    a.to_excel(writer, index=False)
    writer.save()



if __name__ == "__main__":
    select_string_1 = '''
    select id, lo.amount, lo.location from user_loan_orders lo where lo.loan_status=2;
    '''
    name_1 = 'user_loan_orders.xlsx'
    addr(select_string_1, name_1)
    select_string_2 = '''
    select e.id, e.total_price-e.down_pay, e.location from ecshop_orders e where e.create_time<'2018-01' and e.order_status in (4,5,10,11);
    '''
    name_2= 'ecshop_orders.xlsx'
    addr(select_string_2, name_2)