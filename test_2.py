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


def addr_analysis():
    select_string = '''
    select lo.id,e.user_id,d.county,lo.location,ao.location,lo.amount,lo.overdue_days,lo.repay_status from user_loan_orders lo 
    left join ecshop_orders e on e.id=lo.id
    left join (select user_id,location from credit_apply_orders where auth_status=2 and auth_time<'2018-01'   group by user_id) ao on ao.user_id=e.user_id
    inner join user_credit_grant cg on cg.user_id=e.user_id 
    left join user_credit_profile cp on cp.user_id=e.user_id
    left join idcard_district d on d.id=left(cp.idcard,6) 
    where lo.create_time>'2018-01-23' and cg.apply_type=1 and lo.due_repay_date<'2018-04-06';
    '''
    columns = ['id', 'user_id', 'county', 'location_1', 'location_2', 'amount', 'overdue_days', 'repay_status']
    data = mysql_connection(select_string, columns)
    pattern_1 = r'.*?"province":"(.+?)"|(.*?(?:省|自治区|市|特别行政区))'
    pattern_2 = r'.*?"city":"(.+?)"|.*?(?:省|自治区|市|特别行政区)(.*?(?:市|自治州|地区|盟|区))'
    temp_1_1 = data.location_1.str.extract(pattern_1)
    temp_1_2 = data.location_1.str.extract(pattern_2)
    temp_2_1 = data.location_2.str.extract(pattern_1)
    temp_2_2 = data.location_2.str.extract(pattern_2)
    def com(data):
        for i in data[data[0].isnull()].index:
            data.loc[i, 0] = data.loc[i, 1]
        return data[0]
    data.insert(4, 'location_1_result',  com(temp_1_1) + com(temp_1_2))
    data.insert(6, 'location_2_result',  com(temp_2_1) + com(temp_2_2))
    writer = pd.ExcelWriter('D:\\work\\Desktop\\addr.xlsx')
    data.to_excel(writer, 'sheet1', encoding='utf-8')
    writer.save()

addr_analysis()