import pandas as pd
from pandas import Series, DataFrame
import time
import datetime
import numpy as np
import pymysql
import matplotlib.pyplot as plt
'''
每个用户的申请明细，由白骑士统计。

'''


# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
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


#生成本地文件，供后续程序使用
def get_local_file():
    path = 'D:\\work\\database\\white_knight_auth_times\\'
    file = 'white_knight_auth_times'
    select_string = '''
        SELECT 
    lc.user_id,
            ulo.overdue_days,
            lc.risk_count,
            lc.loan_id,
            lc.risks,
            lc.auth_status,
            lc.create_time,
            lc.id as lsar_id,
            lc.type
            
FROM
    (SELECT 
        cao.user_id,
            lsar.loan_id,
            lsar.id,
            lsar.type,
            lsar.risks,
            lsar.risk_count,
            auth_status,
            cao.create_time
    FROM
        qiandaodao.loan_system_auth_record AS lsar
    INNER JOIN credit_apply_orders AS cao ON cao.id = lsar.loan_id
    WHERE
        lsar.type = 24
            AND lsar.risks LIKE '%白骑士%'
            and left(cao.create_time, 7) = '2017-10' )AS lc
        INNER JOIN
    user_loan_orders AS ulo ON ulo.user_id = lc.user_id
WHERE
    ulo.first_loan = 1
        AND ulo.loan_status = 2
        AND ulo.overdue_days = 0
        limit 200
        '''
    columns_add = ['user_id', 'loan_id', 'lsar.id', 'type', 'risks', 'risk_count', 'auth_status', 'create_time']
    data = mysql_connection(select_string, columns_add)
    print('已经从数据库获得数据，正在生成本地文件，请稍候...')
    data.to_csv(path + file + '.csv', sep='\t', encoding='utf-8', index=False)
#生成本地文件



#读取本地文件
def read_local_file():
    path = 'D:\\work\\database\\white_knight_auth_times\\'
    file = 'white_knight_days0'
    data = pd.read_csv(path+file+'.csv', sep=',', encoding='utf-8')
    data['rule_0'] = 'null'
    data['data_0'] = 'null'
    data_rule = data[data['risks'].str.contains('总数')]
    data_rule_0 = data_rule['risks'].str.extract('(总数.+data)').str[0:-7].str.replace('总数', '#总数').str.split('#').str[1:]
    index_0 = data_rule_0.index
    data_rule_1 = data_rule['risks'].str.extract('(data.+credit)').str[7:-9].str.split(';').str[:-1]
    index_1 = data_rule_1.index
    data.ix[index_0,'rule_0'] = data_rule_0
    data.ix[index_1, 'data_0'] = data_rule_1
    data.to_csv(path+file+'_analysis.csv', encoding="gbk", sep=',')
    print(data)


    # return data


#数据分析,把数据分为两部分，






if __name__ == '__main__':
    start = time.time()
    read_local_file()
    print('一共花费{time}s'.format(time=time.time()-start))