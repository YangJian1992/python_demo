import pymysql
import time
import pandas as pd
from pandas import DataFrame, Series


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


if __name__ == '__main__':
    PATH = 'D:\\work\\database\\province-city\\'
    FILE = 'credit_profile_county_null'
    select_string = '''
    select ucp.user_id, ucp.name, ucp.idcard, ucp.county from user_credit_profile as ucp
inner join user_credit_grant as ucg on ucg.user_id = ucp.user_id
where county is null
and idcard is not null
    '''
    columns_add = ['user_id', 'name', 'idcard', 'county']
    county_null_data = mysql_connection(select_string, columns_add)
    county_null_data.to_csv(PATH + FILE + '.csv', index=False, encoding='utf-8', sep='\t')
