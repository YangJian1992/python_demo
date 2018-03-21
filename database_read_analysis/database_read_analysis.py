import pandas as pd
from pandas import DataFrame, Series
import pymysql
import time

class Data_read_analysis():
    def __init__(self, path, filename, select_string, column_add):
        self.path = path
        self.filename = filename
        self.select_string = select_string
        self.column_add = column_add

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

    def get_local_file(path, filename, select_string, columns_add):
        data = mysql_connection(select_string, columns_add)
        print('已经从数据库获得数据，正在生成本地文件，请稍候...')
        data.to_csv(path + filename + '.csv', sep='\t', encoding='utf-8', index=False)

    def analysis(data):
        pass
