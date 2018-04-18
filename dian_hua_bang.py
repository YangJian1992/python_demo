import os
from pandas import Series, DataFrame
import json
import pandas as pd
import time
import pymysql


def read_data(user_data):
    '''
    读取json文件中的数据，根据call_days对通话记录进行筛选
    :return:返回DataFrame格式的通话记录
    '''
    print('正在读取文件，请稍候。。。')
    i = 0#统计已经处理的文件数量
    path = 'D:\\work\\dian_hua_bang\\2018-4-10\\test_1\\'
    date_now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    name_list = os.listdir(path)
    data_list = []
    exception_files = []#读取失败的文件列表
    not_exit_list = []
    exist_list = []
    i=0
    for index_item  in user_data.index:
        call_list = []  # 存放通话记录的列表
        # 读取文件夹中的json文件，并把它们转化成python对象
        file_name = user_data.loc[index_item, 'file_name']
        uid = user_data.loc[index_item, 'user_id']
        if file_name in name_list:
            i += 1
            if i%500==0:
                print('*****************************************')
                print('正在处理第{i}个文件，请稍候。。。'.format(i=i))
                print('*****************************************\n')
            exist_list.append(file_name)
            # 这两句写一下异常报错
            try:
                with open(path + file_name, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    search_time = data.get('last_modify_time', 'null_yj')
                    data = data.get('calls', 'null_yj')
            except Exception as err:#Exception表示万能异常。记得有些json文件会处理失败的
                print('{file_name}, Exception:'.format(file_name=file_name), err)
                exception_files.append(file_name)
                continue#跳出本次循环, 不处理这个文件

            if data != 'null_yj' and len(data) != 0:
                for month_call in data:  # month_call表示每个自然月的通话记录
                    if len(month_call['items']) != 0:  # 'bill_month'通话记录的月份, 且通话记录不能为空
                        for item in month_call['items']:
                            call_list.append(item)
            call_user = DataFrame(call_list)#每个用户的通话记录
            call_user['search_time'] = search_time
            call_user.drop(['details_id', 'fee', 'location', 'location_type'], axis=1, inplace=True)#删除多余的信息
            call_user.insert(0, 'uid', uid)  # 在指定位置插入字段，第0列为手机号
            data_list.append(call_user)
        else:#如果文件名不在本地文件中
            not_exit_list.append(file_name)
    result = pd.concat(data_list, ignore_index=True)
    result.rename(columns={'peer_number':'other_tel', 'time':'start_time', 'duration':'call_duration', 'dial_type':'call_type'}, inplace=True)
    result.call_type = result.call_type.map(lambda x: '主叫' if x == 'DIAL' else '被叫')
    other_tel = result['other_tel'].str.match(r'(?:86|0086|\+86|\(\+?86\))')
    result[other_tel]['other_tel'] = result['other_tel'].str.extract(r'(?:86|0086|\+86|\(\+?86\))(\d+)')#将带有86、0086等的号码去掉前缀
    # print(result.info())
    # category和to_numeric可以减小内存
    for name in ['uid', 'call_type', 'search_time']:
        result[name] = result[name].astype('category')
    result['call_duration'] = pd.to_numeric(result['call_duration'], downcast='integer')

    print("异常文件：", exception_files)
    #print('本地不存在的文件：', not_exit_list)
    print('user_data中总的用户数量：', len(user_data))
    print("异常文件数量：", len(exception_files))
    print('本地不存在的文件的数量：', len(not_exit_list))
    print('本地文件夹中存在的文件数量为：', len(exist_list))
    print('正常处理的文件数量：', len(exist_list)-len(exception_files))
    print('用户uid去重后的数量：', len(result['uid'].drop_duplicates()))
    # print(result.info())
    result.to_csv(path+'钱到到测试数据_{file_id}.csv'.format(file_id=path.split('\\')[-2]), sep=',', encoding='gbk', index=False)
    print(result)
    return result

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


def null_list():
    select_string = '''
    SELECT 
    i.file_name,
    e.user_id,
    h.task_id,
    e.create_time
FROM
    ecshop_orders e
        INNER JOIN
    xh_loan_orders x ON x.loan_id = e.id
        LEFT JOIN
    user_loan_orders lo ON lo.id = e.id
        LEFT JOIN
    users u ON u.id = e.user_id
        LEFT JOIN
    user_credit_profile cp ON cp.user_id = e.user_id
        LEFT JOIN
    (SELECT 
        e.user_id, MAX(o.create_time) new_time, o.task_id
    FROM
        user_loan_orders lo
    INNER JOIN xh_loan_orders x ON x.loan_id = lo.id
    LEFT JOIN ecshop_orders e ON e.id = lo.id
    LEFT JOIN operator_task_info o ON o.user_id = e.user_id
        AND o.create_time < lo.create_time
    WHERE
        lo.first_loan = 1
            AND e.create_time < '2018-04-11 10:30:00'
            AND e.user_id NOT IN (401005 , 934925, 986522, 1285790)
    GROUP BY e.user_id) h ON h.user_id = e.user_id
        LEFT JOIN
    operator_task_info i ON i.user_id = e.user_id
        AND i.create_time = h.new_time
WHERE
    (x.loan_status = 2 OR e.loan_status = 2)
        AND lo.first_loan = 1
        AND e.create_time < '2018-04-11 10:30:00'
        AND e.user_id NOT IN (401005 , 934925, 986522, 1285790, 915645, 311798)
;
    '''
    columns_add = ['file_name', 'user_id', 'task_id', 'create_time']
    user_data = mysql_connection(select_string, columns_add)
    path = 'D:\\work\\dian_hua_bang\\2018-4-10\\test\\'
    name_list= os.listdir(path)
    null_list=[]
    for index_item in user_data.index:
        file_name = user_data.loc[index_item, 'file_name']
        uid = user_data.loc[index_item, 'user_id']
        if file_name.split('/')[-1] not in name_list:
            null_list.append(index_item)
    user_data.loc[null_list].to_csv('D:\\work\\dian_hua_bang\\2018-4-10\\null_list.csv', sep=',', encoding='gbk')
    null_data = user_data.loc[null_list]
    # print(null_data)
    task_tuple=tuple(null_data['task_id'])
    select_string_2 = '''
SELECT user_id, three_month_operator_call_num, task_id, create_time FROM qiandaodao.operator_address_book_rule
where task_id  in {task_list}
;
    '''.format(task_list=task_tuple)
    null_call = mysql_connection(select_string_2, ['user_id', 'three_month_operator_call_num', 'task_id', 'create_time'])
    task_id_list=[]
    for ti in task_tuple:
        if ti not in list(null_call['task_id']):
          task_id_list.append(ti)
    pd.Series(task_id_list).to_csv('D:\\work\\dian_hua_bang\\2018-4-10\\task_id_null.csv',encoding='gbk', sep=',')
    print(null_call)

#这是一个测试
if __name__ == '__main__':
    select_string = '''
    SELECT
    i.file_name,
    e.user_id
FROM
    ecshop_orders e
        INNER JOIN
    xh_loan_orders x ON x.loan_id = e.id
        LEFT JOIN
    user_loan_orders lo ON lo.id = e.id
        LEFT JOIN
    users u ON u.id = e.user_id
        LEFT JOIN
    user_credit_profile cp ON cp.user_id = e.user_id
        LEFT JOIN
    (SELECT
        e.user_id, MAX(o.create_time) new_time
    FROM
        user_loan_orders lo
    INNER JOIN xh_loan_orders x ON x.loan_id = lo.id
    LEFT JOIN ecshop_orders e ON e.id = lo.id
    LEFT JOIN operator_task_info o ON o.user_id = e.user_id
        AND o.create_time < lo.create_time
    WHERE
        lo.first_loan = 1
            AND e.create_time < '2018-04-11 10:30:00'
            AND e.user_id NOT IN (401005 , 934925, 986522, 1285790)
    GROUP BY e.user_id) h ON h.user_id = e.user_id
        LEFT JOIN
    operator_task_info i ON i.user_id = e.user_id
        AND i.create_time = h.new_time
WHERE
    (x.loan_status = 2 OR e.loan_status = 2)
        AND lo.first_loan = 1
        AND e.create_time < '2018-04-11 10:30:00'
        AND e.user_id NOT IN (401005 , 934925, 986522, 1285790, 915645, 311798);
    '''
    columns_add = ['file_name', 'user_id']
    user_data = mysql_connection(select_string, columns_add)
    print('user_data的数量', len(user_data))
    user_data['file_name'] = user_data['file_name'][user_data['file_name'].notnull()]#找出非空值
    user_data['file_name'] = user_data['file_name'].map(lambda x : x.split('/')[-1])
    read_data(user_data)
#     null_list()
