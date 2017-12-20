
from pandas import Series, DataFrame
import pandas as pd
import time
import json
import pymysql
import os
'''
这个文件是用来得到电话邦催收分的测试数据。通话详单的create_time为2017年9月
'''

# for key, value in (data_dict['data']["transportation"][0]).items():
#     print(key, value)
#     print('\n**********************************************\n')
# print("\n\n_______________________________________\n\n")
# for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin'])).items():
#     print(key, value)
#     print('\n**********************************************\n')
# for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin']['call_info'])).items():
#     print(key, value)
#     print('\n**********************************************\n')
# print(((data_dict['data']["transportation"][0]['origin']['call_info']['data'])))
# call_six_monthes = data_dict['data']["transportation"][0]['origin']['call_info']['data']
# print(call_six_monthes)

# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8',  cursorclass=pymysql.cursors.DictCursor)
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)
    data = DataFrame(cur.fetchall())
    # print(data)
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return data


#生成本地文件，供后续程序使用
def get_local_file(m):
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\'
    file = 'operator_info_test_{num}'.format(num=m+1)
    select_string = '''
 SELECT 
    uu.id,
    uu.user_id,
    uu.create_time,
    uu.first_loan,
    uu.loan_status,
    uu.overdue_days,
    uu.overdue_status,
    uu.repay_status,
    uu.mobile,
    oi.data_src,
    oi.reg_time,
    oi.is_valid,
    oi.skip
FROM
    (SELECT 
            ulo.id,
            ulo.user_id,
            ulo.create_time,
            ulo.first_loan,
            ulo.loan_status,
            ulo.overdue_days,
            ulo.overdue_status,
            ulo.repay_status,
            users.mobile
    FROM
        user_loan_orders AS ulo
    INNER JOIN users ON users.id = ulo.user_id
    WHERE
        ulo.first_loan = 1
            AND ulo.loan_status = 2
            AND LEFT(ulo.create_time, 7) = '2017-09') AS uu
        INNER JOIN
    operator_info AS oi ON oi.user_id = uu.user_id
WHERE
    oi.skip = 2
order by uu.user_id
limit {m}, {n}
    '''.format(m=m*1000, n=m+200)
    # columns_add = ['id', 'user_id', 'mobile', 'name', 'reg_time', 'is_valid', 'id_card', 'create_time', 'data_src', 'skip']
    data = mysql_connection(select_string)
    print('已经从数据库获得数据，正在生成本地文件，请稍候...')
    data.to_csv(path + file + '.csv', sep='\t', encoding='utf-8', index=False)


# findall返回的列表中包含时间数据，统一转换成秒
def count_call_time(time_list):
        for x, value in enumerate(time_list):
            time_list[x] = int(value)
        if len(time_list) == 3:
            return ((time_list[0]) * 3600 + time_list[1] * 60 + time_list[2])
        elif len(time_list) == 2:
            return ((time_list[0]) * 60 + time_list[1])
        elif len(time_list) == 1:
            return time_list[0]
        else:
            return (-1)


#读取本地文件
def read_analysis_file(num):
    print('read_analysis_file()开始执行，请稍候。。。')
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\'
    file = 'operator_info_test_{num}'.format(num=num)
    data = pd.read_csv(path+file+'.csv', sep='\t', encoding='utf-8')
    start = time.time()
    #用字符串填充空数据
    data.fillna('null')
    #先把data['data_src']数据转换成字典格式
    # fun1 = lambda x : json.dumps(x)
    # fun2 = lambda x : json.loads(x)
    # data['data_src'] = data['data_src'].map(fun1)
    # data['data_src'] = data['data_src'].map(fun2)
    call_list = []
    for index in data.index:
        if data.loc[index, 'skip'] == 2:
            #建立一张空列表，用来存放某个用户的通话详情
            data_user_list = []
            user_id = data.loc[index, 'user_id']
            mobile = data.loc[index, 'mobile']
            is_valid = data.loc[index, 'is_valid']
            skip = data.loc[index, 'skip']
            create_time = data.loc[index, 'create_time']
            #解析data_src字段，获取通话详情
            data_src = data.loc[index, 'data_src']
            data_src = json.loads(data_src)
            #该用户六个月的通话记录
            if 'call_info' in data_src['data']["transportation"][0]['origin']:
                call_six_monthes = data_src['data']["transportation"][0]['origin']['call_info']['data']
                # print(call_six_monthes)
                #把每个月的通话记录，转换成df格式，并添加到空表中去，存在没有‘detail'的情况。
                for item in call_six_monthes:
                    if 'details' in item:
                        data_item = DataFrame(item["details"])
                        # print(data_item)
                        data_user_list.append(data_item)
                #每个用户的通话数据表， 因为data_user_list可能为空, 无法concat，会报错。
                if len(data_user_list) != 0:
                    data_user = pd.concat(data_user_list, ignore_index=True)
                    data_user['user_id'] = user_id
                    data_user['mobile'] = mobile
                    data_user['is_valid'] = is_valid
                    data_user['skip'] = skip
                    data_user['create_time'] = create_time
                    call_list.append(data_user)
    #所有用户的通话数据表
    data = pd.concat(call_list, ignore_index=True)

    data.to_csv(path + file + '_analysis.csv', sep='\t', encoding='utf-8', index=False)
    print('read_analysis_file()已结束，请稍候。。。')


#再次处理数据，把时间换算成秒，并加上逾期标签。
def analysis(data):
    data['mobile'] = data['mobile'].astype('str', errors='raise')
    data['another_nm'] = data['another_nm'].astype('str', errors='raise')
    data['call_duration'] = data['comm_time'].str.findall('\d+')
    print(data['call_duration'])
    data['call_duration'] = data['call_duration'].map(lambda x: count_call_time(x))
    print(data['call_duration'])
    data['uid'] = 'uid' + data['mobile']
    #逾期标签
    data['level'] = 'null'
    not_overdue_index = data[data['overdue_status'] == 0].index
    if len(not_overdue_index) != 0:
        data.ix[not_overdue_index, ['level']] = '未逾期'

    overdue_index_1 = data[(data['overdue_status'] == 1) & (data['repay_status'] == 2)].index
    if len(overdue_index_1) != 0:
        data.ix[overdue_index_1, ['level']] = '逾期已还'

    overdue_index_2 = data[(data['overdue_status'] == 1) & (data['repay_status'] != 2)].index
    if len(overdue_index_2) != 0:
        data.ix[overdue_index_2, ['level']] = '逾期未还'
    data_2 = data['uid', 'another_nm', 'call_duration', 'start_time', 'comm_mode', 'create_time', 'level']
    data_2 = DataFrame(data_2, columns=['uid', 'other_tel', 'call_duration', 'start_time', 'call_type', 'search_time'])
    data_3 = data['uid', 'level']
    return [data, data_2, data_3]
# def a():
#     with open('C:\\Users\\QDD\\Desktop\\1.txt', 'r') as file:
#         data = file.read()
#     data_dict = json.loads(data)
#     print((data_dict['data']["transportation"][0]).keys())
#     for key, value in (data_dict['data']["transportation"][0]).items():
#         print(key, value)
#         print('\n**********************************************\n')
#     print("\n\n_______________________________________\n\n")
#     # for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin'])).items():
#     #     print(key, value)
#     #     print('\n**********************************************\n')
#     # for key, value in (DataFrame(data_dict['data']["transportation"][0]['origin']['call_info'])).items():
#     #     print(key, value)
#     #     print('\n**********************************************\n')
#     # print(((data_dict['data']["transportation"][0]['origin']['call_info']['data'])))
#     call_six_monthes = data_dict['data']["transportation"][0]['origin']['call_info']['data']
#     # print(call_six_monthes)
#     call_list = []
#     for item in call_six_monthes:
#         data_item = DataFrame(item['details'])
#         call_list.append(data_item)
#     data = pd.concat(call_list, ignore_index=True)
#
#     return data







if __name__ == '__main__':
    # get_local_file()
    for num in range(8):
        get_local_file(num)
        read_analysis_file(num)
        break
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\'
    data_list=[[] for i in range(3)]
    file_list = os.listdir(path)
    print(file_list)
    for key, name in enumerate(file_list):
        print('开始for')
        if 'analysis' in name:
            data = pd.read_csv(path+name, sep='\t', encoding='utf-8')
            data_result_list = analysis(data)
            print(data)
            for key, item in enumerate(data_list):
                item.append(data_result_list[key])
    for key, item in enumerate(data_list):
        data = pd.concat(item, ignore_index=True)
        data.to_csv(path + 'data_result_{key}.csv'.format(key=key), encoding='utf-8', sep='\t')
    data_drop = data_list[0].drop_duplicates('user_id')
    print(len(data_drop))


    # os.system('shutdown -s -t 0')