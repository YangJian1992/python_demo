
from pandas import Series, DataFrame
import pandas as pd
import time
import json
import pymysql
import os
import numpy as np
import datetime
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
    ulo.id,
    ulo.user_id,
    ulo.create_time,
    ulo.first_loan,
    ulo.loan_status,
    ulo.overdue_days,
    ulo.overdue_status,
    ulo.repay_status,
    oi.mobile,
    oi.data_src,
    oi.reg_time,
    oi.is_valid,
    oi.skip
FROM
    user_loan_orders AS ulo
        INNER JOIN
    operator_info AS oi ON oi.user_id = ulo.user_id
WHERE
    ulo.first_loan = 1
        AND ulo.loan_status = 2
        AND LEFT(ulo.create_time, 7) = '2017-09'
        AND oi.skip = 2
ORDER BY oi.create_time
limit {m}, {n}
    '''.format(m=m*100, n=m*100+100)
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
            print('error')


#读取本地文件
def read_analysis_file(num):
    start = time.time()
    print('read_analysis_file()开始执行，请稍候。。。')
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\'
    file = 'operator_info_test_{num}'.format(num=num+1)
    chunk_list = []
    data_chunk = pd.read_csv(path+file+'.csv', sep='\t', encoding='utf-8', chunksize=20)
    for data in data_chunk:
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
                # user_id = data.loc[index, 'user_id']
                mobile = data.loc[index, 'mobile']
                # is_valid = data.loc[index, 'is_valid']
                # skip = data.loc[index, 'skip']
                # create_time = data.loc[index, 'create_time']
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
                        # data_user['user_id'] = user_id
                        data_user['mobile'] = mobile
                        # data_user['is_valid'] = is_valid
                        # data_user['skip'] = skip
                        # data_user['create_time'] = create_time
                        # print(data_user)
                        call_list.append(data_user)
        #删除‘data_src’列， 减少内存
        del data['data_src']
        data_users_all = pd.concat(call_list, ignore_index=True)
        #将两张表作笛卡尔积，为了将data中的字段添加到另一张表中
        data = pd.merge(data, data_users_all, on='mobile', how='inner')
        chunk_list.append(data)
    data_result = pd.concat(chunk_list, ignore_index=True)
    data_result.to_csv(path + file + '_analysis.csv', sep='\t', encoding='utf-8', index=False)
    print('read_analysis_file()已结束，共花费%ds，请稍候。。。'%(time.time()-start))


#再次处理数据，把时间换算成秒，并加上逾期标签。
def analysis(data):
    data['mobile'] = data['mobile'].astype('str', errors='raise')
    data['another_nm'] = data['another_nm'].astype('str', errors='raise')
    data['call_duration'] = data['comm_time'].str.findall('\d+')
    print(data['call_duration'])
    data['call_duration'] = data['call_duration'].map(lambda x: count_call_time(x))
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

    print(data, '****************************************************************************\n')
    data_2 = data[['uid', 'another_nm', 'call_duration', 'start_time', 'comm_mode', 'create_time']]
    # print(data_2, '***********************************************************  data_2\n')
    data_2 = DataFrame(np.array(data_2), columns=['uid', 'other_tel', 'call_duration', 'start_time', 'call_type', 'search_time'])
    data_3 = data[['uid', 'level']].drop_duplicates('uid')
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

def get_result_file():
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\'
    file_list = os.listdir(path)
    file_list_2 = []
    for item in file_list:
        if 'analysis' in item:
            file_list_2.append(item)
    print(file_list_2)
    for key, name in enumerate(file_list):
        data = pd.read_csv(path + name, sep='\t', encoding='utf-8')
        data_result_list = analysis(data)
        for key_2, result_item in enumerate(data_result_list):
            name_index = name.rfind('_')
            result_item.to_csv(path + 'result\\' + name[:name_index] + '_result_{key_2}.csv'.format(key_2=key_2), sep='\t',
                               encoding='utf-8')


#把多个结果文件合并
def conbine_data():
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\result\\test_2\\'
    file_list = os.listdir(path)
    data_list_1 = []
    data_list_2 = []
    for name in file_list:
        if name.endswith('_1.csv'):
            pass
            # data_1 = pd.read_csv(path+name, sep='\t', encoding='uft-8', index=False)
            # data_list_1.append(data_1)
        elif name.endswith('_2.csv'):
            data_2 = pd.read_csv(path + name, sep='\t', encoding='utf-8')
            data_list_2.append(data_2)
        else:
            pass
    # data_1 = pd.concat(data_list_1)
    # data_1.to_csv(path + 'operator_info_test_result_1.csv', sep='\t', encoding='uft-8', index=False)

    data_2 = pd.concat(data_list_2)
    data_2.to_csv(path + 'operator_info_test_result_2.csv', sep='\t', encoding='utf-8')
    # print('\n\ndata_1样本中的用户数量为：', len(data_1.drop_duplicates('uid')))
    print('\n\ndata_2样本中的用户数量为：',len(data_2.drop_duplicates('uid')))


def last_analysis(data):
    count = data.drop_duplicates('uid')
    uid = data['uid']
    return [uid, len(count)]












if __name__ == '__main__':
    num_list = [1, 2, 4, 8, 39]
    count_list = []
    data_list = []
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\result\\qiandaodao_cuishoufen_test\\test_2\\'
    for num in num_list:
        file = 'operator_info_test_{num}_result_2'.format(num=num)
        data = pd.read_csv(path + file + '.csv', sep='\t', encoding='utf-8')
        count_list.append(len(data))
        data_list.append(data)
    data = pd.concat(data_list)
    count_0 = sum(count_list)
    count_1 = len(data)
    data_2 = data.drop_duplicates('uid')
    count_2 = len(data_2)
    print(count_0, count_1, count_2)
    data.to_csv(path + 'operator_info.csv', sep='\t', encoding='utf-8', index=False)


    # merge_list = []
    # path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data_2\\result\\qiandaodao_cuishoufen_test\\test_1\\'
    # for num in num_list:
    #     file = 'operator_info_test_{num}_result_1'.format(num=num)
    #     data = pd.read_csv(path + file + '.csv', sep='\t', encoding='utf-8')
    #     data_list.append(data)
    #     count_list.append(last_analysis(data))
    # for i in range(4):
    #     item = pd.merge(data_list[i], data_list[i+1], on='uid')
    #     print('\nitem:', item)
    #     if len(merge_list) != 0:
    #         merge_list.append(item)
    # print('\nmerge_list:',merge_list)
    # data = pd.concat(data_list, ignore_index=True)
    # count_sum = sum([item[1] for item in count_list])
    # data.to_csv(path+'operator_info.csv', sep='\t', encoding='utf-8')
    #
    # data_1 = data.drop_duplicates('uid')
    # print('\ncount_sum={cs}, data_1={d1}'.format(cs=count_sum, d1=data_1))



    # read_analysis_file(0)
    # for num in range(27, 55):
    #     start = time.time()
    #     print('\n正在处理第{num}块数据。请稍候。。。'.format(num=num+1))
    #     get_local_file(num)
    #     read_analysis_file(num)
    #     print('第{num}块数据处理完毕，共花费{time}s'.format(num=num+1, time=time.time()-start))
    # get_result_file()
    # conbine_data()


    # # data_list=[[] for i in range(3)]
    # # file_list = os.listdir(path)
    # # print(file_list)
    # # for key, name in enumerate(file_list):
    # #     print('开始for')
    # #     if 'analysis' in name:
    # #         data = pd.read_csv(path+name, sep='\t', encoding='utf-8')
    # #         data_result = analysis(data)
    # #         data_result.to_csv(path+'result_'+name, sep='\t', encoding='utf-8')
    # #         # print(data)
    # #         # for key, item in enumerate(data_list):
    # #         #     item.append(data_result_list[key])
    # # for key, item in enumerate(data_list):
    # #     data = pd.concat(item, ignore_index=True)
    # #     data.to_csv(path + 'data_result_{key}.csv'.format(key=key), encoding='utf-8', sep='\t')
    # # data_drop = data_list[0][0].drop_duplicates('user_id')
    # # print(len(data_drop))
    #
    #
    # # os.system('shutdown -s -t 0')