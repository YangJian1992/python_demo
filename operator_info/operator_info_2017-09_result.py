from pandas import Series, DataFrame
import pandas as pd
import time
import json
import pymysql
import os

#findall返回的列表中包含时间数据，统一转换成秒
def count_call_time(time_list):
    for x, value in enumerate(time_list):
        time_list[x] = int(value)
    if len(time_list) == 3:
        return ((time_list[0])*3600 + time_list[1]*60 + time_list[2])
    elif len(time_list) == 2:
        return ((time_list[0])*60 + time_list[1])
    elif len(time_list) == 1:
        return time_list[0]
    else:
        return (-1)


def get_local_file_2():
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data\\'
    file = 'user_loan_order_0915'
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
		users.mobile
    FROM
        user_loan_orders AS ulo
    INNER JOIN users ON users.id = ulo.user_id
    WHERE
        ulo.first_loan = 1
            AND ulo.loan_status = 2
            AND LEFT(ulo.create_time, 10) between '2017-09-01' and '2017-09-15';
    '''
    # columns_add = ['id', 'user_id', 'mobile', 'name', 'reg_time', 'is_valid', 'id_card', 'create_time', 'data_src', 'skip']
    user_loan_data = mysql_connection(select_string)
    print(user_loan_data)
    print('已经从数据库获得数据，正在生成本地文件，请稍候...')
    user_loan_data['level'] = 'null'
    not_overdue_index = user_loan_data[user_loan_data['overdue_status'] == 0].index
    if len(not_overdue_index) != 0:
        user_loan_data.ix[not_overdue_index,['level']] = '未逾期'

    overdue_index_1 = user_loan_data[(user_loan_data['overdue_status']==1) & (user_loan_data['repay_status']==2)].index
    if len(overdue_index_1) != 0:
        user_loan_data.ix[overdue_index_1, ['level']] = '逾期已还'

    overdue_index_2 = user_loan_data[(user_loan_data['overdue_status'] == 1) & (user_loan_data['repay_status'] != 2)].index
    if len(overdue_index_2) != 0:
        user_loan_data.ix[overdue_index_2, ['level']] = '逾期未还'
    user_loan_data.to_csv(path + file + '.csv', sep='\t', encoding='utf-8', index=False)

def read_analysis_file(file):
    print('函数开始。。。')
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data\\'
    # file = 'operator_info_test_{num_1}_{num_2}'.format(num_1=num_1, num_2=num_2)

    data = pd.read_csv(path+file, sep='\t', encoding='utf-8')
    data['mobile'] = data['mobile'].astype('str', errors='raise')
    data['another_nm'] = data['another_nm'].astype('str', errors='raise')
    data['call_duration'] = data['comm_time'].str.findall('\d+')
    print(data['call_duration'])
    data['call_duration'] = data['call_duration'].map(lambda x:count_call_time(x))
    print(data['call_duration'])
    data['uid'] = 'uid' + data['mobile']
    return data

if __name__ == '__main__':
    path = 'D:\\work\\dian_hua_bang\\cui_shou_fen\\test_data\\'
    file = 'user_loan_order_0915'
    user_loan_data = pd.read_csv(path+file+'.csv', sep='\t', encoding='utf-8')
    data_list = []
    file_list = os.listdir(path)
    print(file_list)
    for key, name in enumerate(file_list):
        print('开始for')
        if 'operator_info_test_1_' in name:
            data = read_analysis_file(name)
            print(data)
            data_list.append(data)
            data = None
    # for i in [1, 2, 3]:
    #     for j in [1, 2, 3]:
    #         data = read_analysis_file(i, j)
    #         data_list.append(data)
    data = pd.concat(data_list, ignore_index=True)
    data_result = pd.merge(data, user_loan_data, how='inner', left_on='user_id', right_on='user_id')
    print(data_result)
    data_drop = data_result.drop_duplicates('user_id')
    data_result.to_csv(path+'data_result.csv', encoding='utf-8', sep='\t')
