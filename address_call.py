#coding: utf-8
'''
功能：
    近1个月通话记录中通讯录联系人通话次数小于等于3次
    近3个月通话记录中通讯录联系人通话次数小于等于8次
    近3个月通话记录中通讯录联系人有通话的联系人数量小于等于2个
    近1个月top10联系人无通讯录联系人
    近1个月发生过3次及以上通话行为的去重手机号码且为通讯录联系人的数量等于0个


write by yang_jian
'''

import pandas as pd
from pandas import Series, DataFrame




#输入参数为一个月或三个月的通话记录数据，返回列表，列表依次表示用户id、通讯录联系人的通话次数、
# 通话记录中top10联系人在通讯录联系人中的数量、通话记录中通讯录联系人中数量, 通话次数超过3次的通讯录联系人数量。
def user_call_info(data):
    # print(one_month)
    #这是通讯录联系人数据
    data_address = pd.read_table(path + file_name_2, sep='\t', encoding='utf-8')
    # 往空字典中添加键值对，键为用户id，值为一个列表，依次表示用户id、通话次数、top10联系人数量。
    user_list = []
    for user_id, group in data.groupby('user_id'):
        #mobile_count变量统计每个用户的通话记录中，top10联系人在通讯录中的数量
        mobile_count = 0
        user_id = str(user_id)
        #得mobile_list列表，通讯录联系人的号码表
        mobile_list = data_address[data_address['user_id']==user_id]['mobile'].drop_duplicates(['mobile'])['mobile'].values
        #通讯录联系人的通话记录
        call_book = group[group['receiver'].isin(mobile_list)]
        #与通讯录联系人的通话次数
        call_num = len(call_book)
        #通讯录联系人的数量：
        address_book_num = len(call_book.drop_duplicates('receiver'))

        #通话次数超过3次的通讯录联系人数量为three_times
        temp_count = group.groupby('receiver').count()
        three_times = len(temp_count[temp_count['mobile']>3])

        #通话记录top10联系人
        #groupby().count()返回一个DataFrame，但索引值已不再是data中的索引，而是‘receiver’的值,数据为对应的统计数量，但字段中去除了'receiver',所以排序用'mobile'字段
        #ascending=False表示降序排列，取前十位，如果数量不到10，会自动取最大行数，还是得到一个DataFrame.
        # 取index对象即为'receiver'的值，.values为存放top10联系人手机号的列表
        top_10_list = group.groupby('receiver').count().sort_values('mobile', ascending=False)[:10].index.values
        for mobile in top_10_list:
            if mobile in mobile_list:
                mobile_count = mobile_count + 1

        #往列表中添加用户id、通讯录联系人的通话次数、通话记录中top10联系人数量在通讯录中的数量，通话记录中通讯录联系人中数量, 通话次数超过3次的通讯录联系人数量
        user_list = user_list.append([user_id, call_num, mobile_count, address_book_num, three_times])



def one_month_data():
    one_call_history_1 = pd.read_table(path + file_name_11, encoding='utf-8')
    user_call_1 = user_call_info(one_call_history_1)
    return user_call_1


path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
file_name_11 = 'call_history_old_1.csv'
file_name_2 = 'address_book.csv'
file_name_3 = ''

user_call_1 = one_month_data()

