#coding: utf-8
import re
# import os
# import sys
import time
# from datetime import datetime
import pymysql
import numpy as np
from pandas import DataFrame, Series
import pandas as pd

'''
功能：
    通讯录去重后有效手机号码不足10个
    通讯录包含高危号码数量达15个（套现、黑户、代还、中介等）

'''
#在数据库中读取数据
def fun_readdata_mysql(select_string):
    columns_add = ['user_id', 'id', 'device', 'name', 'mobile', 'home_location']
    """
        创建连接读取mysql数据：
        select_string:用以筛选数据库数据的语句
    """
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao', passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    print('正在对数据库进行查询，请稍候...')
    cur.execute(select_string)

    temp = DataFrame(list(cur.fetchall()), columns=columns_add)
    print('已经查询到数据，正在处理，请稍候...累计花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return(temp)


def address_book_remove(data):
    print('输入的数据共%d行'%len(data))
    print('正在对数据进行筛选，请稍候...')
    #给数据增加一列.默认为'0'
    data['mobile_9'] = 'NULL'
    #手机号前9位重复超过20次的user_id和相应的9位手机号。
    mobile_remove_list = []
    # word_list = ['易信', '微会', 'poo', 'mimicall', 'Skype', '来电', '专线', '阿里通', '有信', '触宝', '微话',
    #              '钉钉', '酷宝', '微信', 'Skype']
    # word_list = ['专线']
    word_list = ['专线', '易信', '微会', 'poo', 'mimicall', 'Skype', '来电', '阿里通', '有信', '触宝', '微话',
                 '钉钉', '酷宝', '微信', 'Skype']
    risky_mobile = []
    # risky_num变量计算通讯录中高危号码数量是否达到15个
    risky_num = 0
    #返回的结果，包括user_id和对应的有效手机号数量
    user_mobile = {}

    #step1:
    # start2 = time.time()
    # #c对data_filter数据进行后续的删除的操作，并把这个数据返回。使用isin方法，不用str.来调用，用Series来调用。
    # #注意~运算符的使用，表示取反，非的意思，~和isin搭配使用。
    # # data_filter = data[(data['mobile'].str.len() == 11) & (data['mobile'] < '18999999999') & (data['mobile'] > '13000000000') &
    # #                    ~(data['name'].str.contains('专线'))]
    # # print('通讯录11位筛选step1，正在生成数据，请稍候...累计花费时间为%ds。' % (time.time() - start2))
    # # print('数量:%d'%len(data_filter))
    # #step2:
    # start3 = time.time()
    # print('已经完成初步筛选，正在对前9位手机号进行判断，请稍候...')

    # print('通讯录前九位进行筛选，正在生成数据，请稍候...累计花费时间为%ds。' % (time.time() - start3))
    # print('两步筛选后的数据中，总行数为%d'%len(data))
    start4 = time.time()
    #第二种方法，比较哪种快
    for index in data.index:
        mobile = str(data.ix[index,['mobile']].values[0])
        flag = 0
        #name也有数字格式的，需考虑str
        name = str(data.ix[index,['name']].values[0])
        if re.findall(r"^1[3-8]\d{9}$", mobile):
            for word in word_list:
                # data.drop(data[data['name'].str.contains(word)].index, axis=0, inplace=True)
                if word in name:
                    data.drop(index, axis=0, inplace=True)
                    #删除该索引后，就不用对word_list遍历,并把flag改为1
                    flag = 1
                    break
            # 符合手机号规则，且不在word_list中时，此时的索引值不会被删除，所以把手机号的前九位作为mobile_9的值
            if flag==0:
                data.ix[index, ['mobile_9']] = mobile[:9]
        else:
            data.drop(index, axis=0, inplace=True)
    print('通讯录筛选step3，正在生成数据，请稍候...累计花费时间为%ds。' % (time.time() - start4))

    for user_id, group in data.groupby('user_id'):
        user_id = str(user_id)
        # for item in risky_mobile:
        #     # 如果含有号码item，则计数变量risky_num加一
        #     risky_item = mobile_only[mobile_only['mobile'] == item]
        #     if len(risky_item):
        #         risky_num += 1

        # 不管数量多少，计数变量重置为0
        # mobile_data为DataFrame，索引为mobile_9,值为mobile_9的数量,但字段中去除了'mobile_9'字段,所以用'mobile'字段表示>10
        mobile_data = group.groupby('mobile_9').count()
        # mobile_remove为一个列表，表示要删除的九位数字的手机号
        #mobile_need为一个列表，表示要保留的九位数字的手机号
        mobile_remove = mobile_data[mobile_data['mobile'] > 20].index.values
        mobile_need = mobile_data[mobile_data['mobile'] <= 20].index.values

        group_1 = group[group['mobile_9'].isin(mobile_need)]
        mobile_only = group_1.drop_duplicates(['mobile'])
        #每个用户的user_id, 有效手机号的数量。
        user_mobile[user_id] = [user_id, len(mobile_only)]
        if len(mobile_remove)> 0:
            print(mobile_remove)
            # mobile_remove_list.append([user_id, mobile_remove])
            # print(mobile_remove_list)

            # 再看看group表中，把'mobile_9'字段中，在mobile_remove列表中的数据全删掉。注意group是独立的数据，一定要用data去调用drop()去删除。
            data.drop(group[group['mobile_9'].isin(mobile_remove)].index, axis=0, inplace=True)
        risky_num = 0

    # #在上述筛选过程的基础上，进一步筛选。删掉手机号前九位数字重复次数超过10次的数据，并删掉
    # for user_id, group in data.groupby('user_id'):
    #     #mobile_data为DataFrame，索引为mobile_9,值为mobile_9的数量,但字段中去除了'mobile_9'字段,所以用'mobile'字段表示>10
    #     mobile_data = group.groupby('mobile_9').count()
    #     #mobile_remove为一个列表，表示要删除的九位数字的手机号
    #     mobile_remove = mobile_data[mobile_data['mobile']>20].index.values
    #     #把要删除的号码打印出来看看
    #     if len(mobile_remove):
    #         mobile_remove_list.append([user_id, mobile_remove])
    #         print(mobile_remove_list)
    #     #再看看group表中，把'mobile_9'字段中，在mobile_remove列表中的数据全删掉。注意group是独立的数据，一定要用data去调用drop()去删除。
    #     data.drop(group[group['mobile_9'].isin(mobile_remove)].index, axis=0, inplace=True)

    return [user_mobile, data]




# #函数返回每个用户的user_id, 有效手机号的数量，高危号码的数量
# def address_mobile_num(data):
#     user_mobile = []
#     #以字符串形式存放着高危号码，所有数据中号码要求为字符串格式的。
#     risky_mobile = []
#     #risky_num变量计算通讯录中高危号码数量是否达到15个
#     risky_num = 0
#     data_1 = data.copy()
#     for name, group in data_1.groupby('user_id'):
#         # mobile_only为去重后的通讯录，为DataFrame格式的。
#         mobile_only = group.drop_duplicates('mobile')
#
#         #也可以考虑mobile_only[mobile_only['mobile'].isin(risky_mobile)],得到的数据表，就是存在于risky_mobile中的mobile，用len()用统计长度即可。
#         for item in risky_mobile:
#             # 如果含有号码item，则计数变量risky_num加一
#             risky_item = mobile_only[mobile_only['mobile'] == item]
#             if len(risky_item):
#                 risky_num += 1
#         #每个用户的user_id, 有效手机号的数量，高危号码的数量。
#         user_mobile.append({str(name), len(mobile_only), risky_num})
#         # 不管数量多少，计数变量重置为0
#         risky_num = 0
#     return user_mobile

def count_user(data):
    print('数量：%d'%len(data.drop_duplicates('user_id')))

'''
#有效号码不足10个
        if len(mobile_only) < 10:
            #去掉不符合条件的索引
            data_1.drop(grop.index, axis = 0, inplace=True)
            continue

        #只对有效号码达到10个的通讯录，处理高危号码情况
        else:
            #遍历高危号码
            for item in risky_mobile:
                #如果含有号码item，则计数变量risky_num加一
                risky_item = mobile_only[mobile_only['mobile'] == item]
                if len(risky_item):
                    risky_num += 1
            if risky_num >= 15:
                data_1.drop(group.index, axis=0, inplace=True)
            #不管数量多少，计数变量重置为0
            risky_num = 0
    #返回值data_1为剔除不符合两条件后的数据，输入参数data不变
    return data_1
'''

start = time.time()

#
# address_book = fun_readdata_mysql(
#     '''
#     select tt.user_id,ab.* from address_book ab
# inner join (
# select a.user_id,a.device,a.s from
# (select dr.user_id,dr.device,dr.login_time,ao.create_time,to_seconds(ao.create_time)-to_seconds(dr.login_time) as s from user_device_record dr
# left join (select * from credit_apply_orders where auth_status=2 group by user_id) ao on ao.user_id=dr.user_id
# where dr.user_id in
# (select ao.user_id
# from
# credit_apply_orders ao
# left join (select * from user_loan_orders where first_loan=1 and loan_status=2 and loan_time>='2017-06-01' and loan_time <'2017-09-01' group by user_id ) lo on lo.user_id=ao.user_id
# left join users u on u.id=ao.user_id
# left join (select user_id,count(id) num , sum(if(overdue_days>0,1,0)) yqbs,max(overdue_days) overdue from user_loan_orders where first_loan=0 and loan_time>='2017-06-01'  and loan_status=2 group by user_id ) lo1 on lo1.user_id=ao.user_id
# where ao.create_time>='2017-06-01' and ao.create_time<'2017-09-01' and ao.auth_status='2'  and ao.auth_time<'2017-09-01' and lo.user_id is not null) and  (to_seconds(ao.create_time)-to_seconds(dr.login_time))<=0 ) a group by a.user_id having a.s=max(a.s)) tt
# on ab.device=tt.device ;
# ''')
path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
file_name_1 = 'address_book_old.csv'
file_name_2 = 'address_book_1.csv'

#address_book为数据库中读取的数据
# address_book.to_csv(path + file_name_1, sep='\t')
#注意encoding='gbk'，不是utf-8
address_book_old = pd.read_table(path+file_name_1, encoding='gbk', na_values='NULL')
# count_user(address_book_old)
#address_book_2为筛选后的数据
user_mobile = address_book_remove(address_book_old[:8000])
print(user_mobile[0])
print(user_mobile[1])
# address_book_2.to_csv(path + file_name_2, sep='\t')

# data = pd.read_table(path+file_name_2, encoding='gbk')
# def count_user(data):
#     print('即将对数据的用户数量进行统计，累计花费%ds'%(time.time() - start))
#     print('数量：%d'%len(data.drop_duplicates('user_id')))
#     print('已完成对数据的用户数量进行统计，累计花费%ds' % (time.time() - start))

# count_user(data)
end = time.time()
print('花费时间：%ds'%(end-start))
# path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
# file_name_1 = 'addressBookTest.xlsx'
# address_book = pd.read_csv(path + file_name_1)
