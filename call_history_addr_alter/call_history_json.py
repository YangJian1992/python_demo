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


class Call():
    '''
    根据用户的时间条件，得到文件夹下所有用户通话记录。如最近七天的通话记录
    '''
    def __init__(self, call_days):
        self.path = 'D:\\work\\Desktop\\details\\'#json文件路径
        self.name_list = os.listdir(self.path)#存放所有json文件名的列表
        self.call_days = call_days#7天、30天、90天
        self.earliest_time = '未计算最早通话时间'
        self.users_mobile = ()#元组中存放用户的手机号

    def read_data(self):
        '''
        读取json文件中的数据，根据call_days对通话记录进行筛选
        :return:返回列表data_list, 其中的每个元素为DataFrame格式，表示每个用户的通话记录
        '''
        #字典中的每个key为手机号， value为DataFrame，表示每个用户的通话记录
        data_dict ={}
        for file_name in self.name_list:
            print(file_name)
            call_list = []# 存放通话记录的列表
            call_time_list = []#存放通话时间的列表
            #取出文件名中的日期作为create_time
            create_time = file_name[12:22]+' 00:00:00'#不包括当天，通话日期应该小于create_time, 大于threshold_date
            # 首先计算出call_days之前的那个日期d
            a = time.strptime(create_time, "%Y-%m-%d %H:%M:%S")
            b = time.mktime(a) - (self.call_days) * 24 * 3600 + 8 * 3600  # 应为不包括当天，所以需要call_days
            c = time.gmtime(b)
            # 计算出call_days前的那个日期，比如今天是4月2号，七天前的日期就是3-27，那么返回的结果就是3月27号到4月2号的通话记录。
            threshold_date = time.strftime("%Y-%m-%d %H:%M:%S", c)
            #读取文件夹中的json文件，并把它们转化成python对象
            with open(self.path+file_name, 'r', encoding='utf-8') as file:
                data = json.load(file).get('calls', 'null_yj')
            if data != 'null_yj' and len(data) != 0:
                for month_call in data:#month_call表示每个自然月的通话记录
                    if len(month_call['items'])!=0:#'bill_month'通话记录的月份, 且通话记录不能为空
                        for item in month_call['items']:
                            call_time = item.get('time', '0')
                            if call_time >= threshold_date and call_time < create_time:#如果某一条的通话记录中不包括time值，则取时间为0，该条通话记录不会被考虑
                                call_list.append(item)
                            if call_time != 0:
                                call_time_list.append(call_time)
            call_user = DataFrame(call_list)
            call_user.insert(0, 'file_mobile', file_name[:11])#在指定位置插入字段，第0列为手机号
            call_user.insert(1, 'earliest_call_time',  min(call_time_list, default='未找到最早通话时间'))
            #print(call_user)
            data_dict[file_name[:11]] =call_user
        self.users_mobile = tuple(data_dict.keys())
        return data_dict

    # 老用户的运营商规则，需要计算，通话记录从qiandaodao.operator_info_xinde中获取。
    def read_xinde(self):
        select_string = '''
        SELECT id, user_id, mobile, data_src, create_time FROM qiandaodao.operator_info_xinde
        where id BETWEEN 41 and 60;
        '''
        columns = ['id', 'user_id', 'mobile', 'data_src', 'create_time']
        data = mysql_connection(select_string, columns)
        data['data_src'] = data['data_src'].map(lambda x : json.loads(x, encoding='utf-8')['callHistory'])
        def abc(x):
            xx = []
            for item in x:
                xx.extend(item['details'])
            return(xx)
        data['data_src'] = data['data_src'].map(lambda x : abc(x))
        data['data_src'] = data['data_src'].map(lambda x:DataFrame(x))
        data['create_time'] = data['create_time'].map(lambda x: datetime.strftime(x, '%Y-%m-%d :%H:%M:%S'))
        for k in data['data_src'].index:#data_src中的每个字段是dataframe格式的通话记录，给它增加以下三个字段id, user_id, mobile。
            data.loc[k, 'data_src']['id'] = data.loc[k, 'id']
            data.loc[k, 'data_src']['user_id'] = data.loc[k, 'user_id']
            data.loc[k, 'data_src']['mobile'] = data.loc[k, 'mobile']
            data.loc[k, 'data_src']['create_time'] = data.loc[k, 'create_time']
            #取前十位作为日期
            create_date = data.loc[k, 'create_time'][0:10]
            data.loc[k, 'data_src']['create_date'] = create_date
            threshold_date = datetime.strptime(create_date, '%Y-%m-%d') - timedelta(days=self.call_days)
            data.loc[k, 'data_src']['threshold_date'] = datetime.strftime(threshold_date, '%Y-%m-%d')
        #把‘data_src'中的每个dataframe放在一个列表中
        call_data = pd.concat(list(data['data_src']))
        #把call_data中的startTime字段中时间转换成常见格式的北京时间。
        def time_con(x):
            a = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=8)
            return(datetime.strftime(a, '%Y-%m-%d %H:%M:%S'))
        call_data['startTime'] = call_data['startTime'].map(lambda x : time_con(x))
        #按日期筛选通话记录
        result = call_data[(call_data['startTime']>call_data['threshold_date']) & (call_data['startTime']<call_data['create_date'])]
        return result
        #上面的calldata也就是要所有用户的dataframe格式的通话记录

    def rules_xinde(self, call_data):
        #call_data为7天、30天、或者90天的通话记录，计算出规则结果。
        result_list = []
        for key, group in call_data.groupby(['id']):
            if len(group) > 0:
                user_dict = {}
                user_dict['id'] = key
                user_dict['mobile'] = group['mobile'].iloc[0]
                user_dict['user_id'] = group['user_id'].iloc[0]
                user_dict['create_time'] = group['create_time'].iloc[0]
                # print(key, group)
                # 最早通话时间
                user_dict['earliest_call_time'] = min(list(group['startTime']))#也可以直接用Series的min，但是之前有bug
                night_data = group[(group['startTime'].str[11:16] <= '04:00') & (group['startTime'].str[11:16] >= '01:00')]
                # print(night_data)
                # 夜间通话次数， 夜间通话次数占比总通话次数， 近1周夜间通话去重手机号码数量
                user_dict['night_call'] = len(night_data)
                user_dict['night_of_total_call_proportion'] = round(user_dict['night_call'] / len(group), 4)
                user_dict['night_distinct_call'] = len(night_data.drop_duplicates('otherPhone'))
                # 通话次数，通话记录中有效手机号码数量，通话记录中发生过主叫的有效手机号码数量， 通话记录中发生过被叫的有效手机号码数量，
                user_dict['operator_call_num'] = len(group)
                # 有效手机号的正则表达式
                pattern = re.compile(r'(^\+?861[3-9]\d{9}$|^1[3-9]\d{9}$|^00861[3-9]\d{9}$)')
                user_dict['operator_valid_num'] = len(list(filter(lambda x: re.match(pattern, x), group['otherPhone'].drop_duplicates())))
                user_dict['operator_valid_call_num'] = len(list(filter(lambda x: re.match(pattern, x),
                                                                       group[group['callType'] == 0]['otherPhone'].drop_duplicates())))
                user_dict['operator_valid_called_num'] = len(list(filter(lambda x: re.match(pattern, x),
                                                                         group[group['callType'] == 1]['otherPhone'].drop_duplicates())))
                # 通话记录中主叫次数，被叫次数
                user_dict['call_operator_num'] = len(group[group['callType'] == 0])
                user_dict['called_operator_num'] = len(group[group['callType'] == 1])
                group['date'] = group['startTime'].str[:10]  # 取time字段中的前10位，也就是日期，‘2018-04-02’
                # 无通话记录的天数，
                user_dict['no_call_days'] = self.call_days - len(group['date'].drop_duplicates())
                # 将日期数据转换成datetime对象，方便做差。记住要先对date字段排序
                date_list = list(group['date'].drop_duplicates().sort_values().map(
                    lambda x: datetime.strptime(x, '%Y-%m-%d').date()))
                # 连续2天未发生通话行为的次数，连续3天未发生通话行为的次数，
                user_dict['two_day_no_call_case_num'] = 0
                user_dict['three_day_no_call_case_num'] = 0
                for key_no_call, date_item in enumerate(date_list):
                    if key_no_call <= len(date_list) - 2:
                        delta_days = (date_list[key_no_call + 1] - date_item).days  # 求两个datetime对象之间相差的天数
                        if delta_days >= 3:  # 前后两次通话时间相差3天，说明连续两天无通话。
                            user_dict['two_day_no_call_case_num'] += 1
                        if delta_days >= 4:
                            user_dict['three_day_no_call_case_num'] += 1
                # 是否仅主叫，是否仅被叫
                if user_dict['call_operator_num'] > 0 and user_dict[
                    'called_operator_num'] == 0:  # 主叫次数大于0， 被叫次数等于0，说明仅主叫
                    user_dict['only_call'] = 1
                else:
                    user_dict['only_call'] = 0
                if user_dict['call_operator_num'] == 0 and user_dict[
                    'called_operator_num'] > 0:  # 主叫次数等于0， 被叫次数大于0，说明仅被叫
                    user_dict['only_called'] = 1
                else:
                    user_dict['only_called'] = 0
                # 3次及以上通话行为的去重手机号码数量,
                call_count = group.groupby('otherPhone').count()['startTime']  # Series, 根据通话号码统计通话次数,索引为电话号码
                user_dict['three_call_distinct_num'] = len(call_count[call_count >= 3])
                # print('call_count[call_count>=3]:', call_count[call_count >= 3])
                result_list.append(user_dict)
            # print(result_list)
        result = DataFrame(result_list)
        # 改变这三个字段的序号，放在前面好看一些
        result.insert(0, 'id', result.pop('id'))
        result.insert(1, 'user_id', result.pop('user_id'))
        result.insert(2, 'mobile', result.pop('mobile'))
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

#查询通讯录数据，返回DataFrame
def address_book(users_mobile):
    select_string = '''
        SELECT 
    tui.user_id, tui.mobile, udr.device, ab.mobile as book_mobile, udr.login_time, ab.home_location 
FROM
    qiandaodao_risks_control.t_user_info  as tui 
        inner join 
    qiandaodao.user_device_record AS  udr on tui.user_id = udr.user_id
        LEFT JOIN
    qiandaodao.address_book AS ab ON udr.device = ab.device
where tui.mobile in  {mobile};
    '''.format(mobile=users_mobile)
    columns = ['user_id', 'mobile', 'device', 'book_mobile', 'login_time', 'home_location']
    result = mysql_connection(select_string, columns)
    return result

def rules(call_days, call_data, book_data):
    '''
    根据通话记录的天数（7，30， 90），通话记录数据和通讯录数据，计算运营商规则。
    '''
    #降低内存
    for name in ['dial_type', 'location_type', 'dial_type', 'earliest_call_time']:
        call_data[name] = call_data[name].astype('category')
    for name in ['user_id', 'mobile', 'login_time']:
        book_data[name] = book_data[name].astype('category')
    result_list = []
    for key, group in call_data.groupby(['file_mobile']):
        mobile = key
        book_data_group = book_data[book_data['mobile']==mobile]
        if len(group) > 0:
            user_dict = {}
            user_dict['mobile'] = mobile
            #print(key, group)
            #最早通话时间
            user_dict['earliest_call_time'] = group['earliest_call_time'].iloc[0]
            night_data = group[(group['time'].str[11:16]<='04:00') & (group['time'].str[11:16]>='01:00')]
            # 夜间通话次数， 夜间通话次数占比总通话次数， 近1周夜间通话去重手机号码数量
            user_dict['night_call'] = len(night_data)
            user_dict['night_of_total_call_proportion'] = round(user_dict['night_call']/len(group), 4)
            user_dict['night_distinct_call'] = len(night_data.drop_duplicates('peer_number'))
            #通话次数，通话记录中有效手机号码数量，通话记录中发生过主叫的有效手机号码数量， 通话记录中发生过被叫的有效手机号码数量，
            user_dict['operator_call_num'] = len(group)
            #有效手机号的正则表达式
            pattern = re.compile(r'(^\++861[3-9]\d{9}$|^1[3-9]\d{9}$)')
            user_dict['operator_valid_num'] = len(list(filter(lambda x: re.match(pattern, x), group['peer_number'].drop_duplicates())))
            user_dict['operator_valid_call_num'] = len(list(filter(lambda x: re.match(pattern, x), group[group['dial_type']=='DIAL']['peer_number'].drop_duplicates())))
            user_dict['operator_valid_called_num'] = len(list(filter(lambda x: re.match(pattern, x), group[group['dial_type']=='DIALED']['peer_number'].drop_duplicates())))
            #通话记录中主叫次数，被叫次数
            user_dict['call_operator_num'] = len(group[group['dial_type']=='DIAL'])
            user_dict['called_operator_num'] = len(group[group['dial_type']=='DIALED'])
            group['date'] = group['time'].str[:10]#取time字段中的前10位，也就是日期，‘2018-04-02’
            # 无通话记录的天数，
            user_dict['no_call_days'] = call_days-len(group['date'].drop_duplicates())
                #将日期数据转换成datetime对象，方便做差。记住要先对date字段排序
            date_list = list(group['date'].drop_duplicates().sort_values().map(lambda x : datetime.strptime(x, '%Y-%m-%d').date()))
            #连续2天未发生通话行为的次数，连续3天未发生通话行为的次数，
            user_dict['two_day_no_call_case_num'] = 0
            user_dict['three_day_no_call_case_num'] = 0
            for key_no_call, date_item in enumerate(date_list):
                if key_no_call <= len(date_list)-2:
                    delta_days = (date_list[key_no_call+1] - date_item).days#求两个datetime对象之间相差的天数
                    if delta_days >= 3:#前后两次通话时间相差3天，说明连续两天无通话。
                        user_dict['two_day_no_call_case_num'] += 1
                    if delta_days >= 4:
                        user_dict['three_day_no_call_case_num'] += 1
            #是否仅主叫，是否仅被叫
            if user_dict['call_operator_num'] >0 and user_dict['called_operator_num'] == 0:#主叫次数大于0， 被叫次数等于0，说明仅主叫
                user_dict['only_call'] = 1
            else:
                user_dict['only_call'] = 0
            if user_dict['call_operator_num'] == 0 and user_dict['called_operator_num'] > 0:#主叫次数等于0， 被叫次数大于0，说明仅被叫
                user_dict['only_called'] = 1
            else:
                user_dict['only_called'] = 0
            #3次及以上通话行为的去重手机号码数量,
            call_count = group.groupby('peer_number').count()['time']#Series, 根据通话号码统计通话次数,索引为电话号码
            user_dict['three_call_distinct_num'] = len(call_count[call_count>=3])
            print('call_count[call_count>=3]:', call_count[call_count>=3])

            #通讯录规则：
            # 通讯录去重后有效手机号码数量,
            #address_book_valid_num = len(list(filter(lambda x: re.match(pattern,x),  book_data_group['book_mobile'].drop_duplicates())))
            user_dict['address_book_valid_num'] = len(book_data_group[book_data_group['book_mobile'].str.match(r'(^\++861[3-9]\d{9}$|^1[3-9]\d{9}$)')].drop_duplicates('book_mobile'))
            #通话记录中未曾与提供的联系人发生通话行为 0:没有，1：有。将用户的通讯录表和通话记录做内联,key为号码，看看结果是否为空。
            if len(pd.merge(book_data_group.drop_duplicates('book_mobile'), group.drop_duplicates('peer_number'), left_on='book_mobile', right_on='peer_number'))>0:
                user_dict['address_book_call'] = 1
            else:
                user_dict['address_book_call'] = 0
            #通话记录中通讯录联系人通话次数。
            user_dict['operator_address_book_num'] = len(pd.merge(group, book_data_group.drop_duplicates('book_mobile'), left_on='peer_number' ,right_on='book_mobile'))
            #top10联系人中通讯录联系人数量。根据通话号码分类，并统计次数，降序排列，取前10行，如果不到10行，会自动取所有行数。结果df的索引为top10号码，放在列表中。
            top10_list = call_count.sort_values(ascending=False).iloc[:10].index.tolist()
            #注意if a in series:默认是对series的索引进行判断，如果是针对值，应该用户series.valuesa或者series.tolist()
            user_dict['top10_operator_address_book_num'] = len([i for i in top10_list if i in book_data_group['book_mobile'].values])
            #3次及以上通话行为的去重手机号码且为通讯录联系人的数量
            user_dict['three_call_address_book_distinct_num'] = len([i for i in call_count[call_count>=3].index.tolist() if i in book_data_group['book_mobile'].values])
            #通话记录中通讯录联系人有通话的联系人数量
            user_dict['address_book_call_num'] = len([i for i in book_data_group['book_mobile'].drop_duplicates() if i in group['peer_number'].values])
            # # 将数据结果放在用户字典中
            # for rule_key in ['mobile','earliest_call_time', 'night_call', 'night_of_total_call_proportion', 'night_distinct_call',
            #                  'operator_call_num', 'operator_valid_num', 'operator_valid_call_num', 'operator_valid_called_num',
            #                  'call_operator_num', 'called_operator_num', 'no_call_days', 'two_day_no_call_case_num', 'three_day_no_call_case_num',
            #                  'only_call', 'only_called']:
            #     user_dict[rule_key] = eval(rule_key)#字段名和为变量名是一样的，直接用eval更方便

            #再把每个用户的结果放在列表中
            result_list.append(user_dict)
    result = DataFrame(result_list)
    #改变这两个字段的序号，放在前面好看一些
    result.insert(0, 'mobile', result.pop('mobile'))
    result.insert(1, 'earliest_call_time', result.pop('earliest_call_time'))
    return result

    #for key, group in book_data.groupby(['mobile']):




if __name__ == '__main__':
    # #近七天, 30, 90的通话记录
    # calls_7 = Call(7)
    # data_7 = calls_7.read_xinde()
    # print(data_7)
    # result_7 = calls_7.rules_xinde(data_7)
    # print(result_7)
    #把7， 30， 90天的通话记录处理结果，放在一张表中。
    writer = pd.ExcelWriter('D:\\work\\Desktop\\xinde_result.xlsx')
    for i in [7, 30, 90]:
        result = Call(i).rules_xinde(Call(i).read_xinde())
        result.to_excel(writer, sheet_name='{i}天'.format(i=i), encoding='utf-8')
    writer.save()
    # data_7 = pd.concat(list(calls_7.read_data().values()), ignore_index=True)
    # calls_30 = Call(30)
    # data_30 = pd.concat(list(calls_30.read_data().values()), ignore_index=True)
    # calls_90 = Call(90)
    # data_90 = pd.concat(list(calls_90.read_data().values()), ignore_index=True)
    # #所有用户的手机号
    # users_mobile = calls_30.users_mobile
    # #print(users_mobile)
    # #通讯录数据
    # book_data = address_book(users_mobile)
    # #print(book_data)
    # #print(data_30)
    # if len(data_30) > 0 and len(book_data)>0:
    #     result = rules(calls_30.call_days, data_30, book_data)
    #     print(result)
    #     print('\n', '************************************')
