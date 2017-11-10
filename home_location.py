#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
#data_home_location 总数量为:343150
data_home_location = pd.read_pickle('D:\\work\\database\\home_location.pkl')

#data_call_history 总数量为:102994
data_call_history = pd.read_pickle('D:\\work\\database\\call_history.pkl')

print('data_home_location 总数量为:%d'%len(data_home_location))
print(len(data_call_history.ix[20,['mobile']][0]))
print('data_call_history 总数量为:%d'%len(data_call_history))








#把home_location中的“中国电信”等字符去掉，只保留地名，并作为新的字段home_location_remove添加到数据中
# for i in range(0, totalNum):
#     tem_loca[i] = tem_loca[i][:-5]
# dataValid['home_location_remove'] = tem_loca

# class user_info():
#
#     def __init__(self, user_id):
#         self.user_id = user_id
#
#
#     def user_info(self):
#         # 根据user_id分类，得到一个迭代对象，用userIterator表示
#         userIterator = dataValid.groupby(dataValid['user_id'])
#
#         # 不同用户的通讯录,用列表userBook表示，共1707个用户。如userBook[0]表示第0个用户，它也是一个列表，有两个子元素。其中，第一个子元素userBook[0][0]
#         # 为一个字符串，表示用户名，而第二个子元素userBook[0][1]表示一个DataFrame，存储着该用户通讯录的信息。
#         for i in userIterator:
#              user_list = list(i)
#             if  user_list[0] == self.user_id:
#                 return  user_list[1]
#
#
# user1 = user_info('1020965').user_info()


#user_info()返回一个列表，存放不同用户的通讯录，并且去掉通讯录中重复号码。
def user_info(data, group_column):
    # 不同用户的通讯录,用列表user_book表示，共1707个用户。如user_book[0]表示第0个用户，它也是一个列表，有两个子元素。其中，第一个子元素user_book[0][0]
    # 为一个字符串，表示用户名，而第二个子元素user_book[0][1]表示一个DataFrame，存储着该用户通讯录的信息。
    user_book = []
    # 根据user_id分类，得到一个迭代对象，用userIterator表示
    userIterator = data.groupby(data[group_column])
    for i in userIterator:
         user_list = list(i)
         #drop_duplicates(['字段'])可以去掉字段中重复的值
         user_list[1] = user_list[1].drop_duplicates([group_column])
         user_book.append(user_list)
    return user_book


#得到一个列表，每个子列表表示一个用户的id和通讯录数量。
def user_count():
    user_book_count = user_info()
    for item in user_book_count:
        item[1] = len(item[1])
    return user_book_count


#user_lookup(user_id)通过user_id查询用户的通讯录信息(DataFram格式)
#user_id为用户id;search_list是查询的关键字列表;flag为状态值，0为查询姓名，1为查询地址
def user_lookup(user_id, search_list, flag):
    #调用函数获取通讯录
    user_book = user_info()
    #将字段名用变量表示，方便修改
    name = 'name'
    home_location_remove = 'home_location_remove'
    #item为每个用户的通讯录，它是一个列表，含有两个元素，第一元素是用户id，第二个元素是DataFrame，存储着用户的通讯录。
    for item in user_book:
        if item[0] == user_id:
            # 建立一个空的DataFrame，把以下符合条件的DataFrame添加到这个空的变量中，作为函数的返回值。
            empty_tem = item[1].drop(item[1].index)
            if len(search_list)!=0:
                for word in search_list:
                    #注意pandas中的str.contains函数，判断是否包含word。
                    if flag==0:
                        wordData = item[1][item[1][name].str.contains(word)]
                        # ignore_index = True表示使用新的索引，否则添加不了
                    elif flag == 1:
                        wordData = item[1][item[1][home_location_remove].str.contains(word)]
                    empty_tem = empty_tem.append(wordData, ignore_index = True)
            #参数searchList列表为空，返回用户所有联系人
            else:
                empty_tem = empty_tem.append(item[1], ignore_index=True)
            return empty_tem
    print('抱歉，找不到该用户id：%s，请核实。\n谢谢！'%user_id)


#统计不同用户中，符合条件的通讯录数量
class NumConditions():
    def __init__(self, condition_1):
        self.condition_1 = condition_1
        print('正在计算符合条件的联系人数量所占的比例。。。')


    def contact_num(self):
        user_id = 'user_id'
        user_id_list = user_count()
        num = []
        for id_item in user_id_list[0:50]:
            tem1 = user_lookup(id_item[0], self.condition_1, 0)
            #tem2和tem3表示进件地和户籍地，每个用户应该不一样，不能手动输入
            # tem2 = user_lookup(id_item, contion_2, 1)
            # tem3 = user_lookup(id_item, contion_3, 1)
            # num = num.append([id_item, len(tem1), len(tem2), len(tem3)])
            # num = num.append([id_item, len(tem1)]) 这种写法是错误的，num为NoneType，因为append没有返回值
            num.append([id_item[0], len(tem1), round(len(tem1) / id_item[1], 4)])
        return num


#判断用户的data为DataFrame格式的数据，column_1为要判断的字段名，
def user_condition(data, column_1):
    #读取数据，
    # data = data_call_history
    # column_1 = 'mobile'
    s = 0
    # user_id_list = dataOld.drop_duplicates([user_id])[user_id]
    # for item in dataOld[user_id]:
    for index in data.index:
        if len(data.ix[index,[column_1]][0])<4:
            print(data.ix[index,[column_1]])
            s += 1
    print('%s的异常数据一共%d条'%(column_1, s))
    #     if item != '':
    #         if len(item) > 10 or len(item) < 3:
    #             s = s+1
    #             print(item)
    #             print('共%d个异常数据' %s)
    #     else:
    #         print('该项数据为空')

#计算不同客户的通讯录数量

#根据手机号找到归属地。data为包含手机号的DataFrame数据
def add_locations(data):
        print('输入参数的类型为%s'%type(data))
        print('输入参数的长度%d'%len(data))

        data = DataFrame(data, columns=["id", 'mobile', 'mobile_province', 'mobile_city', 'mobile_addr',  'receiver',
                                        'receiver_province', 'receiver_city', 'receiver_addr','call_time', 'call_addr','call_type'])
        column_tem1 = 'mobile'
        column_tem2 = 'receiver'

        print('\n正在处理数据，请稍候......')
        for index_data in data.index:
            # data.index是一个序列，所以index_data是一个整数int
            # print('正在查找第%d条数据'%index_data)
            target_1 = (data.ix[index_data, [column_tem1]].values[0])
            target_2 = (data.ix[index_data, [column_tem2]].values[0])

            #lookup_location函数的返回结果为一个列表
            result_list_1 = lookup_location(target_1)
            result_list_2 = lookup_location(target_2)
            #data.ix[index,['a'] = xx。改变index索引对应的若干行或一行，'a'列的值
            # if len(target_1)==11:
            data.ix[index_data, ['%s_province'%column_tem1]] = result_list_1[0]
            data.ix[index_data, ['%s_city'%column_tem1]] = result_list_1[1]
            data.ix[index_data, ['%s_addr'%column_tem1]] = result_list_1[0] +','+ result_list_1[1]
            #
            # else:
            #     data.ix[index_data, ['%s_province'%column_tem1]] = '0'
            #     data.ix[index_data, ['%s_city'%column_tem1]] = '0'
            #     data.ix[index_data, ['%s_addr'%column_tem1]] = '0'

            # if len(target_2)==11:
            data.ix[index_data, ['%s_province'%column_tem2]] = result_list_2[0]
            data.ix[index_data, ['%s_city'%column_tem2]] = result_list_2[1]
            data.ix[index_data, ['%s_addr'%column_tem2]] = result_list_2[0] +','+ result_list_2[1]

            # else:
            # data.ix[index_data, ['%s_province'%column_tem2]] = '0'
            # data.ix[index_data, ['%s_city'%column_tem2]] = '0'
            # data.ix[index_data, ['%s_addr'%column_tem2]] = '0'

        print('------返回新的数据表：------')
        print(data)
        print('\n------函数add_locations运行结束------')
        return data


        # for index_data in data.index:
        #     for index_item in data_tem.index:
        #         #data_choice也是一个DataFrame
        #         if data_tem.ix[index_item,[column_tem]].values[0] == data.ix[index_data,[column_tem]].values[0][:7]:
        #             data.ix[index_data, ['province_%s' % column_tem]].values[0] = data_tem.ix[index_item,['province']].values[0]
        #             data.ix[index_data, ['city_%s' % column_tem]].values[0] = data_tem.ix[index_item, ['city']].values[0]
        #         if data_tem.ix[index_item,[column_tem]].values[0] == data.ix[index_data,[column_tem2]].values[0][:7]:
        #             data.ix[index_data, ['province_%s' % column_tem2]].values[0] = data_tem.ix[index_item, ['province']].values[0]
        #             data.ix[index_data, ['city_%s' % column_tem2]].values[0] = data_tem.ix[index_item, ['city']].values[0]



        # for item in data[column_tem]:
        #     if len(item) == 11:
        #         for index in data_tem:
        #             data_choice = data_tem.ix[index, [column_tem, 'province', 'city']]
        #             if data_choice[0] == item:
        #                 data.ix[item.index,['province_%s'%column_tem]] = data_choice[1]
        #                 data.ix[item.index, ['city_%s'%column_tem]] = data_choice[2]

#输入字符串格式的手机号，返回一个列表，列表的第一个元素为省名，第二个元素为市名。
#定义全局变量lookup_count，用来计数查找的次数
lookup_count =1
def lookup_location(target_number_0):
    #如果手机号码符合规则，则执行查找程序。否则返回的列表中，的省和市都为'0'
    if re.findall(r"1[3-8]\d{9}",target_number_0):
        target_number = int(target_number_0[:7])
        mobile_series = data_home_location['mobile']
        global lookup_count
        low = 0
        high = len(mobile_series) - 1
        # print('------正在进行第%d次查找，  手机号：%s  ，请稍候......------' % (lookup_count, target_number_0))
        lookup_count += 1
        while (low <= high):
            # 用//取整数
            mid = (low + high) // 2
            midval = int(mobile_series[mid])

            if midval < target_number:
                low = mid + 1
            elif midval > target_number:
                high = mid - 1
            elif midval == target_number:
                # print('已找到 手机号:%s， \n归属地为：%s,%s' % (
                # target_number_0, data_home_location.ix[mid, ['province', 'city']].values[0],
                # data_home_location.ix[mid, ['province', 'city']].values[1]))
                # print('------手机号在data_home_location中的索引为%d------\n' %mid)
                return [data_home_location.ix[mid, ['province', 'city']].values[0],
                          data_home_location.ix[mid, ['province', 'city']].values[1]]

        # print('找不到手机号%s'%target_number_0)
        return ['0','0']

    else:
        # print('手机号不符合规则%s' % target_number_0)
        return ['-1', '-1']

    # print('不好意思，没有找到 手机号：%d。\n'%target_number)



    # find_location(data_call_history['mobile'])

# def add_locations():
#     #读取数据
#     data1 = data_call_history
#     data1_loc_mobile = data1['mobile']
#     data1_loc_receiver = data1['receiver']
#
#     data2 = data_home_location
#
#     for index in data1.index:
#         tem = data1.ix[index, ['mobile', 'receiver']]
#         tem1 = tem[0][:8]
#         tem2 = tem[1][:8]
#         data2[data2['mobile']==tem1]

# user_info(data, group_column)



#判断程序运行时间，不知道是否合适。如果只有两个参数，第二个参数会给fun_2么？
def program_time(fun_1, a,fun_2=None, fun_3=None):
    start = time.time()
    while True:
        file_path = 'D:\\work\\database\\home_location_new.txt'
        if os.path.exists(file_path):
            print('错误：%s文件已经存在了'%file_path)
        else:
            break
    data = fun_1(a)
    #index为False，则生成的文本中不显示index。to_csv可以生成txt的文本
    data.to_csv(file_path, index=False, sep='\t')
    # file_data = open(file_path, 'w')
    # file_data.close()

    if fun_2 != None:
        fun_2()
        print('fun_2执行结束')
        if fun_3 != None:
            fun_3()
            print('fun_3执行结束')
    print('-------------------------------------------程序运行结束！-------------------------------------------\n')
    end = time.time()
    print('\n运行时间为:%d秒'%(end-start))

program_time(add_locations, data_call_history)

#将全局变量重置
lookup_count =1