#coding:utf-8
import os
from pandas import Series, DataFrame
import pandas as pd
import time
#原始数据dataOld
dataOld = pd.read_pickle('D:\\work\\database\\df_out.pkl')
# #去掉归属地为NULL的值
# dataValid = dataOld[dataOld['home_location']!='NULL']
# dataValid.to_pickle('D:\\work\\database\\dataValid.pkl')
dataValid = pd.read_pickle('D:\\work\\database\\dataValid.pkl')
tem_loca = list(dataValid['home_location'])
totalNum = len(dataValid)
print('总数量为(99912)?:%d'%totalNum)
#把home_location中的“中国电信”等字符去掉，只保留地名，并作为新的字段home_location_remove添加到数据中
for i in range(0, totalNum):
    tem_loca[i] = tem_loca[i][:-5]
dataValid['home_location_remove'] = tem_loca

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
def user_info():
    # 不同用户的通讯录,用列表user_book表示，共1707个用户。如user_book[0]表示第0个用户，它也是一个列表，有两个子元素。其中，第一个子元素user_book[0][0]
    # 为一个字符串，表示用户名，而第二个子元素user_book[0][1]表示一个DataFrame，存储着该用户通讯录的信息。
    user_book = []
    # 根据user_id分类，得到一个迭代对象，用userIterator表示
    userIterator = dataOld.groupby(dataOld['user_id'])
    for i in userIterator:
         user_list = list(i)
         #drop_duplicates(['字段'])可以去掉字段中重复的值
         user_list[1] = user_list[1].drop_duplicates(['mobile'])
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


#判断用户的user_id是否正常
def user_condition():
    user_id = 'user_id'
    s = 0
    # user_id_list = dataOld.drop_duplicates([user_id])[user_id]
    for item in dataOld[user_id]:
        if item != '':
            if len(item) > 10 or len(item) < 3:
                s = s+1
                print(item)
                print('共%d个异常数据' % s)
        else:
            print('该项数据为空')
    return (user_id_list)

    print(user_id_list)
#计算不同客户的通讯录数量

# print(user_condition())
# print(len(user_info()))

# print(len(user_lookup('1020965', ['妈','舅'], 0)))
start = time.time()
num_con = NumConditions(['爸', '妈'])
print(num_con.contact_num())
print('-------------------------------------------程序运行结束！-------------------------------------------')
end = time.time()
print('运行时间为:%ds'%(end-start))