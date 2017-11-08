#coding:utf-8
import os
from pandas import Series, DataFrame
import pandas as pd
#原始数据dataOld
# dataOld = pd.read_pickle('D:\\work\\database\\df_out.pkl')
# #去掉归属地为NULL的值
# dataValid = dataOld[dataOld['home_location']!='NULL']
# dataValid.to_pickle('D:\\work\\database\\dataValid.pkl')
dataValid = pd.read_pickle('D:\\work\\database\\dataValid.pkl')
temLoca = list(dataValid['home_location'])
totalNum = len(dataValid)
print('总数量为(99912)?:%d'%totalNum)

for i in range(0, totalNum):
    temLoca[i] = temLoca[i][:-5]
dataValid['home_location_remove'] = temLoca





# class UserInfo():
#
#     def __init__(self, user_id):
#         self.user_id = user_id
#
#
#     def userInfo(self):
#         # 根据user_id分类，得到一个迭代对象，用userIterator表示
#         userIterator = dataValid.groupby(dataValid['user_id'])
#
#         # 不同用户的通讯录,用列表userBook表示，共1707个用户。如userBook[0]表示第0个用户，它也是一个列表，有两个子元素。其中，第一个子元素userBook[0][0]
#         # 为一个字符串，表示用户名，而第二个子元素userBook[0][1]表示一个DataFrame，存储着该用户通讯录的信息。
#         for i in userIterator:
#             userList = list(i)
#             if userList[0] == self.user_id:
#                 return userList[1]
#
#
# user1 = UserInfo('1020965').userInfo()

#userInfo()返回一个列表，存放不同用户的通讯录
def userInfo():
    # 不同用户的通讯录,用列表userBook表示，共1707个用户。如userBook[0]表示第0个用户，它也是一个列表，有两个子元素。其中，第一个子元素userBook[0][0]
    # 为一个字符串，表示用户名，而第二个子元素userBook[0][1]表示一个DataFrame，存储着该用户通讯录的信息。
    userBook = []
    # 根据user_id分类，得到一个迭代对象，用userIterator表示
    userIterator = dataValid.groupby(dataValid['user_id'])

    for i in userIterator:
        userList = list(i)
        userBook.append(userList)
    return userBook

#userLookup(user_id)通过user_id查询用户的通讯录信息(DataFram格式)
def userLookup(user_id, keyContact):
    userBook = userInfo()
    for item in userBook:
        if item[0] == user_id:
            if len(keyContact)!=0:
                print(type(item[1]))
                for word in keyContact:
                    item[1] = item[1][item[1]['name'].find(word)>(-1)]

            return item[1]

print(userLookup('1020965', ["朱亚琴"]))