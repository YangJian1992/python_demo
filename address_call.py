import pandas as pd
from pandas import Series, DataFrame
'''
导入pymysql模块，可以直接地把数据读取到内在中。这里没用到，先备着，以后可能会用。
conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao', passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
cur = conn.cursor()
'''



path = 'D:\\work\\database\\ddress_book_rules\\data_code\\test_liuzhibo\\'
file_name_1 = 'one_month_call.csv'
file_name_2 = 'three_month_call.csv'
file_name_3 = 'address_book.csv'

one_month = pd.read_csv(path + file_name_1)
three_month = pd.read_csv(path + file_name_2)
address_book = pd.read_csv(path + file_name_3)


def call_frequency(FRE_NUM= 3, data = one_month):
    # print(one_month)
    #call_num_list列表用来存放mobile和对应的top10的receiver
    call_num_list = []

    #先根据mobile分类，再和通讯录联系起来，但通讯录和通话记录没有联系
    for name, group in data.groupby('mobile'):
        mobile = str(name)
        # print(str(mobile))
        pass

        #通话记录top10联系人
        #groupby().count()返回一个DataFram，但索引值已不再是data中的索引，而是‘receiver’的值,数据为对应的统计数量
        #ascending=False表示降序排列，取前十位，如果数量不到10，会自动取最大行数，还是得到一个DataFrame.
        # 取index对象即为'receiver'的值，values为存放top10联系人手机号的列表
        #
        list = group.groupby('receiver').count().sort_values('receiver', ascending=False)[:10].index.values
        #将mobile用户和对应的top10联系人手机号列表，放在一个列表中，并都添加到call_num_list中
        call_num_list.append([str(name), list])

