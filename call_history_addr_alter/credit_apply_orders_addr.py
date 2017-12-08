import pandas as pd
from pandas import Series, DataFrame
import time
import pymysql
import json

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


def get_local_file(data):
    path = 'D:\\work\\database\\province-city\\'
    file = 'credit_apply_orders_addr'
    data.to_csv(path+file+'.csv', sep='\t', encoding='utf-8', index=False)


def read_local_file():
    path = 'D:\\work\\database\\province-city\\'
    file = 'credit_apply_orders_addr'
    data = pd.read_table(path+file+'.csv', encoding='utf-8', sep='\t', chunksize=100000)
    print('已经读取数据，正在进行处理，请稍候...')
    for item in data:
        data_old = addr_analysis(item)
        data_string = read_local_data(data_old[0])



def addr_analysis(data):
    start = time.time()
    #把数据根据地址分为两部分,一种是地址为json的data_json，一种地址为字符串data_string
    data['location_1'] = 'null'
    data['province'] = 'null'
    data['city'] = 'null'
    data_json = data[data['location'].str.contains('location')]
    print(data_json.dtypes)
    data_string = data[~data['location'].str.contains('location')]
    data_string['location_1'] = data_string['location']
    # print(data_string)
    print('__________________')
    print(len(data), len(data_string), len(data_json))

    # 处理data_json数据，从字典中提取location_1， 省， 市
    for index in data_json.index:
        addr_dict = json.loads(data_json.ix[index, 'location'])
        data_json.ix[index, ['province']] = addr_dict['province']
        data_json.ix[index, ['city']] = addr_dict['city']
        data_json.ix[index, ['location_1']] = addr_dict['location']
    data_json['location_new'] = data_json['province'] + ',' +data_json['city']
    # print(data_json)
    print('addr_analysis(data)运行结束，共花费时间为{time}s。'.format(time=time.time()-start))
    return [data_string, data_json]


def read_local_data(data_old):
    #既有一级行政区又有二级行政区的数据
    #其他

    path = "D:\\work\\database\\province-city\\"
    file_name = "province_city_rule.json"
    # file_name_2 = 'idcard_district_old.csv'

    file = open(path + file_name, encoding='utf-8')
    #读取json文件
    data = json.load(file)
    data = Series(data)
    index_new = []
    for index in data.index:
        index_new.append(int(index))
    #原地址规则文件
    data = DataFrame(data, columns=['addr'])
    data['id'] = index_new
    #得到data表，含有addr，id字段————————————————————————————-----------------————
    # print(data)
    province = data[data['id']%10000==0]
    province['addr_new'] = 'null'

    #第一部分：原数据中含有省名和市名。
    print('找出原数据中的省名和市名，请稍候.............................................................................')
    index_item = province[(province['addr'].str.contains('省'))| (province['addr'].str.contains('市')) ].index
    for item_1 in index_item:
        province.ix[item_1, 'addr_new'] = province.ix[item_1,'addr'][:-1]
    province.ix['150000', 'addr_new'] = '内蒙'
    province.ix['450000', 'addr_new'] = '广西'
    province.ix['540000', 'addr_new'] = '西藏'
    province.ix['640000', 'addr_new'] = '宁夏'
    province.ix['650000', 'addr_new'] = '新疆'
    province.ix['810000', 'addr_new'] = '香港'
    province.ix['820000', 'addr_new'] = '澳门'
    # city = data[(data['id']%100==0) & (data['id']%10000!=0)]
    # 建立一个数据表province，只存放省名，含有addr，id ，addr_new----------------------------------------------------

    #data_2为原地址表，是处理的数据
    data_old['location_new'] = "null"
    #flag判断地址字符中是否含有多个省名,如果只有一个，就确定它属于哪个省，如果有多个省名，则给出提示。
    data_old['flag_province'] = 0
    data_old['flag_city'] = 0
    data_old['flag_county'] = 0

    #找出既有省名又有市名的数据------------------------------------------------------------------------------------------------------------
    #遍历每一个省名
    for index_province in province.index:
        province_word = province.ix[index_province, 'addr_new']
        if province_word not in ['北京', '天津', '上海', '重庆', '香港', '澳门']:
            # print(province_word, '***********************************************************')
            #找到含有该省名的地址，比如 海南
            data_province_index = data_old[data_old["location_1"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1
            # print(data_old)
            #遍历该省下的每一个市名，考虑直辖县，第三位为9的是直辖县，如429004，仙桃市。"469024":"临高县"。市的编码需要大于本省的编码，小于下一省的编码。普通城市编码小于
            #9000，且被100整除，或者大于9000的直辖县。
            for city_index in data[(data['id'] > int(index_province)) & (((data['id'] < int(index_province) + 9000) & (data['id'] % 100 == 0)
                                                                        |(data['id']>int(index_province)+8999)&(data['id']<int(index_province)+10000)))].index:
                city_word = data.ix[city_index, 'addr'][:-1]
                # print(city_word, '_______________')
                #找到地址字符中含有该城市数据索引
                #该省的data_old数据记录中，寻找对应市的记录
                data_old_province = data_old.ix[data_province_index]
                if city_word != '吉林':
                    #data_city_index为data_old中含有特定省名和市名的数据索引，
                    data_city_index = data_old_province[data_old_province['location_1'].str.contains(city_word)].index
                    data_old.ix[data_city_index, 'flag_city'] = data_old.ix[data_city_index, 'flag_city'] + 1
                    # print(data_old.ix[data_city_index, 'location_1'], '____________________________________')

                    #遍历每一个包含该城市名的原数据

                    for data_item in data_city_index:
                        if data_old.ix[data_item, 'flag_city'] == 1:
                            #让标准文件中的城市名作为新的地址
                            data_old.ix[data_item, 'location_new'] = province.ix[index_province, 'addr'] +','+ data.ix[city_index, 'addr']
                        elif data_old.ix[data_item, 'flag_city'] > 1:
                            data_old.ix[data_item, 'location_new'] = "多个城市"
                        else:
                            pass
                else:
                    #如果是吉林市，先不考虑
                    pass
        elif province_word in ['北京', '天津', '上海', '重庆']:
            #如果是这四个直辖市
            data_province_index = data_old[data_old["location_1"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'location_new'] = province_word + '市,' + province_word + '市'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1
        #香港、澳门
        else:
            data_province_index = data_old[data_old["location_1"].str.contains(province_word)].index
            data_old.ix[data_province_index, 'location_new'] = province_word + '特别行政区'
            data_old.ix[data_province_index, 'flag_province'] = data_old.ix[data_province_index, 'flag_province'] + 1

    # writer = pd.ExcelWriter(path + file_name_2[:-4] + '_analysis.xlsx')
    # data_old.to_excel(writer, 'sheet1')
    # writer.save()
    return data_old



if __name__ == '__main__':
#     select_string = '''
#     select id, user_id, location from credit_apply_orders
#     where location is not null
#           and location != ''
# '''
#     cloumns = ['id', 'user_id', 'location']
#     data = mysql_connection(select_string, cloumns)
#     get_local_file(data)
    data = read_local_file()

