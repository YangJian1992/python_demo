import os
from pandas import Series, DataFrame
import pandas as pd
import time
import pymysql
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn import svm
from sklearn import tree
import graphviz
import numpy as np
import json
from datetime import datetime

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


def read_data():
    path = 'D:\\work\\贷款需求模型\\'
    #这些是先申请额度后借款的用户，不包括申请额度之前有过借款记录的用户。
    #由于county地址不规范，有的是江苏，没有省字，不方便分类，需要根据身份证号码确定一级行政区。
    path_province = 'D:\\资料\\githubRepository\\data_location\\'
    json_f = 'list.json'
    with open(path_province+json_f, 'r', encoding='utf-8') as f:
        pro_data = json.load(f)#这里的数据含有k=11或12的,为北京市和天津市。
    pro_dict = {}#pro_dict里放的是两位数字代码和相应的省份名称。
    for k, v in pro_data.items():
        if len(k) == 6:
            if k[2:] == '0000' :
                pro_dict[k[:2]] = v

    select_string = '''
    #这些是先申请额度后借款的用户，不包括申请额度之前有过借款记录的用户, 124025个用户。
    #计算注册、申请、借款的时间间隔。额度申请时间和发起借款次数不止一次,取第一次来计算间隔
SELECT 
    cao.user_id as '用户ID',
    if(ucp.sex='女',0, 1) as '性别',
	users.create_time AS '注册时间',
    if(left(users.device, 4)='IDFV',1, if(left(users.device, 4)='IMEI',2, 3)) as '设备型号',
    ucp.county as '地址',
    ucp.idcard as 'idcard',
    if(right(ucp.county, 1) ='县',1,if(right(ucp.county, 1)='市', 2,if(right(ucp.county, 1)='区',3,4))) as '三级行政区类型',
    #ucp.birth as 'birth date',
    floor(timestampdiff(day,  ucp.birth, now())/365) as 'age',
    if(floor(timestampdiff(day,  ucp.birth, now())/365) <=23,1,if(floor(timestampdiff(day,  ucp.birth, now())/365) >=31,3,2)) as 'age_class',
    #max(users.logon_time) as '最近登陆时间',
    MIN(cao.create_time) AS '最早额度时间',
    max(cao.create_time) AS '最近额度时间',
    #MIN(ulo.create_time) AS '最早借款时间',
    #MAX(ulo.create_time) AS '最近借款时间',
    max(ulo.overdue_days) as '最大逾期天数',
	if(max(ulo.overdue_days)=0, 0, if(max(ulo.overdue_days)>=30, 2, 1)) AS '最大逾期天数',
    DATEDIFF(MIN(cao.create_time), users.create_time) AS '最早额度时间与注册时间间隔',
    COUNT(DISTINCT ulo.id) AS '借款订单数量'
    #DATEDIFF(MIN(ulo.create_time), MIN(cao.create_time)) AS '最早借款时间与最早额度时间间隔',
    #DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time)) AS '最近借款时间与最早借款时间间隔',
    #DATEDIFF(max(users.logon_time),MAX(ulo.create_time)) AS '最近借款时间与最近登陆时间间隔',
    #DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time))/ COUNT(DISTINCT ulo.id) AS '平均借款间隔'
FROM
    (SELECT 
       user_id, create_time, id
    FROM
        qiandaodao.credit_apply_orders 
    WHERE
        auth_status = 2
            AND create_time < '2017-12-13') AS cao
        INNER JOIN
    (
    SELECT 
        user_id, create_time, id, payee_bank_name, overdue_days, payee_bank_card
    FROM
        qiandaodao.user_loan_orders 
    WHERE
        create_time < '2017-12-13'
        ) 
        AS ulo ON cao.user_id = ulo.user_id
        LEFT JOIN
    users ON ulo.user_id = users.id
    LEFT JOIN 
	user_credit_profile as ucp on ucp.user_id = cao.user_id
GROUP BY cao.user_id
HAVING MIN(cao.create_time) < MIN(ulo.create_time)
union 
#这些是先申请额度后借款的用户，不包括申请额度之前有过借款记录的用户, 124025个用户。
#计算注册、申请、借款的时间间隔。额度申请时间和发起借款次数不止一次,取第一次来计算间隔
SELECT 
    cao.user_id as '用户ID',
    if(ucp.sex='女',0, 1) as '性别',
	users.create_time AS '注册时间',
    if(left(users.device, 4)='IDFV',1, if(left(users.device, 4)='IMEI',2, 3)) as '设备型号',
    ucp.county as '地址',
    ucp.idcard as 'idcard',
    if(right(ucp.county, 1) ='县',1,if(right(ucp.county, 1)='市', 2,if(right(ucp.county, 1)='区',3,4))) as '三级行政区类型',
    #ucp.birth as 'birth date',
    floor(timestampdiff(day,  ucp.birth, now())/365) as 'age',
    if(floor(timestampdiff(day,  ucp.birth, now())/365) <=23,1,if(floor(timestampdiff(day,  ucp.birth, now())/365) >=31,3,2)) as 'age_class',
    #max(users.logon_time) as '最近登陆时间',
    MIN(cao.create_time) AS '最早额度时间',
    max(cao.create_time) AS '最近额度时间',
    #MIN(ulo.create_time) AS '最早借款时间',
    #MAX(ulo.create_time) AS '最近借款时间',
	DATEDIFF(MIN(cao.create_time), users.create_time) AS '最早额度时间与注册时间间隔',
    max(ulo.overdue_days) as '最大逾期天数',
	if(max(ulo.overdue_days)<=30, 0, if(max(ulo.overdue_days)>30, 2, null)) AS '最大逾期天数是否大于30',#0, 0-29, 30+对应的值为0，1，2
    COUNT(DISTINCT ulo.id) AS '借款订单数量'
    #DATEDIFF(MIN(ulo.create_time), MIN(cao.create_time)) AS '最早借款时间与最早额度时间间隔',
    #DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time)) AS '最近借款时间与最早借款时间间隔',
    #DATEDIFF(max(users.logon_time),MAX(ulo.create_time)) AS '最近借款时间与最近登陆时间间隔',
    #DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time))/ COUNT(DISTINCT ulo.id) AS '平均借款间隔'
FROM
    (SELECT 
        user_id, create_time, id
    FROM
        qiandaodao.credit_apply_orders
    WHERE
        auth_status = 2
            AND create_time < '2017-12-13') AS cao
        LEFT JOIN
    (SELECT 
        user_id,
            create_time,
            id,
            overdue_days
    FROM
        qiandaodao.user_loan_orders
    WHERE
        create_time < '2017-12-13') AS ulo 
        ON cao.user_id = ulo.user_id
        LEFT JOIN
    users ON cao.user_id = users.id
        LEFT JOIN
    user_credit_profile AS ucp ON ucp.user_id = cao.user_id
    left join qiandaodao_risks_control.t_user_info as tui on tui.user_id = cao.user_id
WHERE
    ulo.user_id IS NULL
GROUP BY cao.user_id 
;
    '''

    col_names = '用户ID, 性别, 注册时间, 设备型号,地址, idcard, 三级行政区类型, age, age_class, 最早额度时间, 最近额度时间,' \
                '最大逾期天数,最大逾期天数是否大于30天, 最早额度时间与注册时间间隔,借款订单数量'
    columns_add = col_names.split(sep=',')
    columns_add = [x.strip() for x in columns_add]
    data_1 = mysql_connection(select_string, columns_add)
    data_1['Y'] = 'null_yj'

    #data_1['最近借款时间与最近登陆时间间隔'][data_1['最近借款时间与最近登陆时间间隔'].isnull()] = 0
    data_1['最早额度时间与注册时间间隔'][data_1['最早额度时间与注册时间间隔'].isnull()] = 0
    #data_2 = mysql_connection(select_string_2, columns_add)
    data_1_x = data_1[['性别','最大逾期天数', '收款银行卡的数量', '最早额度时间与注册时间间隔', '最早借款时间与最早额度时间间隔', '最近借款时间与最早借款时间间隔', '最近借款时间与最近登陆时间间隔', '平均借款间隔']]
    data_1_x = data_1[['最大逾期天数','收款银行卡的数量',  '最近借款时间与最早借款时间间隔', '最近借款时间与最近登陆时间间隔', '平均借款间隔']]
    #对字段值进行分类，全部转化成离散型变量
    data_1_y = pd.cut(data_1['借款订单数量'], [1, 2, 3, 10000000], right=False, labels=['1', '2', '3'])
    data_1_x['最大逾期天数'] = pd.cut(data_1_x['最大逾期天数'], [0, 1, 30, 1000000], right=False, labels=[0, 1, 2])
    data_1_x['收款银行卡的数量'] = pd.cut(data_1_x['收款银行卡的数量'], [1, 2, 1000], right=False, labels=[1,2])
    #data_1_x['最早额度时间与注册时间间隔'] = pd.cut(data_1_x['最早额度时间与注册时间间隔'], [0, 1, 4, 15, 1000000], right=False, labels=[0, 1, 2, 3])
    #data_1_x['最早借款时间与最早额度时间间隔'] = pd.cut(data_1_x['最早借款时间与最早额度时间间隔'], [0, 1, 4, 15, 1000000], right=False, labels=[0, 1, 2, 3])
    data_1_x['最近借款时间与最早借款时间间隔'] = pd.cut(data_1_x['最近借款时间与最早借款时间间隔'], [0, 1, 32, 180, 1000000], right=False, labels=[0, 1, 2, 3])
    data_1_x['最近借款时间与最近登陆时间间隔'] = pd.cut(data_1_x['最近借款时间与最近登陆时间间隔'], [-100, 1, 32, 180, 1000000], right=False,labels=[0, 1, 2, 3])
    data_1_x['平均借款间隔'] = pd.cut(data_1_x['平均借款间隔'], [0, 1, 31, 1000000], right=False,labels=[0, 1, 2])
    x_train, x_test, y_train, y_test = train_test_split(np.array(data_1_x),np.array(data_1_y), test_size=0.4)
    clf = svm.SVC(kernel='rbf')
    scores = cross_val_score(clf, data_1_x, data_1_y, cv=5)
    clf.fit(x_train, y_train)
    y = clf.predict(x_test)

    clf_tree = tree.DecisionTreeClassifier()
    clf_tree.fit(x_train, y_train)
    y_tree = clf_tree.predict(x_test)
    dot_data = tree.export_graphviz(clf_tree, out_file='tree.dot',  # doctest: +SKIP
                                    feature_names=['overdue_days', 'bank_card_count', 'latest_earliest_loan_interval', 'latest_login_loan_interval', 'average_loan_interval'],
                                    class_names=['1', '2', '3'],  # doctest: +SKIP
                                    filled=True, rounded=True,  # doctest: +SKIP
                                    special_characters=True)  # doctest: +SKIP
    graph = graphviz.Source(dot_data)

