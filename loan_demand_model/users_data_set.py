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
    select_string_1 = '''
    SELECT 
    cao.user_id as '用户ID',
    if(ucp.sex='女', 0, 1) as '性别',
	users.create_time AS '注册时间',
    max(users.logon_time) as '最近登陆时间',
    MIN(cao.create_time) AS '最早额度时间',
    max(cao.create_time) AS '最近额度时间',
    MIN(ulo.create_time) AS '最早借款时间',
    MAX(ulo.create_time) AS '最近借款时间',
	max(ulo.overdue_days) AS '最大逾期天数',
	COUNT(DISTINCT ulo.id) AS '借款订单数量',
    count(distinct ulo.payee_bank_card) as '收款银行卡的数量',
    DATEDIFF(MIN(cao.create_time), users.create_time) AS '最早额度时间与注册时间间隔',
    DATEDIFF(MIN(ulo.create_time), MIN(cao.create_time)) AS '最早借款时间与最早额度时间间隔',
    DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time)) AS '最近借款时间与最早借款时间间隔',
    DATEDIFF(max(users.logon_time),MAX(ulo.create_time)) AS '最近借款时间与最近登陆时间间隔',
    DATEDIFF(MAX(ulo.create_time) , MIN(ulo.create_time))/ COUNT(DISTINCT ulo.id) AS '平均借款间隔'
FROM
    (SELECT 
       user_id, create_time, id
    FROM
        qiandaodao.credit_apply_orders 
    WHERE
        auth_status = 2
            AND create_time < '2017-12-13') AS cao
        INNER JOIN
    (SELECT 
        user_id, create_time, id, payee_bank_name, overdue_days, payee_bank_card
    FROM
        qiandaodao.user_loan_orders 
    WHERE
        create_time < '2017-12-13') 
        AS ulo ON cao.user_id = ulo.user_id
        LEFT JOIN
    users ON ulo.user_id = users.id
    LEFT JOIN 
	user_credit_profile as ucp on ucp.user_id = cao.user_id
GROUP BY cao.user_id
HAVING MIN(cao.create_time) < MIN(ulo.create_time) 

    '''
    #下面这些是已经申请额度但没有借款的用户
    select_string_2 = '''

SELECT 
    cao.user_id AS '用户ID',
    if(ucp.sex='男',1,if(ucp.sex='女', 0 , -1)) as '性别',
    users.create_time AS '注册时间',
    MAX(users.logon_time) AS '最近登陆时间',
    MIN(cao.create_time) AS '最早额度时间',
    MAX(cao.create_time) AS '最近额度时间',
    MIN(ulo.create_time) AS '最早借款时间',
    MAX(ulo.create_time) AS '最近借款时间',
    MAX(ulo.overdue_days) AS '最大逾期天数',
    COUNT(DISTINCT ulo.id) AS '借款订单数量',
    COUNT(DISTINCT ulo.payee_bank_card) AS '收款银行卡的数量',
    DATEDIFF(MIN(cao.create_time), users.create_time) AS '最早额度时间与注册时间间隔',
    DATEDIFF(MIN(ulo.create_time), MIN(cao.create_time)) AS '最早借款时间与最早额度时间间隔',
    DATEDIFF(MAX(ulo.create_time), MIN(ulo.create_time)) AS '最近借款时间与最早借款时间间隔',
    DATEDIFF(MAX(users.logon_time), MAX(ulo.create_time)) AS '最近借款时间与最近登陆时间间隔',
    DATEDIFF(MAX(ulo.create_time), MIN(ulo.create_time)) / COUNT(DISTINCT ulo.id) AS '平均借款间隔'
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
            payee_bank_name,
            overdue_days,
            payee_bank_card
    FROM
        qiandaodao.user_loan_orders
    WHERE
        create_time < '2017-12-13') AS ulo 
        ON cao.user_id = ulo.user_id
        LEFT JOIN
    users ON cao.user_id = users.id
        LEFT JOIN
    user_credit_profile AS ucp ON ucp.user_id = cao.user_id
WHERE
    ulo.user_id IS NULL
GROUP BY cao.user_id

    '''
    col_names = '用户ID, 性别, 注册时间, 最近登陆时间, 最早额度时间, 最近额度时间, 最早借款时间, 最近借款时间, 最大逾期天数, 借款订单数量, 收款银行卡的数量, 最早额度时间与注册时间间隔, 最早借款时间与最早额度时间间隔, 最近借款时间与最早借款时间间隔, 最近借款时间与最近登陆时间间隔, 平均借款间隔'
    columns_add = col_names.split(sep=',')
    columns_add = [x.strip() for x in columns_add]
    data_1 = mysql_connection(select_string_1, columns_add)
    data_1['最近借款时间与最近登陆时间间隔'][data_1['最近借款时间与最近登陆时间间隔'].isnull()] = 0
    data_1['最早额度时间与注册时间间隔'][data_1['最早额度时间与注册时间间隔'].isnull()] = 0
    #data_2 = mysql_connection(select_string_2, columns_add)
    data_1_x = data_1[['性别','最大逾期天数', '收款银行卡的数量', '最早额度时间与注册时间间隔', '最早借款时间与最早额度时间间隔', '最近借款时间与最早借款时间间隔', '最近借款时间与最近登陆时间间隔', '平均借款间隔']]
    data_1_x = data_1[['最大逾期天数','收款银行卡的数量',  '最近借款时间与最早借款时间间隔', '最近借款时间与最近登陆时间间隔', '平均借款间隔']]
    #对字段值进行分类，全部转化成离散型变量
    data_1_y = pd.cut(data_1['借款订单数量'], [1, 2, 3, 10000000], right=False, labels=['1', '2', '3'])
    data_1_x['最大逾期天数'] = pd.cut(data_1_x['最大逾期天数'], [0, 1, 30, 1000000], right=False, labels=[0, 1, 2])
    data_1_x['收款银行卡的数量'] = pd.cut(data_1_x['收款银行卡的数量'], [1, 2, 1000], right=False, labels=[1,2])
    data_1_x['最早额度时间与注册时间间隔'] = pd.cut(data_1_x['最早额度时间与注册时间间隔'], [0, 1, 4, 15, 1000000], right=False, labels=[0, 1, 2, 3])
    data_1_x['最早借款时间与最早额度时间间隔'] = pd.cut(data_1_x['最早借款时间与最早额度时间间隔'], [0, 1, 4, 15, 1000000], right=False, labels=[0, 1, 2, 3])
    data_1_x['最近借款时间与最早借款时间间隔'] = pd.cut(data_1_x['最近借款时间与最早借款时间间隔'], [0, 1, 32, 180, 1000000], right=False, labels=[0, 1, 2, 3])
    data_1_x['最近借款时间与最近登陆时间间隔'] = pd.cut(data_1_x['最近借款时间与最近登陆时间间隔'], [-100, 1, 32, 180, 1000000], right=False,labels=[0, 1, 2, 3])
    data_1_x['平均借款间隔'] = pd.cut(data_1_x['平均借款间隔'], [0, 1, 32, 1000000], right=False,labels=[0, 1, 2])
    x_train, x_test, y_train, y_test = train_test_split(data_1_x, data_1_y, test_size=0.4)
    clf = svm.SVC(kernel='rbf')
    scores = cross_val_score(clf, data_1_x, data_1_y, cv=5)
    clf.fit(x_train, y_train)
    y = clf.predict(x_test)

    clf_tree = tree.DecisionTreeClassifier()
    clf_tree.fit(x_train, y_train)
    y_tree = clf_tree.predict(x_test)
    dot_data = tree.export_graphviz(clf_tree, out_file='tree.dot')
    graph = graphviz.Source(dot_data)
