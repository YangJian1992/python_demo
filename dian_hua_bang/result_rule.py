import pandas as pd
from pandas import DataFrame, Series
import pandas as pd
from datetime import datetime
from datetime import timedelta
import pymysql
import json
import time


#连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
def mysql_connection(select_string, columns):
    start = time.time()
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    print('已经连接到数据库，请稍候...')
    cur = conn.cursor()
    cur.execute(select_string)
    temp = DataFrame(list(cur.fetchall()), columns=columns)
    print('已经查询到数据，正在处理，请稍候...查询花费时间为%ds。' % (time.time() - start))
    # 提交
    conn.commit()
    # 关闭指针对象
    cur.close()
    # 关闭连接对象
    conn.close()
    return (temp)


def read_data():
    path = 'D:\\work\\dian_hua_bang\\2018-4-10\\'
    file = '电话邦对比结果.xlsx'
    data = pd.read_excel(path+file, sheetname='汇总', header=0)
    data['uid'] = data['uid'].astype('str')
    uid_all = tuple(data['uid'])
    uid_rule_1 = tuple(data[(data['(催收)通话时长60秒以上的次数']>=8) & (data['(近30个自然日)(催收)通话时长60秒以上的次数']>0)
                            & (data['(近30-60个自然日)(催收)通话时长60秒以上的次数'] >0) & (data['(近60-90个自然日)(催收)通话时长60秒以上的次数']>0)
                       ]['uid'])
    # r1 = data['所有时期_(催收)通话号码总个数']>=13
    print(data.dtypes)
    r1 = (data['(催收)通话时长60秒以上的次数']>=8) & (data['(近30个自然日)(催收)通话时长60秒以上的次数']>0)& (data['(近30-60个自然日)(催收)通话时长60秒以上的次数'] >0) & (data['(近60-90个自然日)(催收)通话时长60秒以上的次数']>0)
    r2 =  data['(近一周)(催收)通话号码总个数']>=3
    r3 = data['(近一周)(催收)通话平均时长'] >120
    r4 = data['(近两周)(催收)通话号码总个数'] > 3
    r5 = data['(近两周)(催收)被叫时长']>180
    r6 = data['(近两周)(催收)通话时长60秒以上的次数'] > 2
    r7 = data['(近两周)(催收)通话平均时长']>120
    r8 = data['(近三周)(催收)通话号码总个数'] > 4
    r9 = data['(近三周)(催收)被叫时长'] > 280
    uid_rule_2= tuple(data[r1 | r2 | r3| r4| r5| r6| r7| r8 | r9]['uid'])
    # uid_rule_2 = tuple(data[r2 | r3 | r4 | r5 | r6 | r7 | r8 | r9]['uid'])
    #'1003741' in tuple(data[r4]['uid'])

    uid_2 = tuple(data[~(r1 | r2 | r3| r4| r5| r6| r7| r8 | r9)]['uid'])

    a =list(uid_rule_1)
    a.extend(list(uid_rule_2))
    a = tuple(set(a))


    select_string_2='''
    select 
    #left(u.create_time, 7) as m, 
    #count(u.user_id), 
    sum(if(u.overdue_days>0, u.amount, 0)) as amount_0,
    sum(if(u.overdue_days>15, u.amount, 0)) as amount_15,
    sum(u.amount) as amount_all,
    sum(if(u.overdue_days>0, u.amount, 0))/sum(u.amount) as ratio_0, 
    sum(if(u.overdue_days>15, u.amount, 0))/sum(u.amount)  as ratio_15
    from qiandaodao.user_loan_orders as u 
    left join qiandaodao.ecshop_orders as e
    on u.id = e.id
    where u.loan_status = 2
    and u.first_loan=1
    and u.create_time>'2018-01-23'
    and u.due_repay_date < '2018-04-24'
    and e.user_id in {uid_all}
    #group by m
    '''.format(uid_all=uid_all)
    for uid_tuple in [uid_rule_2, uid_2, uid_all]:
        select_string = '''
        select 
    count(distinct(if(ucg.apply_type=1,e.user_id,null)))as uid_1,
    sum(if(u.overdue_days>0 and ucg.apply_type=1, u.amount, 0)) as amount_0_1,
    sum(if(u.overdue_days>15 and ucg.apply_type=1, u.amount, 0)) as amount_15_1,
    sum(if(ucg.apply_type=1,u.amount,0)) as amount_all_1,
    sum(if(u.overdue_days>0 and ucg.apply_type=1, u.amount, 0)) / sum(if(ucg.apply_type=1,u.amount,0)) as ratio_0_1, 
    sum(if(u.overdue_days>15 and ucg.apply_type=1, u.amount, 0)) / sum(if(ucg.apply_type=1,u.amount,0))  as ratio_15_1,
    count(distinct(if(ucg.apply_type=4,e.user_id,null)))as uid_4,
    sum(if(u.overdue_days>0 and ucg.apply_type=4, u.amount, 0)) as amount_0_4,
    sum(if(u.overdue_days>15 and ucg.apply_type=4, u.amount, 0)) as amount_15_4,
    sum(if(ucg.apply_type=4,u.amount, 0)) as amount_all_4,
    sum(if(u.overdue_days>0 and ucg.apply_type=4, u.amount, 0))/sum(if(ucg.apply_type=4,u.amount, 0)) as ratio_0_4, 
    sum(if(u.overdue_days>15 and ucg.apply_type=4, u.amount, 0))/sum(if(ucg.apply_type=4,u.amount, 0))  as ratio_15_4
            #left(u.create_time, 7) as m, 
            # count(u.user_id), 
            # sum(if(u.overdue_days>0, u.amount, 0)) as amount_0,
            # sum(if(u.overdue_days>15, u.amount, 0)) as amount_15,
            # sum(u.amount) as amount_all,
            # sum(if(u.overdue_days>0, u.amount, 0))/sum(u.amount) as ratio_0, 
            # sum(if(u.overdue_days>15, u.amount, 0))/sum(u.amount)  as ratio_15
            from qiandaodao.user_loan_orders as u 
            left join qiandaodao.ecshop_orders as e
            on u.id = e.id
            left join qiandaodao.user_credit_grant as ucg
            on e.user_id = ucg.user_id
            where u.loan_status = 2
            and u.first_loan=1
            and u.create_time>='2018-01-23'
            and u.due_repay_date <= '2018-04-08'
            and e.user_id in {uid}
        '''.format(uid=uid_tuple)
        data_1 = mysql_connection(select_string, ['count_user_1', 'amount_0_1','amount_15_1','amount_all_1', 'ratio_0_1',  'ratio_15_1',
                                                  'count_user_4', 'amount_0_4', 'amount_15_4', 'amount_all_4',
                                                  'ratio_0_4', 'ratio_15_4'])
        print(data_1)




read_data()