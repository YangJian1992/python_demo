import pandas as pd
from pandas import Series, DataFrame
import time
import datetime
import numpy as np
import pymysql
import matplotlib.pyplot as plt
'''
1.得到一张表，字段为应还的日期，该天的应还金额，提前还款的金额，以及每三小时的还款比例，该天总的还款比例，共12个字段。
2.随着时间推移，不断更新表格的数据。

'''


# 连接到数据库，输入参数为查询语句字符串，用'''表示，第二个参数为列名，返回查询到的DataFrame格式的数据
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


#根据输入数据，得到所需要的表。
def loan_result(loan_data):
    print('loan_result()函数开始执行。。。\n')
    print('输入数据的类型：\n', loan_data.dtypes)
    print('\n数据一共有%s行'%len(loan_data))
    repay_time_list = []
    result_list = []
    loan_data['amount'] = loan_data['amount'].astype('float', errors='raise')
    loan_data['repay_time'] = loan_data['repay_time'].astype('str', errors='raise')
    loan_data['due_repay_date'] = loan_data['due_repay_date'].astype('str', errors='raise')
    print('输入数据的类型：\n', loan_data.dtypes)

    for i in range(9):
        if i <=3 :
            repay_time_list.append(' 0'+ str(i * 3) +':00:00')
        else:
            repay_time_list.append(' '+ str(i * 3) + ':00:00')
    # print(repay_time_list)

    for due_repay_date, group in loan_data.groupby('due_repay_date'):
        repay_group = group[group['repay_status'] == 2]
        #每个时间段的还款金额和还款比例。
        repay_amount_list = []
        repay_ratio_list = []
        #总的还款金额
        amount_sum = group['amount'].sum()
        amount_repay_before = repay_group[repay_group['repay_time'] < due_repay_date + repay_time_list[0]]['amount'].sum()
        repay_ratio_before = round(amount_repay_before/amount_sum*100, 2)
        #计算每个时间段的金额和比例，放在列表中
        for j in range(8):
            amount_repay = repay_group[(repay_group['repay_time'] < due_repay_date + repay_time_list[j+1]) & (repay_group
                ['repay_time'] >= due_repay_date + repay_time_list[j])]['amount'].sum()
            # print(due_repay_date + repay_time_list[j+1])
            repay_amount_list.append(amount_repay)
        for k in range(8):
            repay_ratio = round(repay_amount_list[k] / amount_sum*100, 2)
            # print(repay_ratio)
            repay_ratio_list.append(repay_ratio)
        repay_ratio_total = round(sum(repay_ratio_list) + repay_ratio_before, 2)
        amount_sum = round(group['amount'].sum(), 2)
        # print(repay_ratio_list)
        #将最终结果放在一个列表中， 记住extend和append都没有返回值，犯一百回这样的错误了。
        # print([due_repay_date, amount_sum, repay_ratio_before, repay_ratio_total].extend(repay_ratio_list))
        temp_list = [due_repay_date, amount_sum, repay_ratio_total, repay_ratio_before]
        temp_list.extend(repay_ratio_list)
        result_list.append(temp_list)
    #把result_list转换成DF， 再返回。
    # result_columns = ['due_repay_date', 'amount_sum', 'repay_ratio_total', 'repay_ratio_before']
    # for i in range(8):
    #     result_columns.append('repay_ratio_' + str(i+1))
    # data_result = DataFrame(result_list, columns=result_columns)
    #设置数据表的字段表
    result_columns = ['应还日期', '贷款金额总和', '还款金额的比例之和,%', '提前还款的比例,%']
    for i in range(8):
        result_columns.append('时间段_' + str((i+1)*3) + ':00,%')
    data_result = DataFrame(result_list, columns=result_columns)
    return data_result


#输入原数据和首借的参数，得到是否首借的数据
def data_first_loan(loan_data, first_loan):
    print('data_first_loan()函数开始执行。。。')
    print(loan_data.dtypes)
    return(loan_data[loan_data['first_loan']==first_loan])


#输入原数据和申请参数，得到不同申请类型的数据
def data_apply_type(loan_data, apply_type):
    print('data_apply_type()函数开始执行。。。')
    print(loan_data.dtypes)
    return(loan_data[loan_data['apply_type']==apply_type])


#生成本地文件
def get_local_file(data_result):
    PATH = 'D:\\work\\database\\user_loan_amount\\'
    FILE = 'user_loan_amount'

    data_result.to_csv(PATH + FILE + '.csv', index=False, encoding='utf-8', sep='\t')
    # 得到工作簿
    writer = pd.ExcelWriter(PATH + FILE + '.xlsx')
    # data_result.to_excel(writer, 'total')

    # 依次得到下面每个工作表
    data_first_1 = data_first_loan(loan_data, 1)
    data_result = loan_result(data_first_1)
    data_result.to_excel(writer, '首借_1')

    data_first_0 = data_first_loan(loan_data, 0)
    data_result = loan_result(data_first_0)
    data_result.to_excel(writer, '首借_0')

    # data_apply_1 = data_apply_type(loan_data, 1)
    # data_result = loan_result(data_apply_1)
    # data_result.to_excel(writer, 'apply_type_1')
    #
    # data_apply_4 = data_apply_type(loan_data, 4)
    # data_result = loan_result(data_apply_4)
    # data_result.to_excel(writer, 'apply_type_4')

    data_apply_1_first_1 = data_apply_type(data_first_1, 1)
    data_result = loan_result(data_apply_1_first_1)
    data_result.to_excel(writer, '申请类型_1_首借_1')

    data_apply_4_first_1 = data_apply_type(data_first_1, 4)
    data_result = loan_result(data_apply_4_first_1)
    data_result.to_excel(writer, '申请类型_4_首借_1')

    data_apply_1_first_0 = data_apply_type(data_first_0, 1)
    data_result = loan_result(data_apply_1_first_0)
    data_result.to_excel(writer, '申请类型_1_首借_0')

    data_apply_4_first_0 = data_apply_type(data_first_0, 4)
    data_result = loan_result(data_apply_4_first_0)
    data_result.to_excel(writer, '申请类型_4_首借_0')
    # 记住保存工作簿
    writer.save()


#f绘图。参数date_0是时间序列的起始日期。
def data_plot(data_result, date_start, date_end):
    PATH = 'D:\\work\\database\\user_loan_amount\\fig\\'
    FILE = 'month_ratio'
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_ylabel('ratio,%')
    ax.set_xlabel('date')
    #创建时间序列索引,也可以用date_range()函数
    index = pd.DatetimeIndex(data_result['应还日期'].values)
    # index = pd.date_range(date_start, date_end, freq='24h')
    # print(data_result)
    #用np.array重新构造一个DF表，并使用时间序列索引
    loan_month = DataFrame(np.array(data_result[['还款金额的比例之和,%', '提前还款的比例,%']]), index=index,
                           columns=['loan_ratio','loan_ratio_before'])
    #当日还款比例
    loan_month['loan_ratio_day'] = loan_month['loan_ratio'] - loan_month['loan_ratio_before']
    # print(index)
    #plot()函数画图
    print(loan_month)
    loan_month.plot(ax=ax, ylim=[0, 100], title='repay_ratio')
    plt.savefig(PATH + FILE + date_start[:-3] + '.png')
    plt.show()


#开始执行函数
if __name__ == '__main__':
    print('************************** start *******************************')
    #查询数据库的日期范围。注意，这里的是变量是字符串，但select语句中日期还要再加字符串，也就是format中{}外面也要加引号。
    date_start = '2017-11-01'
    date_end = '2018-01-015'
    select_string = '''SELECT 
        ulo.id,
        ulo.user_id,
        ulo.due_repay_date,
        ulo.repay_time,
        ulo.amount,
        ulo.repay_status,
        ulo.first_loan,
        ucg.apply_type
    FROM
        user_loan_orders AS ulo
            LEFT JOIN
        user_credit_grant AS ucg ON ucg.user_id = ulo.user_id
    WHERE
        ucg.apply_type IN (1 , 4)
            AND ulo.due_repay_date IS NOT NULL
            AND ulo.due_repay_date BETWEEN "{date_start}" and "{date_end}"
            '''.format(date_start=date_start, date_end=date_end)
    print(select_string)
    columns_add = ['id', 'user_id', 'due_repay_date', 'repay_time', 'amount', 'repay_status', 'first_loan',
                   'apply_type']
    #由数据库得到数据
    loan_data = mysql_connection(select_string, columns_add)
    data_result = loan_result(loan_data)
    data_plot(data_result, date_start, date_end)
    # get_local_file(loan_data)

    # print(data_result)
    print('*************************** end ********************************')