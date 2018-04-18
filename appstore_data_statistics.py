import pymysql
import pandas as pd
import numpy as np
import time
import re
from datetime import timedelta
import datetime
import pdb
import copy


def processing_status(status_num):
    status_exp = ''
    if status_num == -3:
        status_exp = '退回到用户'
    elif status_num == -2:
        status_exp = '退回到现场办理'
    elif status_num == -1:
        status_exp = '申请未激活'
    elif status_num == 0:
        status_exp = '等待'
    elif status_num == 1:
        status_exp = '拒绝'
    elif status_num == 2:
        status_exp = '通过'
    elif status_num == 3:
        status_exp = '关闭'
    return status_exp
def get_dealing_data(start_date_set_ini, end_date_set_ini):
    cur.execute('select i.mobile, c.auth_status, c.live_submit, c.auth_time from credit_apply_orders c left join users i' \
                ' on c.user_id = i.id where c.create_time >= \'' + start_date_set_ini + '\' and c.create_time <= \'' + end_date_set_ini + '\' order by c.create_time desc')
    df_credit = pd.DataFrame(list(cur.fetchall()),columns=['mobile', 'auth_status', 'live_submit', 'auth_time'])
    cur.execute('select i.mobile from user_loan_orders p left join users i on p.user_id = i.id' \
                ' where p.create_time >= \'' + start_date_set_ini + '\' and p.create_time <= \'' + end_date_set_ini + '\'')
    df_loan = pd.DataFrame(list(cur.fetchall()), columns=['mobile'])
    cur.execute('select i.mobile from user_loan_installment p left join users i on p.user_id = i.id' \
                ' where p.create_time >= \'' + start_date_set_ini + '\' and p.create_time <= \'' + end_date_set_ini + '\'')
    df_loan_ins = pd.DataFrame(list(cur.fetchall()), columns=['mobile'])
    cur.execute('select i.mobile from ecshop_orders e left join users i on e.user_id = i.id where e.create_time >= \'' + start_date_set_ini + '\' and e.create_time <= \'' + end_date_set_ini + '\'')
    df_ecshop = pd.DataFrame(list(cur.fetchall()), columns=['mobile'])
    cur.execute('select name, code from agents')
    df_agents = pd.DataFrame(list(cur.fetchall()), columns=['name', 'code'])
    return df_credit, df_loan, df_loan_ins, df_ecshop, df_agents
def type_determining(device_in):
    type_out = ''
    if device_in != None and device_in != '':
        if 'IMEI#' in device_in:
            type_out = 'android'
        elif 'IDFV#' in device_in:
            type_out = 'ios'
    return type_out
def dealt_with_raw_data(input_list_mobiles):
    list_for_present_code = []
    for list_temp in input_list_mobiles:
        list_for_present_code_temp = [list_temp]
        if list_temp in list_cre_mos:
            credit_temp_data = df_credit[df_credit['mobile'] == list_temp].values.tolist()[0]
            if credit_temp_data[3].to_pydatetime() >= datetime.datetime.strptime(start_date_set_ini,'%Y-%m-%d %H:%M:%S') \
                    and credit_temp_data[3].to_pydatetime() <= datetime.datetime.strptime(end_date_set_ini,'%Y-%m-%d %H:%M:%S'):
                credit_status_temp = processing_status(credit_temp_data[1])
                list_for_present_code_temp.append(credit_status_temp)
            else:
                list_for_present_code_temp.append('N')
            list_for_present_code_temp.append(credit_temp_data[2])
        else:
            list_for_present_code_temp.append('N')
            list_for_present_code_temp.append('N')
        if list_temp in list_loan_ing_nums:
            list_for_present_code_temp.append('是')
        else:
            list_for_present_code_temp.append('否')
        if list_temp in list_ecshop_ing_nums:
            list_for_present_code_temp.append('是')
        else:
            list_for_present_code_temp.append('否')
        if list_temp in list_loan_ins_ing_nums:
            list_for_present_code_temp.append('是')
        else:
            list_for_present_code_temp.append('否')
        list_for_present_code.append(list_for_present_code_temp)
    df_for_present_code = pd.DataFrame(list_for_present_code, columns=['mobile', 'auth_status', 'live_submit', 'is_loan', 'is_ecshop', 'is_loan_ins'])
    return df_for_present_code
def append_group_temp(input_str):
    group_temp[input_str + 'sum'] = group_temp[input_str].sum()
def get_first_loan_sum(input_mobile_list):
    if len(input_mobile_list) == 1:
        cur.execute('select u.amount from user_loan_orders u left join users i on u.user_id = i.id '\
                    'where u.loan_status = 2 and u.first_loan = 1 and i.mobile = \'' + input_mobile_list[0] + '\'')
    else:
        cur.execute('select u.amount from user_loan_orders u left join users i on u.user_id = i.id ' \
                    'where u.loan_status = 2 and u.first_loan = 1 and i.mobile in ' + str(tuple(input_mobile_list)))
    df_data = pd.DataFrame(list(cur.fetchall()), columns=['amount'])
    return df_data['amount'].sum()

try:
    print('程序开始执行')
    start = time.time()
    code_is_exe_date = datetime.datetime.now().strftime('%Y-%m-%d')
    input_parameters_set = input('请输入查询所需数据（例：2017-10-06 18:12:23/2017-10-10 10:06:01)：')
    start_date_set_ini = re.findall(r'[^\/]+\/', input_parameters_set)[0][:-1]
    end_date_set_ini = re.findall(r'\/[^\/]+', input_parameters_set)[0][1:]
    print('起始时间为：%s' % start_date_set_ini)
    print('终止时间为：%s' % end_date_set_ini)
    output_address = 'E:///'
    conn = pymysql.connect(host='rr-bp1jnr76z49y5k9mno.mysql.rds.aliyuncs.com', port=3306, user='qiandaodao',
                           passwd='qdddba@2017*', db='qiandaodao', charset='utf8')
    cur = conn.cursor()
    cur.execute('select mobile, create_time, agent_code, device from users where '\
                'agent_type = 2 and create_time >= \'' + start_date_set_ini + '\' and create_time <= \'' + end_date_set_ini + '\' order by create_time asc')
    df_data_from = pd.DataFrame(list(cur.fetchall()), columns=['mobile', 'create_time', 'agent_code', 'device'])
    cur.execute('select mobile, create_time, agent_code, device from users where ' \
                'agent_type = 0 and create_time >= \'' + start_date_set_ini + '\' and create_time <= \'' + end_date_set_ini + '\' and device like \'%IDFV#%\' order by create_time asc')
    df_data_from2 = pd.DataFrame(list(cur.fetchall()), columns=['mobile', 'create_time', 'agent_code', 'device'])
    df_data_from2['type'] = 'ios'
    df_data_from2['agent_code'] = 'N_not_invited'
    cur.execute('select mobile, create_time, agent_code, device from users where ' \
                'agent_type = 1 and create_time >= \'' + start_date_set_ini + '\' and create_time <= \'' + end_date_set_ini + '\' and device like \'%IDFV#%\' order by create_time asc')
    df_data_from3 = pd.DataFrame(list(cur.fetchall()), columns=['mobile', 'create_time', 'agent_code', 'device'])
    df_data_from3['type'] = 'ios'
    df_data_from3['agent_code'] = 'N_invited_by_users'
    df_data_from['type'] = df_data_from['device'].apply(lambda x: type_determining(x))
    df_data_from = df_data_from[['mobile', 'create_time', 'agent_code', 'type']]
    df_credit, df_loan, df_loan_ins, df_ecshop, df_agents = get_dealing_data(start_date_set_ini, end_date_set_ini)
    list_loan_ing_nums = df_loan['mobile'].unique().tolist()
    list_ecshop_ing_nums = df_ecshop['mobile'].unique().tolist()
    list_loan_ins_ing_nums = df_loan_ins['mobile'].unique().tolist()
    list_dealing_nums = df_loan.append(df_ecshop.append(df_loan_ins, ignore_index=True), ignore_index=True)['mobile'].unique().tolist()
    list_cre_mos = df_credit['mobile'].tolist()
    df_data_from1 = df_data_from[-((df_data_from['agent_code'].isnull()) | (df_data_from['agent_code'] == ''))]
    df_data_from = df_data_from1.append(df_data_from2.append(df_data_from3, ignore_index = True), ignore_index = True)
    df_groupby = df_data_from.groupby(['agent_code', 'type'])
    all_keys = df_groupby.groups.keys()
    list_for_res = []
    for key_temp_up in all_keys:
        key_temp = key_temp_up[0]
        if key_temp != 'N_not_invited' and key_temp != 'N_invited_by_users':
            list_for_res_temp = [start_date_set_ini + '/' + end_date_set_ini, df_agents[df_agents['code'] == key_temp]['name'].tolist()[0], key_temp, key_temp_up[1]]
        else:
            list_for_res_temp = [start_date_set_ini + '/' + end_date_set_ini, 'ios_appstore', key_temp, key_temp_up[1]]
        group_temp = df_groupby.get_group(key_temp_up)
        list_for_res_temp.append(get_first_loan_sum(group_temp['mobile'].tolist()))
        list_for_res_temp.append(len(group_temp))
        df_for_present_code = dealt_with_raw_data(group_temp['mobile'].tolist())
        list_for_res_temp.append(df_for_present_code[-(df_for_present_code['auth_status'] == 'N')]['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[-(df_for_present_code['auth_status'] == 'N')]['auth_status'].count()/len(group_temp))
        list_for_res_temp.append(df_for_present_code[df_for_present_code['auth_status'] == '通过']['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[(df_for_present_code['auth_status'] == '通过') & (df_for_present_code['live_submit'] != 0)]['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[(df_for_present_code['auth_status'] == '通过') & (df_for_present_code['live_submit'] == 0)]['auth_status'].count())
        if df_for_present_code[-(df_for_present_code['auth_status'] == 'N')]['auth_status'].count() > 0:
            list_for_res_temp.append(df_for_present_code[\
                                         df_for_present_code['auth_status'] == '通过']['auth_status'].count()/df_for_present_code[-(df_for_present_code['auth_status'] == 'N')]['auth_status'].count())
        else:
            list_for_res_temp.append('额度申请人数为0')
        list_for_res_temp.append(df_for_present_code[df_for_present_code['auth_status'] == '拒绝']['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[df_for_present_code['auth_status'] == '申请未激活']['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[(df_for_present_code['auth_status'] == '退回到用户') | (df_for_present_code['auth_status'] == '退回到现场办理')]['auth_status'].count())
        list_for_res_temp.append(df_for_present_code[df_for_present_code['is_loan'] == '是']['is_loan'].count())
        if df_for_present_code[df_for_present_code['auth_status'] == '通过']['auth_status'].count() > 0:
            list_for_res_temp.append(df_for_present_code[df_for_present_code['is_loan'] == '是']['is_loan'].count()/df_for_present_code[df_for_present_code['auth_status'] == '通过']['auth_status'].count())
        else:
            list_for_res_temp.append('额度通过的人数为0')
        list_for_res_temp.append(df_for_present_code[df_for_present_code['is_ecshop'] == '是']['is_ecshop'].count())
        list_for_res_temp.append(df_for_present_code[df_for_present_code['is_loan_ins'] == '是']['is_loan_ins'].count())
        list_for_res.append(list_for_res_temp)
    df_for_res = pd.DataFrame(list_for_res, columns=['时间', '应用市场名称', '邀请码', '设备类型', '首笔借款', '注册的人数', '提交额度申请的人数', '申请率', '额度通过的人数', '通过面签拿额度的人数'\
        , '通过线上申请拿额度的人数', '额度通过率', '申请额度被拒绝的人数', '申请未激活的人数' \
        , '被退回到现场和被退回到个人的人数', '发起快贷的人数', '快贷率', '发起购物的人数', '发起分期贷的人数'])
    df_groupby = df_for_res.groupby(['邀请码'])
    all_keys = df_groupby.groups.keys()
    df_for_ult = pd.DataFrame([])
    columns_set = ['时间', '应用市场名称', '邀请码', '设备类型', '首笔借款', '注册的人数sum', '注册的人数', '提交额度申请的人数sum', '提交额度申请的人数', \
                   '申请率', '额度通过的人数sum', '额度通过的人数', '通过面签拿额度的人数sum', '通过面签拿额度的人数', '通过线上申请拿额度的人数sum', \
                   '通过线上申请拿额度的人数', '额度通过率', '申请额度被拒绝的人数sum', '申请额度被拒绝的人数', '申请未激活的人数sum', '申请未激活的人数', \
                   '被退回到现场和被退回到个人的人数sum', '被退回到现场和被退回到个人的人数', '发起快贷的人数sum', '发起快贷的人数', '快贷率', \
                   '发起购物的人数sum', '发起购物的人数', '发起分期贷的人数sum', '发起分期贷的人数']
    append_field_str = ['注册的人数', '提交额度申请的人数', '额度通过的人数', '通过面签拿额度的人数', '通过线上申请拿额度的人数', \
                        '申请额度被拒绝的人数', '申请未激活的人数', '被退回到现场和被退回到个人的人数', '发起快贷的人数', '发起购物的人数', '发起分期贷的人数']
    for key_temp in all_keys:
        group_temp = df_groupby.get_group(key_temp)
        for field_str_temp in append_field_str:
            append_group_temp(field_str_temp)
        df_for_ult = df_for_ult.append(group_temp[columns_set], ignore_index = True)
    writer = pd.ExcelWriter(output_address + code_is_exe_date + '各邀请码在对应时间段内的数据.xlsx', engine='xlsxwriter')
    df_for_ult.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    cur.close()
    conn.close()
    print(time.time() - start)
    print('程序执行完成')
    input('请按Enter键退出')
except Exception as e:
    print(str(e))
    print('程序运行异常！请查看报错文件！请按Enter退出！')
    new_file = open(output_address + code_is_exe_date + '.txt', 'w')
    new_file.write(str(e))
    new_file.close()
    input('请按Enter键退出')






