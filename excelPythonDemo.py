# -*- coding:utf-8 -*-
import xlrd, xlwt
# import os
import shutil
import sys
import re

#定义文件路径和文件名，并用unicode转码，支持中文名。
workDir = 'D:\\work\\'
fileName = [unicode(workDir + '线上总体.xlsx', 'utf-8'),
            unicode(workDir + '线上首借.xlsx', 'utf-8'),
            unicode(workDir + '线上续借.xlsx', 'utf-8'),
            unicode(workDir + '线下总体.xlsx', 'utf-8'),
            unicode(workDir + '线下首借.xlsx', 'utf-8'),
            unicode(workDir + '线下续借.xlsx', 'utf-8'), ]

#------读取数据并返回结果start----------------------------------------------------------------
#打开EXCEL文件,由于每个工作簿只有一个表，直接获取工作表。
#定义readSheet()，读取表格
def readSheet():
    sheetR = [[] for i in xrange(6)]#创建sheetR列表，用来存放读取的的表格
    for i in xrange(6):
        sheetR[i] = xlrd.open_workbook(fileName[i]).sheets()[0]
    # sheetR[0] = xlrd.open_workbook('D:\\work\\onlineTotal.xlsx').sheets()[0]
    # sheetR[1] = xlrd.open_workbook('D:\\work\\onlineFirst.xlsx').sheets()[0]
    # sheetR[2] = xlrd.open_workbook('D:\\work\\onlineContinue.xlsx').sheets()[0]
    # sheetR[3] = xlrd.open_workbook('D:\\work\\offlineTotal.xlsx').sheets()[0]
    # sheetR[4] = xlrd.open_workbook('D:\\work\\offlineFirst.xlsx').sheets()[0]
    # sheetR[5] = xlrd.open_workbook('D:\\work\\offlineContinue.xlsx').sheets()[0]
    return sheetR

# i = 0
# while i<=5:
#     result[0].append(2) #每个子列表也含三个2，遍历每一个列表。
#     i = i+1
# print result

#定义函数，计算累加值
def addValue():
    resultAll = []  # 用来存放所有表的result，如：resultAll[0]表示第0张表。不用在[]里新建六个空列表，直接把每个result作为子元素。
    for tableNum in xrange(6):#记住用xrange或range，不能用直接写(0,6)
        dpdNum = 1  # dpdNum为1，表示excel表格的第B列，为sum值；dpdNum为2，表示excel表格的第C列，为dpd1值...
        result = [[] for i1 in range(0, 32)]  # 每张表的累加结果
        # result中每个元素是一个列表，或者result = [[] for i in range (0, 32)]
        # for i in range(0, 32):
        #     result.append([])  # 不能append(blank),否则，添加的是指向同一地址的索引，所有的子列表都是相同的，指向同一段内存。
        while dpdNum <= 31:
            date = 1
            sumAmount = 0
            while date <= 30:
                sumAmount += sheetR[tableNum].cell_value(date, dpdNum)
                result[dpdNum - 1].append(sumAmount)
                date += 1
            dpdNum += 1
        resultAll.append(result)
    return resultAll
#result中第0，1，2...个子列表表示sum,dpd1,dpd2..中的每一天的累加值
# for i in range(0,31):
#     print 'dpd%d'%i
#     print result[i]

#定义函数，计算最终结果，放在一个列表valueAll中
def getValue(dpd,date):#date表示这个月的天数
    resultAll = addValue()
    valueAll = []
    #value[]中每一个子列表表示dpd1、dpd2...
    for tableNum in xrange(6):
        value = [[] for i in range(0, 30)]
        j = 1
        # print 'tableNum:%d'%tableNum
        while j <= dpd:
            i = 0
            while i < date:
                value[j-1].append(resultAll[tableNum][j][i] / resultAll[tableNum][0][i])
                i = i + 1
            # print 'value%d:' % (j - 1)
            j += 1
        # print 'value%d'%tableNum
        # print value

        valueAll.append(value)
        # print '第%d表'%tableNum
    return valueAll
#------读取数据并返回结果end--------

#------写入数据到新表格中start--------
#定义一个函数，将表格写入工作簿中
def writeSheet():
    valueAll = getValue(dpd, days)
    workbook = xlwt.Workbook(encoding='ascii')

    # 新建一张sheetW列表，用来存放新建的六个表格
    sheetW = [[] for i in range (0, 6)]

    #给六个表格命名，取"\"和"."之间的内容。
    for i in xrange(6):
        sheetW[i] = workbook.add_sheet(fileName[i][(fileName[i].rindex('\\')+1):(fileName[i].index('.')):1])

    #创建日期，以及字段名,dpd1,dpd2...等
    for tableNum in range(0, 6):
        for j in range(1, dpd+1):
            sheetW[tableNum].write(0,j,'dpd%d'%j)
        for i in range(1, days+1):
            if i<10:
                sheetW[tableNum].write(i, 0, '%s-0%d'%(month,i))
            else:
                sheetW[tableNum].write(i, 0, '%s-%d'%(month,i))
        for i in range(1, dpd+1):
            for j in range(1, days+1):
                sheetW[tableNum].write(j, i, '%.2f%%'%(100*valueAll[tableNum][i-1][j-1]))

    resultName = 'vintage_%s.xls'%month
    workbook.save(resultName)
    return resultName
#------写入数据到新表格中end--------


#------执行以下程序----------

month = raw_input('输入年份和月份，如：2017-09，不能输入其他格式的数据。请输入：')
#判断用户输入的数据是否符合要求
while re.match('^20[0-9]{2}-[0-9]{2}$',month) == None:
    print '输入有误，请重新输入：'
    month = raw_input('输入年份和月份，如：2017-09，不能输入其他格式的数据。请输入：')
print '输入的数据正确，请稍候。'

#增加一个功能判断每个月的天数（考虑闰年），最好用正则，以后再看看。
if month[-2:] in ['01', '03', '05', '07', '08', '10', '12']:
    days = 31
elif month[-2:] in ['04', '06', '09', '11']:
    days = 30
elif month[-2:]=='02':
    year = int(month[:4])
    if (year%4==0 and year%100!=0) or year%400==0:
        days = 29
    else:
        days = 28
else:
    try:
        sys.exit(0)
    except:
        print "输入有误，程序已终止"

dpd = 30 #dpd的数量
sheetR = readSheet()
resultName = writeSheet()

#将结果移动到指定的文件夹下，使用shutil.move，该方法先复制，再删除，如果删除不了，就只能复制了。
shutil.move(resultName, workDir + unicode('每月处理结果汇总', 'utf-8') + '\\'+ resultName)
#删除六个原文件
# os.chdir(workDir)
# for i in xrange(6):
#     os.remove(fileName[i])
print '结果已经存放在: \n'+workDir + '每月处理结果汇总\\' + resultName
# print '原文件已经删除'
