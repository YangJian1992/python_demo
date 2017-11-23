#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
data= DataFrame({'a':[469025, 2, 3, 4],
                 'b': [6, 7, 8, 9],
                 'c': ['adkkda', 6, 5, 6]},index=['a', 'b', 'c', 'd'])
string = '6984国252中'
# data.ix[:2, 'a'] = data.ix[:2, 'a']+1 469000
print(data[(data['a']%10000)//1000==9])
# data.ix[0, 'a'] = 555
# data['d'] = 'NULL'
# data.ix[0,'c'] = data.ix[0, 'c'][:3]
# # data.ix[0,'a'] = data.ix[0,'a'][0] + 300
# ind = data[data['a']>3].index
# for i in ind:
#     print(data.ix[i])