#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
data= DataFrame({'a':[469025, 2, 3, 4],
                 'b': [6, 7, 8, 9],
                 'c': ['adkkda', 6, 5, 6]},index=['a', 'b', 'c', 'd'])
string = '6984菲律'
# data.ix[:2, 'a'] = data.ix[:2, 'a']+1 469000
if re.search(r'阿联酋|新西兰|菲律宾|越南|韩国|马来西亚|马尔代夫|英国|美国|俄罗斯|缅甸|海外|加拿大|'
                         r'泰国|法国|日本|新加坡|意大利|德国|意大利|挪威|阿尔巴尼亚|老挝|阿富汗|阿拉伯|瑞士|柬埔寨', string):
    print('000000000000000')
# data.ix[0, 'a'] = 555
# data['d'] = 'NULL'
# data.ix[0,'c'] = data.ix[0, 'c'][:3]
# # data.ix[0,'a'] = data.ix[0,'a'][0] + 300
# ind = data[data['a']>3].index
# for i in ind:
#     print(data.ix[i])