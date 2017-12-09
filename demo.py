# -*- coding:utf-8 -*-

import time
# import datetime
# import threading
# import webbrowser
# from collections import  Iterator
# import copy
# from urllib import parse
# import urllib.request
# import operator
# from collections import Counter
# import logging
# import itertools
# import matplotlib.pyplot as plt
# import numpy as np
# import datetime
# import dateutil.parser as dp
# import pytz
# import tkinter
# import scrapy
# from bs4 import BeautifulSoup
# import math


#这是一个测试
if __name__ == '__main__':
    # data = DataFrame([[3, 4, 5], [5, 6, 7], [5, 5, 5]], columns=['a', 'b', 'c'])
    # a = [('john', 'A', 15), ('jane', 'C', 12), ('dave', 'B', 10)]
    # b = ['a', '2', 2, 4, 5, '2', 'b', 4, 7, 'a', 5, 'd', 'a', 'z']
    # f = DataFrame(np.array(data), index=data['a'].values, columns=['a', 'b', 'c'])
    #
    # print(data)
    # print(data.set_index('b',  drop=False))
    start = time.time()
    for i in range(1000):
        for j in range(100):
            for k in range(100):
                i**j
    print('时间为{time}'.format(time=(time.time()-start)))