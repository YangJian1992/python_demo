import os
import sys
import gc
import re
import smtplib
from pandas import Series, DataFrame
from email.mime.text import MIMEText
from email.header import Header
import pandas as pd
import time
import datetime
import threading
import webbrowser
from collections import  Iterator
import copy
from urllib import parse
import urllib.request
import operator
from collections import Counter
import logging
import itertools
import matplotlib.pyplot as plt
import numpy as np
import datetime
import dateutil.parser as dp
import pytz
import tkinter
import scrapy
from bs4 import BeautifulSoup
import math
import json
import requests
import pymysql
from dateutil.parser import parse
from functools import reduce
from pyspark.sql.types import *
from sklearn import linear_model

data = DataFrame([{'t':'2018-01-08 14:12:26', 'name':'yang'},{'t':'2017-01-08 14:12:26', 'name':'jian'}, {'t':'2014-01-08 14:12:26', 'name':'yj'}])
a= range(10)

num_list =[1, 2,   4, 5, 6, 7,   9,   22, 23, 24]
count_list = []
count_num = 1
for key, item in enumerate(num_list):
    if key > 0 :
        if num_list[key] - num_list[key-1] == 1:
            count_num += 1
            #序列中的最后一个元素需要单独考虑
            if key == len(num_list) - 1:
                count_list.append(count_num)
        else:
            #当出现不连续的数时，就把之前的计数变量加到计数列表中去
            count_list.append(count_num)
            count_num = 1
            # 序列中的最后一个元素需要单独考虑
            if key == len(num_list) - 1:
                count_list.append(count_num)
    else:
        print('序列至少包含两个数据')

print(len(data))





