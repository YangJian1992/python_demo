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
path = 'D:\\work\\bang_miao_pei\\'
file = 'cui_shou'
data = pd.read_csv(path + file +'.csv', sep=',', encoding='utf-8')
print(data.dtypes)
data['mobile'] = data['mobile'].astype('str', errors='raise')
print(data.dtypes)
writer = pd.ExcelWriter(path + file + '.xlsx')
data.to_excel(writer, 'sheet')
writer.save()