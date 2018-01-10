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

with open('C:\\Users\\QDD\\Desktop\\operator\\operator\\17369735519_2018-01-08.json', encoding='utf-8') as file:
    data = json.load(file)
for key, item in data.items():
    print(key, item, '\n')





