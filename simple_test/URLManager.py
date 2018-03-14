import os
import sys
import gc
import re
import smtplib
import xlrd
import  openpyxl
import xlsxwriter
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
import tensorflow as tf
import matplotlib.pyplot as plt
import shutil
from sklearn.linear_model import LinearRegression
from matplotlib.font_manager import FontProperties
from dateutil.parser import parse

class UrlManager(object):
    def __init__(self):
        self.new_urls = set()#未爬取的集合
        self.old_urls = set()#已爬取的集合

    #判断是否有待取的URL
    def has_new_url(self):
        return self.new_url_size()!=0

    #获取一个未爬取的URL
    def get_new_url(self):
        new_url = self.new_urls.pop()
        self.old_urls.add(new_url)
        return new_url

    #添加新的url到未爬取的集合, 单个url
    def add_new_url(self, url):
        if url is None:
            return
        if url not in self.new_urls and url not in self.old_urls:
            self.new_urls.add(url)

    # 添加新的url到未爬取的集合， urls集合
    def add_new_urls(self, urls):
        if urls is None or len(urls) == 0:
            return
        for url in urls:
            self.add_new_url(url)

    #获取未爬取集合
    def new_url_size(self):
        return  len(self.new_urls)

    #已爬取集合的大小
    def old_url_size(self):
        return len(self.old_urls)

