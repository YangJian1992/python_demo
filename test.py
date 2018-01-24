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
import tensorflow as tf
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from matplotlib.font_manager import FontProperties
font = FontProperties(fname=r"c:\windows\fonts\msyh.ttc", size=15)

a = [{"a":[{"a":'yj'}], "b":{"a":'yy'}}, {"a":[{"a":'jian'}], "b":{"a":'yang'}}]
data = DataFrame(a)
print(data['a'].map(lambda x:x[0]['a']))




