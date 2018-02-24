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
import shutil
from sklearn.linear_model import LinearRegression
from matplotlib.font_manager import FontProperties


m = [{'a':2.4455, "b":3.44344}, {'a':4.4435, "b":6.42}, {'a':3.4485, "b":8.32532}]
n = DataFrame(m)
print(n)
print(n['b'].sum(), round(n['b'].max(),2), '\n****************************')
n['a'] = n['a'].map(lambda x:round(x, 1))
print(n)
print(round(n['a'].sum(), 2), round(n['a'].max(),2))
