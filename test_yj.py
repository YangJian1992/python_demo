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


#这是一个测试
if __name__ == '__main__':
    data = DataFrame([[3, 4, 5], [5, 6, 7]], columns=['a', 'b', 'c'])
    a = [('john', 'A', 15), ('jane', 'C', 12), ('dave', 'B', 10)]
    b = ['a', '2', 2, 4, 5, '2', 'b', 4, 7, 'a', 5, 'd', 'a', 'z']

