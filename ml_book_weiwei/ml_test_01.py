import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import taobao_orders.user_info_jd_taobao as tou



a = {"a":3, "b":4, "c":5}
arr = pd.DataFrame([a,a])
data = pd.DataFrame(arr.values, columns=[['m','m','n'],['b', 'c', 'a']])
print(data)
print(data.stack())
print(arr)
print(arr.stack())
