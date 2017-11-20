#coding:utf-8
import os
import re
from pandas import Series, DataFrame
import pandas as pd
import time
def merge_sort(alist):
    if len(alist) <= 1:
        print('alist:', alist)
        return alist
    # 二分分解
    num = len(alist)// 2
    left = merge_sort(alist[:num])
    print('left:', left)
    right = merge_sort(alist[num:])
    print('right:', right)
    # 合并
    return merge(left,right)

def merge(left, right):
    '''合并操作，将两个有序数组left[]和right[]合并成一个大的有序数组'''
    #left与right的下标指针
    l, r = 0, 0
    result = []
    while l<len(left) and r<len(right):
        if left[l] < right[r]:
            result.append(left[l])
            l += 1
        else:
            result.append(right[r])
            r += 1
    #第一遍时，left的序列l并没有加1，仍然为0
    result += left[l:]
    result += right[r:]
    print('----------------result:', result)
    return result

alist = [54,26,93,17,77,31,44,55,20]
sorted_alist = merge_sort(alist)
print(sorted_alist)