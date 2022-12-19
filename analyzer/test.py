#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :test.py
# @Time      :2022/12/19 19:28
# @Author    :Colin
# @Note      :None


import pandas as pd

print(pd.read_excel('/home/ubuntu/notebook/DataSets/FORUM_SENT/SE_InvestorSentimentSta.xlsx', skiprows=[1, 2]))
