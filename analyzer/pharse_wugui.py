#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :pharse_wugui.py
# @Time      :2022/12/10 11:40
# @Author    :Colin
# @Note      :None


import pandas as pd

import os

PATH_FILE = '/home/ubuntu/notebook/DataSets/IDX_WUGUI/'
df = pd.DataFrame()
for i in os.listdir(PATH_FILE):
    df = pd.concat([df, pd.read_excel(PATH_FILE + i)], axis=0)

# ['指数代码', '指数名称', '日期', '收盘价', 'PE_TTM_加权', 'PE_TTM_等权', 'PB_TTM_加权',
#        'PB_TTM_等权', '股息收益率 %', 'ROE %', '成分股平均滚动净利润(亿)', '成分股平均市值(亿)',
#        '指数总流通市值(亿)', '指数总市值(亿)']
# 改名字
df = df.rename(
        columns={'指数代码'          : 'ts_code', '指数名称': 'code_name', '日期': 'trade_date', '收盘价': 'close',
                 'PE_TTM_加权'       : 'pe_ttm_weight',
                 'PE_TTM_等权'       : 'pe_ttm_equal', 'PB_TTM_加权': 'pb_ttm_weight',
                 'PB_TTM_等权'       : 'pb_ttm_equal', '股息收益率 %': 'div_rate', 'ROE %': 'roe', '成分股平均滚动净利润(亿)': 'profit_avg',
                 '成分股平均市值(亿)': 'mv_avg',
                 '指数总流通市值(亿)': 'float_mv', '指数总市值(亿)': 'total_mv'})
# 改指标
df['ts_code'] = df['ts_code'].str[2:] + '.' + df['ts_code'].str[:2]
df['trade_date'] = df['trade_date'].str.replace('-', '').astype('uint32')
#
df = df.set_index(['trade_date', 'ts_code']).sort_index()
df.to_parquet('/data/DataSets/investor_sentiment/IDX_PANEL_BASIC_WUGUI.parquet', index=True)
