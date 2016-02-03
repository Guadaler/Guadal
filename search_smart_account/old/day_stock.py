#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# day_stock
# $Id: day_stock.py  2015-10-29 Zhangruibo $
#

# --------------------------------------------------------------------
# day_stock.py is
#
#get every  stock's content  every day
#
# --------------------------------------------------------------------

"""
day_stock.py

"""
import tushare as ts
F = ts.get_today_all()
list(F)
L = F.code
STR = ''
for stock_code in L:
    stock_code = str(stock_code)
    df = ts.get_hist_data(stock_code, start='2015-11-25', end='2015-11-25')
    list(df)
    Open = df.open
    for i in Open:
        Open = str(i)
    Close = df.close
    for j in Close:
        Close = str(j)
        stock_open_close = stock_code + ',20151125,' + Open+',' + Close+'\n'
        STR = STR + stock_open_close
print STR
STR = str(STR)
FILE = open('D:/gupiao/2015-11-25.txt', 'r+')
FILE.write(STR)
FILE.close()



