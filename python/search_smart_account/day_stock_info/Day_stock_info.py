# !/usr/bin/python
# -*- coding: UTF-8 -*-
#
#  Day_stock_info
# $ Id: Day_stock.py  2015-12-21  Liu $
#

# --------------------------------------------------------------------
#  Day_stock.py is
#
#  get stock's  information  from  network  every day, and store in database
#
#  the format of the stock's information stored in the database  is  like  'stock_code   data   open_price   close_price'
#
# --------------------------------------------------------------------

"""
Day_stock_info.py

"""

import tushare as ts
import os
import MySQLdb

# 获取当天所有股票的行情信息（TuShare的数据主要来源于网络）
# 这里爬取的内容结构是： code   name       changepercent   trade    open    high    low   settlement  \\number(从0开始编号）
#              0      000520   长航凤凰       682.609      19.80   18.00    19.80   16.96      2.53
F = ts.get_today_all()
# 这里list()函数是转换数据类型，Tushare返回的一般都是pandas DataFrame类型（是Tushare自己定义的一种数据类型）
list(F)
print F
# 获得当天大盘上每一支股票的代码
L = F.code
print L

L_1 = []
L_day =[]
# "stock_code"文档中存放着所有股票的代码，读出所有的历史股票代码
if os.path.exists('stock_code.txt'):
    FILE_1 = open('stock_code.txt')
    L_s = FILE_1.read()
    #print L_s
    L_1 = L_s.split('\n')
    # 最后一个元素分割以后是个空字符，所有要去掉
    del L_1[-1]
    #print L_1
    FILE_1.close()

# 将当天所有新增的股票代码添加到"stock_code"文档中
for code in L:
    # 将股票代码转换成string类型，理由同上
    code = str(code)
    #print(code)
    L_1.append(code)
    L_day.append(code)
#print L_1
#print L_day
# 对所有的股票代码去重
L_1 = list(set(L_1))
print L_1

L_day = list(set(L_day))
print L_day

# 将去重后的 股票代码 写入"stock_code"文档中
FILE_1 = open('stock_code.txt', 'w')
for code1 in L_1:
    #print code1
    FILE_1.write(code1 + '\n')
FILE_1.close()

# 连接数据库
CONN = MySQLdb.connect(host='192.168.1.14', user='root',
                       passwd='root', db='stock',
                       port=3306)
print 'Successfully link SQLdb'

# 创建一个游标变量
CUR = CONN.cursor()

# 这里每天的信息要逐天存入数据库，所有数据表新建一次就可以（但是暂时不知道如果一天的数据重新写入两次会出现什么后果，这里要注意避免重复存入）
# SQL = 'drop table if exists day_stock_info'
# CUR.execute(SQL)
# print 'Successfully drop table'

# 创建数据表
SQL = """create table if not exists day_stock_info(
          code varchar(255),
          date varchar(255),
          open varchar(255),
          close varchar(255)
          )"""
# 调用游标的execute方法执行sql语句
CUR.execute(SQL)
print 'Successfully create a table'

# 设置变量，便于每日爬取数据时修改日期
# 注意这里爬取信息的时候，日期的格式是“####-##-##”否则无法爬取数据，但是进行存储时，是没有‘-’的
day = '2015-12-28'
day_w = '20151228'
FILE = open('2015-12-28.txt', 'w')

STR = ''
# 对每一支股票抽取大盘信息：股票代码  固定时间区间  开盘价  收盘价
for stock_code in L_day:
    # 将股票代码转换成string类型，理由同上
    # stock_code = str(stock_code)
    # print(stock_code)

    # 获取股票指定时间区间的大盘信息
    # 这里爬取的信息结构是：  open   high   close   low    volume   price_change   p_change   ma5  \\ndata
    df = ts.get_hist_data(stock_code, start=day, end=day)
    list(df)
    #print(df)

    # 提取开盘价open的值，并将其转换为string类型（这里为什么要用循环，不是只有一个open吗？）——因为DataFrame数据类型的原因
    Open = df.open
    for i in Open:
        Open = str(i)

    # 提取收盘价close的值，并将其转换为string类型
    Close = df.close
    for j in Close:
        Close = str(j)

        # 存储每一支股票的内容信息：股票代码  时间  开盘价  收盘价
        stock_open_close = stock_code + ',' + day_w + ',' + Open + ',' + Close + '\n'
        print stock_open_close
        STR = STR + stock_open_close
        CUR.execute('insert into day_stock_info '
                    'values("%s", "%s", "%s", "%s") '
                    % (stock_code, day_w, Open, Close))

# 将信息写入文件
STR = str(STR)
FILE.write(STR)
FILE.close()

CONN.commit()
CONN.close()