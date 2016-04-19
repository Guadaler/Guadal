# !/usr/bin/python
# -*- coding: UTF-8 -*-
#
#  Stock_info
# $ Id: Stock_info.py  2015-12-28  Liu $
#

# --------------------------------------------------------------------
#  Stock_info.py is
#
#  get stock's  information  from  text, and store in database
#
#  the format of the stock's information stored in the database  is  like  'stock_code   data   open_price   close_price'
#
# --------------------------------------------------------------------

"""
Day_stock_info.py

"""

import MySQLdb

# 连接数据库
CONN = MySQLdb.connect(host='192.168.1.14', user='root',
                       passwd='root', db='stock',
                       port=3306)
print 'Successfully link SQLdb'

# 创建一个游标变量
CUR = CONN.cursor()

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

FILE = open('2015-11-03.txt')
info = FILE.readlines()
for day in info:
    # 去掉末尾的换行符
    day = day[:-1]
    day = day.split(',')
    print day
    CUR.execute('insert into day_stock_info '
                'values("%s", "%s", "%s", "%s") '
                % (day[0], day[1], day[2], day[3]))
FILE.close()
CONN.commit()
CONN.close()