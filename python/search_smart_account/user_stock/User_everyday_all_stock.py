#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# user_day_value
# $ Id: user_everyday_all_stock.py  2015-12-22 Liu $
#

# --------------------------------------------------------------------
# User_everyday_all_stock.py is
#
# get every user's  all stock  in every day
#
# get 'date' 、every day's 'user' and every user's 'stock' from redis  and store in database
# the format of the stock code's information stored in the database  is  like    'user_code   date  stock_code'
#
# --------------------------------------------------------------------

"""
user_every_all_code.py

"""
import redis
import MySQLdb
import re

# 连接 redis 数据库
REDIS = redis.StrictRedis(host='192.168.1.24', port=6379,
                           db=5, password='kunyandata')
print 'Successfully link redis'

# 获取所有有用户记录的日期
Date = REDIS.keys()               # 获得redis中所有的关键字，这边的key()get到的是时间date
Date = sorted(Date)               # 对所有的date进行排序
Len_Date = len(Date)              # 获得时间列表的长度
print Date
print Len_Date

# 找出每天所有的用户，记录到USER里，循环找到所有日期中的所有用户
USER = []
for date in Date:
    user_s = REDIS.hgetall(date)   # hgetall命令用于获取存储在键的散列的所有字段和值，返回的值是每一个字段名后跟其值，所以回复的长度是散列值两倍的大小。
    for user in user_s:           # 由于hgetall返回值是一个字段名加值，因此要获得用户名需要抽取其中的字段名，值并不抽取，值是每个用户对应持有的股票代码
        USER.append(user)

USER = list(set(USER))    # 对用户去重
print USER
print len(USER)

# 链接 MySQL 数据库，connect方法有四个参数，第一个参数是主机名，第二个参数是数据库的用户名，第三个参数是用户名的密码，最后一个参数是要链接的数据库名。
CONN = MySQLdb.connect(host='192.168.1.14', user='root',
                       passwd='root', db='stock',
                       port=3306)
print 'Successfully link SQLdb'

CUR = CONN.cursor()    # 创建一个游标变量

SQL = 'drop table if exists user_everyday_all_stock'        # 如果数据表存在，先删除。
CUR.execute(SQL)                                              # 调用游标的execute方法执行sql语句
print 'Successfully drop table'

SQL = """create table user_everyday_all_stock(
          user varchar(255) ,
          date varchar(255),
          code varchar(255)
          )"""
CUR.execute(SQL)
print 'Successfully create a table'

FILE = open('user_every_all_stock_code.txt', 'w')
FILE_1 = open('user_every_all_stock_num.txt', 'w')
all_info = ''
for date in Date:
    info = REDIS.hgetall(date)       # 取得当天所有 用户：股票 信息
    date = re.findall(r'\d+', date)                  # 将日期格式规范化
    date = date[0] + date[1] + date[2]
    for user in USER:
        if re.search(user, str(info)):
            count = 0
            code_info = re.findall(r'\d+', str(info[user]))     # 取得 股票 信息
            for stock in code_info:                             # 写入数据库
                count +=1
                CUR.execute('insert into  '
                            'user_everyday_all_stock '
                            'values("%s", "%s","%s")'
                            % (user, date, stock))
                print(user + " " + date + " " + stock + " " + "\n")           # 输出记录
                user_date_code = user + ',' + date + ',' + stock + '\n'
                all_info += user_date_code
            FILE_1.write(user + ',' + date + ',' + str(count) + '\n')
FILE.write(all_info)
FILE.close()
CONN.commit()
CONN.close()