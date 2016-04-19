#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# user_day_value
# $ Id: user_everyday_add_stock.py  2015-12-22 Liu $
#

# --------------------------------------------------------------------
# User_everyday_add_stock.py is
#
# get every user's  new  code  in every day
#
# get 'date' 、every day's 'user' and every user's 'stock' from redis, count every user  every day's  new code, and store in database
# the format of the new stock code's information stored in the database  is  like    'user_code   date   stock_code'
#
# --------------------------------------------------------------------

"""
user_every_add_code.py

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

SQL = 'drop table if exists user_everyday_add_stock'        # 如果数据表存在，先删除。
CUR.execute(SQL)                                              # 调用游标的execute方法执行sql语句
print 'Successfully drop table'

SQL = """create table user_everyday_add_stock(
          user varchar(255) ,
          date varchar(255),
          code varchar(255)
          )"""
CUR.execute(SQL)
print 'Successfully create a table'

FILE = open('user_every_add_stock_num.txt', 'w')

# 记录每一个用户每天新增股票到数据库中  函数的参数是： 用户   开始日期指针   开始日期的下一天指针   (这里的日期参数 是日期list Date 的一个指针)
def Get_new_stock(user_code, date_now, date_next):
    Info_now = REDIS.hgetall(Date[date_now])           # 数据开始时间的所有用户信息
    Info_next = REDIS.hgetall(Date[date_next])         # 数据开始时间的下一天的所有用户信息

    if re.search(user_code, str(Info_next)):          # 如果当前用户在下一天出现，则去匹配是否有新增股票
        date_now = date_next
        # re.findall可以获取字符串中所有匹配的字符串。
        stock_code_next = re.findall(r'\d+', str(Info_next[user_code]))       # 查找下一天用户持有的所有股票
        stock_code_now = re.findall(r'\d+', str(Info_now[user_code]))         # 查找当天用户持有的所有股票

        date = re.findall(r'\d+', Date[date_next])                  # 获得下一天的日期
        date = date[0] + date[1] + date[2]                          # 这里的data存储的时候相当于是去掉了 “-”  将 2015-10-27 转换成了 20151027 存储

        count_new = 0
        for stock in stock_code_next:
            if re.search(stock, str(stock_code_now)):   # 根据用户下一天持有的股票 来匹配当天持有的股票
                count_new += 0
            else:                                                           # 若没有匹配到，说明是新增股票，则记录信息
                count_new += 1
                # 获得新增的股票代码 并按照“用户 时间 新增股票”的格式存入数据库 （每一个都是一个记录 独立存入的）
                CUR.execute('insert into  '
                            'user_everyday_add_stock '
                            'values("%s", "%s","%s")'
                            % (user_code, date, stock))
                print(user_code + " " + date + " " + stock + " " + "\n")           # 输出记录

        # 将用户每日新增股的数量记录到文件中 格式为 “用户  日期  新增股数量”
        FILE.write(user_code + " " + date + " " + str(count_new) + "\n")

        if date_next+1 < Len_Date:                          # 当前用户下一天没有匹配到新增股，继续匹配下下一天
            date_next += 1
            Get_new_stock(user_code, date_now, date_next)
    else:                                                   # 当用户在下一天没有出现，继续匹配下下一天
        if date_next + 1 < Len_Date:
            date_next += 1
            Get_new_stock(user_code, date_now, date_next)

# 匹配每一个用户在每一天的信息，如果当天有用户信息，则调用函数去匹配下一天用户有没有新增股
for user in USER:
    for count in range(len(Date)):
        if re.search(user, str(REDIS.hgetall(Date[count]))):
            Get_new_stock(user, count, count+1)
            break

CONN.commit()
CONN.close()
FILE.close()
