#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# user_day_value
# $ Id: user_every_stock_value.py  2015-12-22 Liu $
#
# --------------------------------------------------------------------
# user_every_stock_value.py is
#
# get every user's each stock value in every day
#
#
# --------------------------------------------------------------------

"""
user_day_code_value.py

"""
import datetime
import redis
import MySQLdb
import re

REDIS = redis.StrictRedis(host='192.168.1.24', port=6379, db=5,
                           password='kunyandata')
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

# 连接数据库
CONN = MySQLdb.connect(host='192.168.1.14', user='root',
                       passwd='root', db='stock', port=3306)
print 'Successfully link SQLdb'

CUR = CONN.cursor()

SQL = 'drop table if exists user_everyday_all_stock_value'
CUR.execute(SQL)

SQL = 'create table user_everyday_all_stock_value( ' \
      'user varchar(255) , ' \
      'date varchar(255), ' \
      'code varchar(255), ' \
      'value float )'
CUR.execute(SQL)
print 'Successfully create a table'

def Count_value(user, date, code):
    CUR.execute('SELECT open FROM '
                        'day_stock_info '
                        'where date = %s  and  '
                        'code =%s ' %(date, code))
    STRING_O = str(CUR.fetchall())
    STRING_O = re.findall(r'\d+.\d+', STRING_O)

    CUR.execute('SELECT close FROM '
                        'day_stock_info '
                        'where date = %s  and  '
                        'code =%s ' %(date, code))
    STRING_C = str(CUR.fetchall())
    STRING_C = re.findall(r'\d+.\d+', STRING_C)
    print STRING_O + STRING_C

    value = 0.0
    if STRING_O != [] and STRING_C != []:
        value = (float(STRING_C[0])-float(STRING_O[0]))/float(STRING_O[0])
        print(user +" " + date + " " +  code + " " + str(value) + "\n")
        CUR.execute('insert into  '
                    'user_everyday_all_stock_value '
                    'values("%s", "%s", "%s", "%f")'
                    % (user, date, code, value))

for user in USER:
    TIME = []
    for date in Date:
        # 首先匹配出所有的数字，相当于将日期中的‘-’去掉了，然后读取前三个，相当于‘年月日’ 输入格式‘2015-1023 16’ 输出格式‘20151023‘
        date_1 = re.findall(r'\d+', date)
        date_1 = date_1[0] + date_1[1] + date_1[2]
        print date_1

        if re.search(date_1, str(TIME)):                # 这里 redis 中存的时间是带小时的，因此有重复的日期
            pass
        else:
            # 从user_everyday_all_stock 调取出当前用户在这一天的所有股票的code
            CUR.execute('SELECT code FROM '
                        'user_everyday_all_stock  '
                        'where date = %s  and  '
                        'user =%s ' %(date_1, user))
            STRING = str(CUR.fetchall())                   # (('000760',), ('000166',), ('002024',), ('000707',))
            STRING = re.findall(r'\d+.\d+', STRING)       # ['000760', '000166', '002024', '000707']
            print STRING

            if STRING == []:
                pass
            else:
                # 对每每一支股票调用函数，计算收益率
                for code in STRING:
                    # 函数的参数是 用户 时间  股票
                    Count_value(user, date_1, code)
        TIME.append(date_1)

CONN.commit()
CONN.close()


