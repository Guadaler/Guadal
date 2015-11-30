#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# code_pro_every
# $Id: code_pro_every.py  2015-11-17 Zhangruibo $
#

# --------------------------------------------------------------------
# code_pro_every is
#
#get every  stock's probability  every day
#
# --------------------------------------------------------------------

"""
code_pro_every.py

"""
import MySQLdb
import datetime
import re
import user_every_add_code
CONN = MySQLdb.connect(host='localhost', user='root', passwd='mysql',
                       db='mysql', port=3306, charset='utf8')
CUR = CONN.cursor()
SQL = 'create table if not exists code_pro_every(' \
      'date varchar(255),'\
      'code varchar(255),' \
      'pro float)'
CUR.execute(SQL)
CUR.execute('select date from user_everyday_add_code ')
TIME_BEGIN_END = CUR.fetchall()
TIME = []
for each_date in TIME_BEGIN_END:
    if re.findall(str(each_date[0]), str(TIME)):
        pass
    else:
        TIME.append(str(each_date[0]))
TIME = sorted(TIME)
for each_time in TIME:
    stock_name = dict()
    stock_name1 = dict()
    user = []
    code = []
    each_user = 0
    CUR.execute('select user,code from user_everyday_add_code  '
                'where date = %s' % (each_time))
    user_code = CUR.fetchall()
    user_code_len = len(user_code)
    while each_user < user_code_len:
        if re.findall(user_code[each_user][0], str(user)):
            pass
        else:
            user.append(user_code[each_user][0])
        each_user = each_user + 1
    user_len = len(user)
    each_code = 0
    count = 0
    while count < user_len:
        while each_code < user_code_len:
            every_code = user_code[each_code][1]
            if re.findall(every_code, str(code)):
                code.append(every_code)
                number = re.findall(every_code, str(code))
                stock_name[every_code] = len(number)
            else:
                code.append(every_code)
                stock_name[every_code] = 1
            each_code = each_code + 1
        count = count + 1
    for x in stock_name:
        code_pro = float(stock_name[x])/user_len
        CUR.execute('insert into code_pro_every '
                    'values("%s","%s","%f")' % (each_time, x, code_pro))
CONN.commit()
CONN.close()



