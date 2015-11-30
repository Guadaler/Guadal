#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# code_pro_pre
# $Id: code_pro_pre.py  2015-11-17 Zhangruibo $
#

# --------------------------------------------------------------------
# code_pro_pre.py is
#
#get previous one day  stock's probability in previous one day
#
# --------------------------------------------------------------------

"""
code_pro_pre.py

"""
import datetime
import re
import MySQLdb
import user_every_add_code
CONN = MySQLdb.connect(host='localhost', user='root', passwd='mysql',
                       db='mysql', port=3306, charset='utf8')
CUR = CONN.cursor()
SQL = 'create table if not exists code_now(' \
      'date varchar(255),'\
      'code varchar(255),' \
      'pro float)'
CUR.execute(SQL)
NOW_TIME = datetime.datetime.now()
NOW_TIME = datetime.datetime.strftime(NOW_TIME, '%Y%m%d')
CUR.execute('select user,code from '
            'user_everyday_add_code '
            ' where date = %s' % (NOW_TIME))
USER_CODE = CUR.fetchall()
USER_CODE_LEN = len(USER_CODE)
EACH_USER = 0
USER = []
CODE = []
STOCK_NAME = dict()
while EACH_USER < USER_CODE_LEN:
    if re.findall(USER_CODE[EACH_USER][0], str(USER)):
        pass
    else:
        USER.append(USER_CODE[EACH_USER][0])
    EACH_USER = EACH_USER + 1
USER_LEN = len(USER)
EACH_CODE = 0
COUNT = 0
while COUNT < USER_LEN:
    while EACH_CODE < USER_CODE_LEN:
        EVERY_CODE = USER_CODE[EACH_CODE][1]
        if re.findall(EVERY_CODE, str(CODE)):
            CODE.append(EVERY_CODE)
            NUMBER = re.findall(EVERY_CODE, str(CODE))
            STOCK_NAME[EVERY_CODE] = len(NUMBER)
        else:
            CODE.append(EVERY_CODE)
            STOCK_NAME[EVERY_CODE] = 1
        EACH_CODE = EACH_CODE + 1
    COUNT = COUNT +1
for x in STOCK_NAME:
    code_pro = float(STOCK_NAME[x])/USER_LEN
    print x, code_pro
    CUR.execute('insert into '
                'code_pro values'
                '("%s","%s","%f")' %
                (NOW_TIME, x, code_pro))
CONN.commit()
CONN.close()


